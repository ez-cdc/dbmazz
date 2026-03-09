// Copyright 2025
// Licensed under the Elastic License v2.0

//! Snapshot worker: reads existing rows from PostgreSQL and loads them into StarRocks.
//!
//! # Algorithm (Flink CDC concurrent snapshot)
//!
//! The WAL consumer and snapshot worker run concurrently. Deduplication is handled
//! by the WAL consumer via `SharedState::should_emit()`.
//!
//! For each chunk:
//! 1. Emit LW watermark via `pg_logical_emit_message`
//! 2. `SELECT * FROM table WHERE pk >= start AND pk < end`
//! 3. Emit HW watermark → returns the HW LSN
//! 4. Stream Load rows to StarRocks (upsert)
//! 5. Mark chunk COMPLETE in `dbmazz_snapshot_state`
//! 6. Register (start_pk, end_pk, hw_lsn) in `SharedState.finished_chunks`
//! 7. Update `SharedState` snapshot progress counters

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::grpc::state::{SharedState, Stage};
use crate::connectors::sinks::starrocks::stream_load::{StreamLoadClient, StreamLoadOptions};
use crate::connectors::sinks::starrocks::StarRocksSinkConfig;
use super::chunker::{Chunk, chunk_table};
use super::quote_ident;
use super::state_store;
use super::utils::find_integer_pk_column;

/// Pre-computed metadata for a snapshot table (avoids redundant catalog queries per chunk).
struct TableMeta {
    pk_col: String,
    col_names: Vec<String>,
}

/// Run the full snapshot for all configured tables.
///
/// This function is spawned as a concurrent task alongside the WAL consumer.
/// It exits when all chunks are complete or on a non-retriable error.
pub async fn run_snapshot(
    config: Arc<Config>,
    shared_state: Arc<SharedState>,
) -> Result<()> {
    info!(
        "Snapshot worker starting (chunk_size={}, workers={})",
        config.snapshot_chunk_size, config.snapshot_parallel_workers
    );

    shared_state.set_snapshot_active(true);
    shared_state.set_stage(Stage::Snapshot, "Connecting to source").await;
    let snapshot_start = std::time::Instant::now();

    // Connect to PostgreSQL (regular connection, not replication).
    // Strip `replication=database` from the URL — DDL is not allowed in replication mode.
    let plain_url = strip_replication_param(&config.database_url);
    let (client, connection) = tokio_postgres::connect(&plain_url, NoTls)
        .await
        .context("snapshot worker: failed to connect to PostgreSQL")?;
    let client = Arc::new(client);

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("snapshot worker: postgres connection error: {}", e);
        }
    });

    // Ensure state table exists
    state_store::ensure_state_table(&client).await?;

    // Build the Stream Load client using the normalized HTTP URL from the sink config
    let sr_config = StarRocksSinkConfig::from_sink_config(&config.sink)
        .context("snapshot worker: failed to build StarRocks config")?;
    let sl_client = Arc::new(StreamLoadClient::new(
        sr_config.http_url.clone(),
        sr_config.database.clone(),
        sr_config.user.clone(),
        sr_config.password.clone(),
    ));

    let slot_name = config.slot_name.clone();
    let tables = config.tables.clone();
    let chunk_size = config.snapshot_chunk_size;

    // Pre-compute table metadata to avoid redundant catalog queries per chunk.
    // Done before workers start — fast catalog queries only.
    let mut table_meta: HashMap<String, TableMeta> = HashMap::new();
    for table in &tables {
        if let Some(pk_col) = find_integer_pk_column(&client, table).await? {
            let col_names = get_column_names(&client, table).await?;
            table_meta.insert(table.clone(), TableMeta { pk_col, col_names });
        }
    }
    let table_meta = Arc::new(table_meta);

    // Clear per-table progress from any previous snapshot run
    shared_state.clear_table_progress().await;

    // Initial progress from previous runs (for resumed snapshots)
    let (initial_total, initial_done) = state_store::chunk_counts(&client, &slot_name)
        .await.unwrap_or((0, 0));
    let initial_rows = state_store::total_rows_synced(&client, &slot_name)
        .await.unwrap_or(0);
    shared_state.update_snapshot_progress(
        initial_total as u64, initial_done as u64, initial_rows as u64,
    );

    // Load per-table progress from previous runs (for resumed snapshots)
    if let Ok(per_table) = state_store::per_table_progress(&client, &slot_name).await {
        for (table, total, done, rows) in per_table {
            shared_state.set_table_chunks_total(&table, total as u64).await;
            shared_state.update_table_progress(&table, done as u64, rows as u64).await;
        }
    }

    // Open N worker PG connections for truly parallel SELECTs.
    // Each worker gets a dedicated connection (vs pipelining on one, which PG serializes).
    // The original `client` is reserved for the producer task (chunking + state_store).
    let n_workers = (config.snapshot_parallel_workers as usize).max(1);
    let mut pool_conns: Vec<Arc<Client>> = Vec::with_capacity(n_workers);
    for i in 0..n_workers {
        let (c, conn) = tokio_postgres::connect(&plain_url, NoTls)
            .await
            .with_context(|| format!("snapshot worker: failed to open PG connection {}", i))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("snapshot PG connection {} error: {}", i, e);
            }
        });
        pool_conns.push(Arc::new(c));
    }
    info!("Opened {} PG connections for parallel snapshot", n_workers);

    // Connection pool: semaphore limits concurrency, pool assigns dedicated connections.
    let pool = Arc::new(tokio::sync::Mutex::new(pool_conns));
    let semaphore = Arc::new(Semaphore::new(n_workers));

    // Channel for streaming chunks from producer to workers.
    // Workers start processing as soon as the first table's chunks arrive,
    // instead of waiting for all tables to be chunked (~3min on 113K chunks).
    let (tx, mut rx) = mpsc::channel::<(String, Chunk)>(n_workers * 2);

    // Spawn producer: compute chunks per table and stream to workers.
    let producer_client = Arc::clone(&client);
    let producer_slot = slot_name.clone();
    let producer_tables = tables.clone();
    let producer_shared_state = Arc::clone(&shared_state);
    let producer = tokio::spawn(async move {
        for table in &producer_tables {
            let chunks = chunk_table(&producer_client, table, chunk_size).await
                .unwrap_or_else(|e| {
                    warn!("Failed to chunk table {}: {}", table, e);
                    vec![]
                });

            // Persist chunks to state_store (idempotent: ON CONFLICT DO NOTHING)
            for chunk in &chunks {
                state_store::upsert_chunk(
                    &producer_client, &producer_slot, table,
                    chunk.partition_id, chunk.start_pk, chunk.end_pk,
                ).await.unwrap_or_else(|e| warn!("upsert_chunk failed: {}", e));
            }

            // Skip already-complete chunks (resumability)
            let complete_ids = state_store::load_complete_partition_ids(
                &producer_client, &producer_slot, table,
            ).await.unwrap_or_default();

            let total = chunks.len();
            let skipped = complete_ids.len();
            info!("Table {}: {} chunks ({} pending, {} already done)",
                table, total, total - skipped, skipped);

            // Set per-table total chunks (producer knows the definitive count)
            producer_shared_state.set_table_chunks_total(table, total as u64).await;

            // Stream pending chunks to workers
            for chunk in chunks {
                if complete_ids.contains(&chunk.partition_id) {
                    continue;
                }
                if tx.send((table.clone(), chunk)).await.is_err() {
                    warn!("Chunk channel closed — workers may have exited");
                    return;
                }
            }
        }
        // tx is dropped here → channel closes → consumer loop exits
    });

    // Consumer: spawn worker tasks as chunks arrive from the producer.
    let mut join_set = JoinSet::new();
    while let Some((table, chunk)) = rx.recv().await {
        let pool = Arc::clone(&pool);
        let semaphore = Arc::clone(&semaphore);
        let sl_client = Arc::clone(&sl_client);
        let slot_name = slot_name.clone();
        let shared_state = Arc::clone(&shared_state);
        let table_meta = Arc::clone(&table_meta);

        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let meta = table_meta.get(&table)
                .ok_or_else(|| anyhow::anyhow!("no metadata for table {}", table))?;
            let pg_client = match pool.lock().await.pop() {
                Some(c) => c,
                None => return Err(anyhow::anyhow!("snapshot pool exhausted")),
            };
            let result = process_chunk(
                &pg_client, &sl_client, &slot_name, &table, &chunk, &shared_state, meta,
            ).await;
            pool.lock().await.push(pg_client);
            result
        });
    }

    // Wait for producer to finish (should already be done since channel is closed)
    if let Err(e) = producer.await {
        error!("Snapshot producer task panicked: {}", e);
    }

    // Wait for all worker tasks to complete
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("Chunk failed: {:#}", e),
            Err(e) => error!("Chunk task panicked: {}", e),
        }
    }

    // Update final progress
    let (final_total, final_done) = state_store::chunk_counts(&client, &slot_name).await?;
    let final_rows = state_store::total_rows_synced(&client, &slot_name).await?;
    shared_state.update_snapshot_progress(
        final_total as u64, final_done as u64, final_rows as u64,
    );

    let elapsed = snapshot_start.elapsed();
    if state_store::all_chunks_complete(&client, &slot_name).await? {
        info!(
            "Snapshot complete: {} chunks, {} rows synced in {:.1}s ({:.0} rows/sec)",
            final_total, final_rows, elapsed.as_secs_f64(),
            final_rows as f64 / elapsed.as_secs_f64().max(0.001)
        );
        shared_state.set_snapshot_active(false);
        shared_state.set_stage(Stage::Cdc, "Replicating").await;
    } else {
        warn!(
            "Snapshot finished with some failed chunks in {:.1}s — will retry on next start",
            elapsed.as_secs_f64()
        );
        shared_state.set_snapshot_active(false);
        shared_state.set_stage(Stage::Cdc, "Replicating (snapshot incomplete)").await;
    }

    Ok(())
}

/// Process a single chunk: LW watermark → SELECT → HW watermark → Stream Load → mark complete.
async fn process_chunk(
    client: &Client,
    sl_client: &StreamLoadClient,
    slot_name: &str,
    table: &str,
    chunk: &Chunk,
    shared_state: &SharedState,
    meta: &TableMeta,
) -> Result<()> {
    debug!("Processing chunk {}/{}: pk=[{}, {})",
        table, chunk.partition_id, chunk.start_pk, chunk.end_pk);

    state_store::mark_chunk_in_progress(client, slot_name, table, chunk.partition_id).await?;

    // Step 1: Emit LW (low watermark) — non-transactional
    let lw_content = format!("LW:{}:{}:{}", table, chunk.start_pk, chunk.end_pk);
    client.execute(
        "SELECT pg_logical_emit_message(false, 'dbmazz', $1)",
        &[&lw_content],
    ).await.context("failed to emit LW watermark")?;

    // Step 2: SELECT rows for this chunk
    // Extract table name for the destination (strip schema prefix)
    let dest_table = table.rsplit('.').next().unwrap_or(table);

    // Use pre-computed metadata (avoids redundant catalog queries per chunk)
    let col_names = &meta.col_names;

    // Build SELECT with all columns cast to ::text for universal type handling
    let cols_sql: String = col_names.iter()
        .map(|c| format!("{}::text AS {}", quote_ident(c), quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted_pk = quote_ident(&meta.pk_col);
    let select_query = format!(
        "SELECT {cols} FROM {table} WHERE {pk} >= $1::bigint AND {pk} < $2::bigint",
        cols = cols_sql,
        table = quote_ident(table),
        pk = quoted_pk,
    );
    let rows = client.query(&select_query, &[&chunk.start_pk, &chunk.end_pk])
        .await
        .with_context(|| format!("SELECT failed for {} chunk {}", table, chunk.partition_id))?;

    let row_count = rows.len() as i64;

    // Step 3: Emit HW (high watermark) immediately after SELECT — captures LSN
    // before Stream Load so WAL events during load are correctly deduplicated.
    let hw_content = format!("HW:{}:{}:{}", table, chunk.start_pk, chunk.end_pk);
    let hw_row = client.query_one(
        "SELECT pg_logical_emit_message(false, 'dbmazz', $1)::text",
        &[&hw_content],
    ).await.context("failed to emit HW watermark")?;

    // pg_logical_emit_message returns pg_lsn (displayed as hex string like "0/1234AB")
    let hw_lsn_str: String = hw_row.get(0);
    let hw_lsn = parse_pg_lsn(&hw_lsn_str)
        .ok_or_else(|| anyhow::anyhow!("failed to parse HW LSN: '{}'", hw_lsn_str))?;

    // Step 4: Serialize rows to JSON (for Stream Load)
    // All columns are text thanks to ::text cast, so we just read Option<String>
    let synced_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let body = if rows.is_empty() {
        b"[]".to_vec()
    } else {
        serialize_text_rows_to_json(&rows, col_names, &synced_at, hw_lsn)?
    };

    // Step 5: Stream Load to StarRocks (only if there are rows)
    if !rows.is_empty() {
        let body_arc = Arc::new(body);
        let result: crate::connectors::sinks::starrocks::stream_load::StreamLoadResult =
            sl_client.send(dest_table, body_arc, StreamLoadOptions::default())
            .await
            .with_context(|| format!("Stream Load failed for {} chunk {}", table, chunk.partition_id))?;

        debug!("Stream Load chunk {}/{}: {} rows loaded", table, chunk.partition_id, result.loaded_rows);
    }

    // Step 6: Mark chunk complete in state store
    state_store::mark_chunk_complete(
        client, slot_name, table, chunk.partition_id, row_count, hw_lsn as i64,
    ).await?;

    // Step 7: Register in SharedState so WAL consumer can deduplicate
    let relation_id = get_relation_id(client, table).await.unwrap_or_else(|e| {
        tracing::warn!("Could not resolve relation OID for table '{}': {}. WAL dedup may be incomplete.", table, e);
        0
    });
    shared_state.register_finished_chunk(relation_id, chunk.start_pk, chunk.end_pk, hw_lsn).await;

    // Update global progress counters
    let (total, done) = state_store::chunk_counts(client, slot_name).await?;
    let rows_synced = state_store::total_rows_synced(client, slot_name).await?;
    shared_state.update_snapshot_progress(total as u64, done as u64, rows_synced as u64);

    // Update per-table progress counters
    let (table_done, table_rows) = state_store::table_chunk_progress(client, slot_name, table).await?;
    shared_state.update_table_progress(table, table_done as u64, table_rows as u64).await;

    info!("Chunk {}/{} complete: {} rows, hw_lsn=0x{:X}",
        table, chunk.partition_id, row_count, hw_lsn);

    Ok(())
}

/// Serialize tokio_postgres rows to a JSON array string for Stream Load.
/// Format: `[{"col1":"val1","col2":"val2"}, ...]`
/// Serialize rows to JSON where all columns were cast to ::text in the query.
/// Each column is read as Option<String> — no type-specific conversions needed.
/// Appends CDC audit columns (dbmazz_op_type, dbmazz_is_deleted, dbmazz_synced_at, dbmazz_cdc_version).
fn serialize_text_rows_to_json(
    rows: &[tokio_postgres::Row],
    col_names: &[String],
    synced_at: &str,
    hw_lsn: u64,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(b'[');

    let cdc_version_str = (hw_lsn as i64).to_string();

    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 {
            out.push(b',');
        }
        out.push(b'{');
        for (col_idx, col_name) in col_names.iter().enumerate() {
            if col_idx > 0 {
                out.push(b',');
            }
            let val: Option<String> = row.get(col_idx);

            out.push(b'"');
            out.extend_from_slice(col_name.as_bytes());
            out.extend_from_slice(b"\":");
            match val {
                Some(s) => {
                    out.push(b'"');
                    // Escape JSON-special chars at byte level (avoids UTF-8 decode/re-encode)
                    write_json_escaped(&mut out, &s);
                    out.push(b'"');
                }
                None => out.extend_from_slice(b"null"),
            }
        }

        // Append CDC audit columns
        out.extend_from_slice(b",\"dbmazz_op_type\":0");
        out.extend_from_slice(b",\"dbmazz_is_deleted\":false");
        out.extend_from_slice(b",\"dbmazz_synced_at\":\"");
        out.extend_from_slice(synced_at.as_bytes());
        out.extend_from_slice(b"\"");
        out.extend_from_slice(b",\"dbmazz_cdc_version\":");
        out.extend_from_slice(cdc_version_str.as_bytes());

        out.push(b'}');
    }

    out.push(b']');
    Ok(out)
}

/// Write a JSON-escaped string directly at the byte level.
///
/// JSON-special characters (`"`, `\`, `\n`, `\r`, `\t`) are all ASCII (< 0x80).
/// Multi-byte UTF-8 continuation bytes are >= 0x80 and never match these,
/// so we can safely scan and copy byte slices between escape points.
#[inline]
fn write_json_escaped(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let escape: &[u8] = match b {
            b'"'  => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        out.extend_from_slice(escape);
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
}

/// Get column names for a table.
async fn get_column_names(client: &Client, table_name: &str) -> Result<Vec<String>> {
    let (schema, table) = if table_name.contains('.') {
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    let rows = client.query(
        "SELECT a.attname
         FROM pg_attribute a
         JOIN pg_class c ON c.oid = a.attrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2
           AND a.attnum > 0 AND NOT a.attisdropped
         ORDER BY a.attnum",
        &[&schema, &table],
    ).await?;

    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Strip the `replication=database` query parameter from a PostgreSQL URL.
fn strip_replication_param(url_str: &str) -> String {
    match url::Url::parse(url_str) {
        Ok(mut parsed) => {
            let pairs: Vec<(String, String)> = parsed.query_pairs()
                .filter(|(k, _)| k != "replication")
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
            if pairs.is_empty() {
                parsed.set_query(None);
            } else {
                let qs = pairs.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&");
                parsed.set_query(Some(&qs));
            }
            parsed.to_string()
        }
        Err(_) => {
            // Fallback: simple string replacement
            url_str
                .replace("&replication=database", "")
                .replace("?replication=database&", "?")
                .replace("?replication=database", "")
        }
    }
}

/// Get the pg_class OID (relation_id) for a table, matching what the WAL handler sees.
async fn get_relation_id(client: &Client, table_name: &str) -> Result<u32> {
    let (schema, table) = if table_name.contains('.') {
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    let row = client.query_one(
        "SELECT c.oid::int4
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2",
        &[&schema, &table],
    ).await.context("failed to get relation OID")?;

    let oid: i32 = row.get(0);
    Ok(oid as u32)
}

/// Parse a PostgreSQL LSN string like "0/1234AB" into a u64.
fn parse_pg_lsn(s: &str) -> Option<u64> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let hi = u64::from_str_radix(parts[0], 16).ok()?;
    let lo = u64::from_str_radix(parts[1], 16).ok()?;
    Some((hi << 32) | lo)
}

