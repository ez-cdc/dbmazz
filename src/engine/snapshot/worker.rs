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

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::grpc::state::{SharedState, Stage};
use crate::connectors::sinks::starrocks::stream_load::{StreamLoadClient, StreamLoadOptions};
use crate::connectors::sinks::starrocks::StarRocksSinkConfig;
use super::chunker::{Chunk, chunk_table};
use super::state_store;

/// Run the full snapshot for all configured tables.
///
/// This function is spawned as a concurrent task alongside the WAL consumer.
/// It exits when all chunks are complete or on a non-retriable error.
pub async fn run_snapshot(
    config: Arc<Config>,
    shared_state: Arc<SharedState>,
) -> Result<()> {
    info!("Snapshot worker starting (chunk_size={}, workers={})",
        config.snapshot_chunk_size, config.snapshot_parallel_workers);

    shared_state.set_snapshot_active(true);
    shared_state.set_stage(Stage::Snapshot, "Connecting to source").await;

    // Connect to PostgreSQL (regular connection, not replication).
    // Strip `replication=database` from the URL — DDL is not allowed in replication mode.
    let plain_url = config.database_url
        .replace("&replication=database", "")
        .replace("?replication=database&", "?")
        .replace("?replication=database", "");
    let (client, connection) = tokio_postgres::connect(&plain_url, NoTls)
        .await
        .context("snapshot worker: failed to connect to PostgreSQL")?;

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
    let sl_client = StreamLoadClient::new(
        sr_config.http_url.clone(),
        config.starrocks_db.clone(),
        config.starrocks_user.clone(),
        config.starrocks_pass.clone(),
    );

    let slot_name = config.slot_name.clone();
    let tables = config.tables.clone();
    let chunk_size = config.snapshot_chunk_size;

    // Build all chunks for all tables and persist to state_store (idempotent)
    let mut all_chunks: Vec<(String, Chunk)> = Vec::new();
    for table in &tables {
        let chunks = chunk_table(&client, table, chunk_size).await
            .unwrap_or_else(|e| {
                warn!("Failed to chunk table {}: {}", table, e);
                vec![]
            });

        for chunk in &chunks {
            state_store::upsert_chunk(
                &client, &slot_name, table,
                chunk.partition_id, chunk.start_pk, chunk.end_pk,
            ).await.unwrap_or_else(|e| warn!("upsert_chunk failed: {}", e));
        }
        let n = chunks.len();
        info!("Table {}: {} chunks queued", table, n);
        all_chunks.extend(chunks.into_iter().map(|c| (table.clone(), c)));
    }

    // Load pending chunks (skips already-complete ones from previous runs)
    let pending = state_store::load_pending_chunks(&client, &slot_name).await?;
    let pending_set: std::collections::HashSet<(String, i32)> =
        pending.iter().map(|c| (c.table_name.clone(), c.partition_id)).collect();

    let chunks_to_run: Vec<(String, Chunk)> = all_chunks.into_iter()
        .filter(|(t, c)| pending_set.contains(&(t.clone(), c.partition_id)))
        .collect();

    let total = {
        let (total, _) = state_store::chunk_counts(&client, &slot_name).await?;
        total as u64
    };
    let done_at_start = {
        let (_, done) = state_store::chunk_counts(&client, &slot_name).await?;
        done as u64
    };
    let rows_at_start = state_store::total_rows_synced(&client, &slot_name).await? as u64;
    shared_state.update_snapshot_progress(total, done_at_start, rows_at_start);

    info!("Snapshot: {} total chunks, {} already done, {} to run",
        total, done_at_start, chunks_to_run.len());

    // Process chunks sequentially (parallel workers are future work)
    for (table, chunk) in &chunks_to_run {
        if let Err(e) = process_chunk(
            &client, &sl_client, &slot_name, table, chunk, &shared_state,
        ).await {
            error!("Chunk {}/{} failed: {:#}", table, chunk.partition_id, e);
            // Continue with next chunk — failed chunks will be retried on restart
        }
    }

    // Update final progress
    let (final_total, final_done) = state_store::chunk_counts(&client, &slot_name).await?;
    let final_rows = state_store::total_rows_synced(&client, &slot_name).await?;
    shared_state.update_snapshot_progress(
        final_total as u64, final_done as u64, final_rows as u64,
    );

    if state_store::all_chunks_complete(&client, &slot_name).await? {
        info!("Snapshot complete: {} chunks, {} rows synced", final_total, final_rows);
        shared_state.set_snapshot_active(false);
        shared_state.set_stage(Stage::Cdc, "Replicating").await;
    } else {
        warn!("Snapshot finished with some failed chunks — will retry on next start");
        shared_state.set_snapshot_active(false);
        shared_state.set_stage(Stage::Cdc, "Replicating (snapshot incomplete)").await;
    }

    Ok(())
}

/// Process a single chunk: LW watermark → SELECT → Stream Load → HW watermark → mark complete.
async fn process_chunk(
    client: &Client,
    sl_client: &StreamLoadClient,
    slot_name: &str,
    table: &str,
    chunk: &Chunk,
    shared_state: &SharedState,
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
    let dest_table = table.split('.').last().unwrap_or(table);

    // Find the integer PK column name
    let pk_col = find_pk_col(client, table).await
        .context("failed to find PK column")?
        .ok_or_else(|| anyhow::anyhow!("no integer PK found for table {}", table))?;

    // Get column names for this table so we can cast all to text (handles any PG type)
    let col_names = get_column_names(client, table).await
        .with_context(|| format!("failed to get columns for {}", table))?;

    // Build SELECT with all columns cast to ::text for universal type handling
    let cols_sql: String = col_names.iter()
        .map(|c| format!("{}::text AS {}", c, c))
        .collect::<Vec<_>>()
        .join(", ");
    let select_query = format!(
        "SELECT {cols} FROM {table} WHERE {pk} >= $1::bigint AND {pk} < $2::bigint",
        cols = cols_sql,
        table = table,
        pk = pk_col,
    );
    let rows = client.query(&select_query, &[&chunk.start_pk, &chunk.end_pk])
        .await
        .with_context(|| format!("SELECT failed for {} chunk {}", table, chunk.partition_id))?;

    let row_count = rows.len() as i64;

    // Step 3: Serialize rows to JSON (for Stream Load)
    // All columns are text thanks to ::text cast, so we just read Option<String>
    let body = if rows.is_empty() {
        b"[]".to_vec()
    } else {
        serialize_text_rows_to_json(&rows, &col_names)?
    };

    // Step 4: Stream Load to StarRocks (only if there are rows)
    if !rows.is_empty() {
        let body_arc = Arc::new(body);
        let result: crate::connectors::sinks::starrocks::stream_load::StreamLoadResult =
            sl_client.send(dest_table, body_arc, StreamLoadOptions::default())
            .await
            .with_context(|| format!("Stream Load failed for {} chunk {}", table, chunk.partition_id))?;

        debug!("Stream Load chunk {}/{}: {} rows loaded", table, chunk.partition_id, result.loaded_rows);
    }

    // Step 5: Emit HW (high watermark) — returns the LSN of this message
    let hw_content = format!("HW:{}:{}:{}", table, chunk.start_pk, chunk.end_pk);
    let hw_row = client.query_one(
        "SELECT pg_logical_emit_message(false, 'dbmazz', $1)::text",
        &[&hw_content],
    ).await.context("failed to emit HW watermark")?;

    // pg_logical_emit_message returns pg_lsn (displayed as hex string like "0/1234AB")
    let hw_lsn_str: String = hw_row.get(0);
    let hw_lsn = parse_pg_lsn(&hw_lsn_str).unwrap_or(0);

    // Step 6: Mark chunk complete in state_store
    state_store::mark_chunk_complete(
        client, slot_name, table, chunk.partition_id, row_count, hw_lsn as i64,
    ).await?;

    // Step 7: Register in SharedState so WAL consumer can deduplicate
    shared_state.register_finished_chunk(chunk.start_pk, chunk.end_pk, hw_lsn).await;

    // Update progress counters
    let (total, done) = state_store::chunk_counts(client, slot_name).await?;
    let rows_synced = state_store::total_rows_synced(client, slot_name).await?;
    shared_state.update_snapshot_progress(total as u64, done as u64, rows_synced as u64);

    info!("Chunk {}/{} complete: {} rows, hw_lsn=0x{:X}",
        table, chunk.partition_id, row_count, hw_lsn);

    Ok(())
}

/// Serialize tokio_postgres rows to a JSON array string for Stream Load.
/// Format: `[{"col1":"val1","col2":"val2"}, ...]`
/// Serialize rows to JSON where all columns were cast to ::text in the query.
/// Each column is read as Option<String> — no type-specific conversions needed.
fn serialize_text_rows_to_json(rows: &[tokio_postgres::Row], col_names: &[String]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(b'[');

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
                    // Escape special JSON characters
                    for ch in s.chars() {
                        match ch {
                            '"' => out.extend_from_slice(b"\\\""),
                            '\\' => out.extend_from_slice(b"\\\\"),
                            '\n' => out.extend_from_slice(b"\\n"),
                            '\r' => out.extend_from_slice(b"\\r"),
                            '\t' => out.extend_from_slice(b"\\t"),
                            c => {
                                let mut buf = [0u8; 4];
                                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                            }
                        }
                    }
                    out.push(b'"');
                }
                None => out.extend_from_slice(b"null"),
            }
        }
        out.push(b'}');
    }

    out.push(b']');
    Ok(out)
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

/// Find the first integer PK column name for a table.
async fn find_pk_col(client: &Client, table_name: &str) -> Result<Option<String>> {
    let (schema, table) = if table_name.contains('.') {
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    let rows = client.query(
        "SELECT a.attname
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
         JOIN pg_class c ON c.oid = i.indrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_type t ON t.oid = a.atttypid
         WHERE i.indisprimary
           AND n.nspname = $1
           AND c.relname = $2
           AND t.typname IN ('int2', 'int4', 'int8')
         ORDER BY a.attnum
         LIMIT 1",
        &[&schema, &table],
    ).await?;

    Ok(rows.first().map(|r| r.get::<_, String>(0)))
}
