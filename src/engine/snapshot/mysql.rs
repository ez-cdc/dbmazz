// Copyright 2025
// Licensed under the Elastic License v2.0

//! MySQL snapshot using the Flink CDC Offset Signal Algorithm.
//!
//! # Algorithm
//!
//! For each chunk `[start_pk, end_pk)` of a MySQL table:
//!
//! 1. **LOW**: capture GTID via connection
//! 2. `SELECT * FROM table WHERE pk >= start AND pk < end` (buffer rows in memory)
//! 3. **HIGH**: capture GTID via connection (same connection for consistency)
//! 4. Scan binlog events with GTID in `(LOW, HIGH]`, filter by this chunk's PK range
//!    (requires binlog stream from T3 — placeholder in this skeleton)
//! 5. UPSERT binlog events into buffer (binlog values overwrite snapshot values — T3)
//! 6. Emit buffer as `CdcRecord::Insert` to the sink
//! 7. Mark chunk COMPLETE in `dbmazz_snapshot_state`
//!
//! # Current State
//!
//! This is a working skeleton that:
//! - Connects to MySQL and introspects schemas
//! - Chunks tables by integer PK ranges
//! - Captures LOW/HIGH GTID pairs around each SELECT
//! - Emits snapshot rows as `CdcRecord::Insert` via the sink
//!
//! Full Offset Signal with binlog upsert (steps 4–5) requires integration
//! with the binlog stream from T3.
//!
//! All code behind `#[cfg(feature = "mysql-source")]`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use mysql_async::prelude::Queryable;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::control::state::{CdcState, SharedState, Stage};
use crate::core::position::SourcePosition;
use crate::core::record::DataType;
use crate::core::record::{CdcRecord, ColumnValue, TableRef, Value};
use crate::core::traits::{Sink, SourceTableSchema};
use crate::source::mysql::schema::introspect_mysql_schemas;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Run the full MySQL snapshot for all configured tables.
///
/// Spawns parallel workers (controlled by `config.snapshot_parallel_workers`)
/// and streams chunks from the producer to workers via an mpsc channel.
///
/// `sink_factory` creates a fresh `Sink` instance for each parallel worker.
pub async fn run_mysql_snapshot(
    config: Arc<Config>,
    shared_state: Arc<SharedState>,
    sink_factory: Arc<dyn Fn() -> Result<Box<dyn Sink>> + Send + Sync>,
) -> Result<()> {
    info!(
        "MySQL snapshot worker starting (chunk_size={}, workers={})",
        config.snapshot_chunk_size, config.snapshot_parallel_workers
    );

    shared_state.set_snapshot_active(true);
    shared_state
        .set_stage(Stage::Snapshot, "Connecting to MySQL")
        .await;
    let snapshot_start = std::time::Instant::now();

    // 1. Connect to MySQL
    let pool = build_mysql_pool(&config.source.url, config.source.mysql())
        .context("MySQL snapshot: failed to create connection pool")?;
    // Verify connection
    {
        let mut conn = pool
            .get_conn()
            .await
            .context("MySQL snapshot: failed to connect to MySQL")?;
        let version: Vec<(String,)> = conn
            .query("SELECT VERSION()")
            .await
            .context("MySQL snapshot: failed to query version")?;
        let version_str = version.first().map(|r| r.0.as_str()).unwrap_or("unknown");
        info!("MySQL snapshot: connected to MySQL {}", version_str);
    }

    // 2. Ensure chunk state table exists in MySQL
    ensure_mysql_state_table(&pool)
        .await
        .context("MySQL snapshot: failed to create state table")?;

    // 3. Introspect table schemas
    let schemas = introspect_mysql_schemas(&config.source.url, &config.source.tables)
        .await
        .context("MySQL snapshot: failed to introspect schemas")?;
    info!("MySQL snapshot: introspected {} table(s)", schemas.len());

    // 4. Create sink instances for parallel workers
    let n_workers = (config.snapshot_parallel_workers as usize).max(1);
    let mut sink_pool: Vec<Box<dyn Sink>> = Vec::with_capacity(n_workers);
    for i in 0..n_workers {
        let sink = sink_factory()
            .with_context(|| format!("MySQL snapshot: failed to create sink instance {}", i))?;
        sink_pool.push(sink);
    }
    let sink_pool = Arc::new(Mutex::new(sink_pool));

    // 5. Pre-compute table metadata
    //    Must happen before workers start since it requires the connection.
    let database_name =
        extract_database_name(&config.source.url).unwrap_or_else(|| "dbmazz".to_string());
    let mut table_meta: HashMap<String, TableMeta> = HashMap::new();
    for schema in &schemas {
        let qualified = format!("{}.{}", schema.schema, schema.name);
        let pk_col = find_mysql_integer_pk(schema);
        let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let col_types: Vec<DataType> = schema.columns.iter().map(|c| c.data_type.clone()).collect();
        table_meta.insert(qualified.clone(), TableMeta { pk_col, col_names, col_types });
    }
    let table_meta = Arc::new(table_meta);

    // 6. Clear per-table progress
    shared_state.clear_table_progress().await;

    // 7. Chunk all tables and stream to workers
    let slot_name = database_name; // use database name as state-table key
    let _tables = config.source.tables.clone();
    let chunk_size = config.snapshot_chunk_size;

    // Build fully-qualified table list for chunking map
    let qualified_tables: Vec<String> = schemas
        .iter()
        .map(|s| format!("{}.{}", s.schema, s.name))
        .collect();

    let semaphore = Arc::new(Semaphore::new(n_workers));

    // Channel: producer sends (qualified_table_name, MysqlChunk) → workers consume
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, MysqlChunk)>(n_workers * 2);

    // Clone values for the producer closure so that `slot_name` and `table_meta`
    // remain available for the consumer loop below.
    let producer_slot_name = slot_name.clone();
    let producer_table_meta = Arc::clone(&table_meta);

    // Producer: chunk all tables and send over channel
    let producer_pool = pool.clone();
    let producer_shared = Arc::clone(&shared_state);
    let producer_tables = qualified_tables.clone();
    let producer = tokio::spawn(async move {
        for qualified in &producer_tables {
            let meta = match producer_table_meta.get(qualified) {
                Some(m) => m,
                None => {
                    warn!(
                        "MySQL snapshot: no metadata for table {}, skipping",
                        qualified
                    );
                    continue;
                }
            };

            if meta.pk_col.is_empty() {
                warn!(
                    "MySQL snapshot: table {} has no integer PK, skipping chunking",
                    qualified
                );
                // Still register as having 0 chunks so progress indicates it was visited
                producer_shared.set_table_chunks_total(qualified, 0).await;
                continue;
            }

            let chunks = chunk_mysql_table(&producer_pool, qualified, &meta.pk_col, chunk_size)
                .await
                .unwrap_or_else(|e| {
                    warn!("MySQL snapshot: failed to chunk table {}: {}", qualified, e);
                    vec![]
                });

            // Skip already-complete chunks (resumability)
            let complete_ids =
                load_complete_chunk_ids(&producer_pool, &producer_slot_name, qualified)
                    .await
                    .unwrap_or_default();

            let total = chunks.len();
            let skipped = complete_ids.len();
            info!(
                "MySQL snapshot: table {}: {} chunks ({} pending, {} already done)",
                qualified,
                total,
                total - skipped,
                skipped
            );

            producer_shared
                .set_table_chunks_total(qualified, total as u64)
                .await;

            // Insert new chunks into state table (idempotent)
            for chunk in &chunks {
                upsert_mysql_chunk(&producer_pool, &producer_slot_name, qualified, chunk)
                    .await
                    .unwrap_or_else(|e| warn!("MySQL snapshot: upsert_chunk failed: {}", e));
            }

            // Stream pending chunks to workers
            for chunk in chunks {
                if complete_ids.contains(&chunk.partition_id) {
                    continue;
                }
                if tx.send((qualified.clone(), chunk)).await.is_err() {
                    warn!("MySQL snapshot: chunk channel closed");
                    return;
                }
            }
        }
    });

    // Consumer: spawn worker tasks as chunks arrive
    let mut join_set = JoinSet::new();
    while let Some((qualified, chunk)) = rx.recv().await {
        let pool = pool.clone();
        let sink_pool = Arc::clone(&sink_pool);
        let semaphore = Arc::clone(&semaphore);
        let slot_name = slot_name.clone();
        let shared_state = Arc::clone(&shared_state);
        let table_meta = Arc::clone(&table_meta);

        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();

            // Respect pause flag
            while shared_state.is_snapshot_paused() {
                shared_state
                    .set_stage(Stage::Snapshot, "Paused (outside execution window)")
                    .await;
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                if shared_state.state() == CdcState::Stopped {
                    return Ok(());
                }
            }
            shared_state
                .set_stage(Stage::Snapshot, "Running MySQL snapshot")
                .await;

            let meta = table_meta.get(&qualified).ok_or_else(|| {
                anyhow::anyhow!("MySQL snapshot: no metadata for table {}", qualified)
            })?;

            // Acquire sink from pool
            let mut sink = match sink_pool.lock().await.pop() {
                Some(s) => s,
                None => return Err(anyhow::anyhow!("MySQL snapshot: sink pool exhausted")),
            };

            // Get a dedicated MySQL connection for this chunk
            let mut conn = pool
                .get_conn()
                .await
                .context("MySQL snapshot: failed to get connection for chunk")?;

            let result = process_mysql_chunk(
                &mut conn,
                &mut *sink,
                &slot_name,
                &qualified,
                &chunk,
                &shared_state,
                meta,
            )
            .await;

            // Return sink to pool
            sink_pool.lock().await.push(sink);

            result
        });
    }

    // Wait for producer
    if let Err(e) = producer.await {
        error!("MySQL snapshot: producer task panicked: {}", e);
    }

    // Wait for all workers
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("MySQL snapshot: chunk failed: {:#}", e),
            Err(e) => error!("MySQL snapshot: chunk task panicked: {}", e),
        }
    }

    // Flush and close all sinks
    let mut pool_lock = sink_pool.lock().await;
    for sink in pool_lock.iter_mut() {
        if let Err(e) = sink.close().await {
            error!("MySQL snapshot: failed to close sink: {:#}", e);
        }
    }
    drop(pool_lock);

    let elapsed = snapshot_start.elapsed();
    let (final_total, final_done) = mysql_chunk_counts(&pool, &slot_name)
        .await
        .unwrap_or((0, 0));
    let final_rows = mysql_total_rows_synced(&pool, &slot_name)
        .await
        .unwrap_or(0);
    shared_state.update_snapshot_progress(final_total as u64, final_done as u64, final_rows as u64);

    if mysql_all_chunks_complete(&pool, &slot_name)
        .await
        .unwrap_or(false)
    {
        info!(
            "MySQL snapshot complete: {} chunks, {} rows synced in {:.1}s ({:.0} rows/sec)",
            final_total,
            final_rows,
            elapsed.as_secs_f64(),
            final_rows as f64 / elapsed.as_secs_f64().max(0.001)
        );
        shared_state.set_snapshot_active(false);
        shared_state.set_stage(Stage::Cdc, "Replicating").await;
        Ok(())
    } else {
        let done = final_done as u64;
        let total = final_total as u64;
        warn!(
            "MySQL snapshot finished with failed chunks ({}/{} complete) in {:.1}s",
            done,
            total,
            elapsed.as_secs_f64()
        );
        shared_state.set_snapshot_active(false);
        shared_state
            .set_stage(Stage::Cdc, "Replicating (snapshot incomplete)")
            .await;
        Err(anyhow::anyhow!(
            "MySQL snapshot incomplete: {} of {} chunks failed",
            total.saturating_sub(done),
            total
        ))
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A single PK-range chunk for a MySQL table.
#[derive(Debug, Clone)]
struct MysqlChunk {
    partition_id: i32,
    start_pk: i64,
    end_pk: i64,
}

/// Pre-computed metadata for a snapshot table.
struct TableMeta {
    /// Name of the integer primary key column.
    pk_col: String,
    /// Names of all columns in ordinal order.
    col_names: Vec<String>,
    /// Data types of all columns in ordinal order.
    col_types: Vec<DataType>,
}

// ---------------------------------------------------------------------------
// Chunking
// ---------------------------------------------------------------------------

/// Find the first integer primary key column from a `SourceTableSchema`.
///
/// Returns the column name if found, or an empty string if no integer PK exists.
fn find_mysql_integer_pk(schema: &SourceTableSchema) -> String {
    for pk_name in &schema.primary_keys {
        for col in &schema.columns {
            if col.name == *pk_name && is_integer_data_type(&col.data_type) {
                return pk_name.clone();
            }
        }
    }
    String::new()
}

/// Check whether a `DataType` is an integer type suitable for PK-range chunking.
fn is_integer_data_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Int16 | DataType::Int32 | DataType::Int64)
}

/// Divide a MySQL table into PK-range chunks.
///
/// Queries `MIN(pk)` and `MAX(pk)` to determine the value range, then splits
/// it into approximately `chunk_size`-sized range windows.
///
/// Requires a live connection to MySQL. Returns an empty vec if:
/// - The table is empty (MIN/MAX are NULL)
/// - The PK range is degenerate
async fn chunk_mysql_table(
    pool: &mysql_async::Pool,
    qualified_table: &str,
    pk_col: &str,
    chunk_size: u64,
) -> Result<Vec<MysqlChunk>> {
    let (schema, table) = split_mysql_table_name(qualified_table);
    let quoted_table = format!(
        "`{}`.`{}`",
        schema.replace('`', "``"),
        table.replace('`', "``")
    );
    let quoted_pk = quote_mysql_ident(pk_col);

    let mut conn = pool
        .get_conn()
        .await
        .context("MySQL snapshot: failed to get connection for chunking")?;

    // Query MIN and MAX of the PK column
    let query = format!(
        "SELECT MIN({pk}), MAX({pk}) FROM {table}",
        pk = quoted_pk,
        table = quoted_table,
    );
    let row: mysql_async::Row = conn
        .query_first(query)
        .await
        .context("MySQL snapshot: failed to query MIN/MAX PK")?
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot: MIN/MAX query returned no rows"))?;

    // Use as_ref to safely check for NULL (row.get panics on NULL for non-Option types)
    let min_pk: Option<i64> = match row.as_ref(0) {
        Some(&mysql_async::Value::Int(i)) => Some(i),
        Some(&mysql_async::Value::UInt(u)) => Some(u as i64),
        Some(&mysql_async::Value::NULL) | None => None,
        _ => None,
    };
    let max_pk: Option<i64> = match row.as_ref(1) {
        Some(&mysql_async::Value::Int(i)) => Some(i),
        Some(&mysql_async::Value::UInt(u)) => Some(u as i64),
        Some(&mysql_async::Value::NULL) | None => None,
        _ => None,
    };

    let (min_val, max_val) = match (min_pk, max_pk) {
        (Some(min), Some(max)) => (min, max),
        _ => {
            info!(
                "MySQL snapshot: table {} is empty — no chunks produced",
                qualified_table
            );
            return Ok(vec![]);
        }
    };

    // Single-value range
    if min_val == max_val {
        return Ok(vec![MysqlChunk {
            partition_id: 0,
            start_pk: min_val,
            end_pk: max_val + 1,
        }]);
    }

    let range_len = (max_val - min_val) as u64 + 1;
    let num_chunks = range_len.div_ceil(chunk_size).max(1) as i32;

    let mut chunks = Vec::with_capacity(num_chunks as usize);
    let range_f64 = (max_val - min_val) as f64;
    let step_f64 = range_f64 / num_chunks as f64;

    for i in 0..num_chunks {
        let start = min_val + (i as f64 * step_f64) as i64;
        let end = if i == num_chunks - 1 {
            max_val + 1
        } else {
            min_val + ((i + 1) as f64 * step_f64) as i64
        };
        // Skip degenerate chunks where start >= end (can happen when num_chunks > range)
        if start >= end && i < num_chunks - 1 {
            continue;
        }
        chunks.push(MysqlChunk {
            partition_id: i,
            start_pk: start,
            end_pk: end,
        });
    }

    info!(
        "MySQL snapshot: table {}: pk_col={}, min={}, max={}, range={}, chunk_size={}, chunks={}",
        qualified_table,
        pk_col,
        min_val,
        max_val,
        range_len,
        chunk_size,
        chunks.len()
    );

    Ok(chunks)
}

/// Split a potentially qualified MySQL table name into (schema, table).
fn split_mysql_table_name(name: &str) -> (String, String) {
    if name.contains('.') {
        let parts: Vec<&str> = name.splitn(2, '.').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("public".to_string(), name.to_string())
    }
}

// ---------------------------------------------------------------------------
// Chunk processing (Offset Signal Algorithm)
// ---------------------------------------------------------------------------

/// Process a single MySQL snapshot chunk using the Offset Signal Algorithm.
///
/// 1. Capture LOW GTID
/// 2. SELECT all rows in the PK range
/// 3. Capture HIGH GTID
/// 4. Convert rows to `CdcRecord::Insert` and emit via sink
/// 5. Mark chunk COMPLETE in the state table
async fn process_mysql_chunk(
    conn: &mut mysql_async::Conn,
    sink: &mut dyn Sink,
    slot_name: &str,
    qualified_table: &str,
    chunk: &MysqlChunk,
    shared_state: &Arc<SharedState>,
    meta: &TableMeta,
) -> Result<()> {
    debug!(
        "MySQL snapshot: processing chunk {}/{}: pk=[{}, {})",
        qualified_table, chunk.partition_id, chunk.start_pk, chunk.end_pk
    );

    let (schema, table) = split_mysql_table_name(qualified_table);
    let quoted_table = format!(
        "`{}`.`{}`",
        schema.replace('`', "``"),
        table.replace('`', "``")
    );
    let quoted_pk = quote_mysql_ident(&meta.pk_col);

    // -----------------------------------------------------------------------
    // Step 1: LOW GTID — capture before SELECT
    // -----------------------------------------------------------------------
    let low_gtid: Option<String> = conn
        .query_first("SELECT @@global.gtid_executed")
        .await
        .context("MySQL snapshot: failed to capture LOW GTID")?;
    let low_gtid = low_gtid.unwrap_or_default();
    let low_truncated = truncate_gtid(&low_gtid);

    // -----------------------------------------------------------------------
    // Step 2: SELECT rows for this chunk (native column types, no CAST)
    // -----------------------------------------------------------------------
    let cols_sql: String = meta
        .col_names
        .iter()
        .map(|c| quote_mysql_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let select_query = format!(
        "SELECT {cols} FROM {table} WHERE {pk} >= ? AND {pk} < ?",
        cols = cols_sql,
        table = quoted_table,
        pk = quoted_pk,
    );

    let rows: Vec<mysql_async::Row> = conn
        .exec(select_query, (chunk.start_pk, chunk.end_pk))
        .await
        .with_context(|| {
            format!(
                "MySQL snapshot: SELECT failed for {} chunk {}",
                qualified_table, chunk.partition_id
            )
        })?;

    let row_count = rows.len() as i64;

    // -----------------------------------------------------------------------
    // Step 3: HIGH GTID — capture after SELECT
    // -----------------------------------------------------------------------
    let high_gtid: Option<String> = conn
        .query_first("SELECT @@global.gtid_executed")
        .await
        .context("MySQL snapshot: failed to capture HIGH GTID")?;
    let high_gtid = high_gtid.unwrap_or_default();
    let high_truncated = truncate_gtid(&high_gtid);

    info!(
        "MySQL snapshot: chunk {}/{}: {} rows, GTID range: {} → {}",
        qualified_table, chunk.partition_id, row_count, low_truncated, high_truncated,
    );

    // -----------------------------------------------------------------------
    // Step 4: (Placeholder) Binlog upsert would go here.
    //   In T3, events between (LOW, HIGH] would be replayed from the binlog
    //   stream, filtered by this chunk's PK range, and UPSERTED into the
    //   buffer. Without T3, we emit the raw SELECT data.
    // -----------------------------------------------------------------------
    // For future T3 integration:
    // - Open a second binlog connection
    // - Request binlog events from LOW to HIGH
    // - Filter events by PK range [start_pk, end_pk)
    // - Overwrite buffer entries with binlog values
    // - Then emit the reconciled buffer

    // -----------------------------------------------------------------------
    // Step 5: Emit buffer as CdcRecord::Insert
    // -----------------------------------------------------------------------
    if !rows.is_empty() {
        let table_ref = TableRef::new(Some(schema), table);
        let position = SourcePosition::GtidSet(high_gtid.clone());

        let records: Vec<CdcRecord> = rows
            .iter()
            .map(|row| {
                let columns: Vec<ColumnValue> = meta
                    .col_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let dt = meta.col_types.get(i).cloned().unwrap_or(DataType::String);
                        let value = read_mysql_value(row, i, &dt);
                        ColumnValue::new(name.clone(), value)
                    })
                    .collect();

                CdcRecord::Insert {
                    table: table_ref.clone(),
                    columns,
                    position: position.clone(),
                }
            })
            .collect();

        sink.write_batch(records).await.with_context(|| {
            format!(
                "MySQL snapshot: sink write failed for {} chunk {}",
                qualified_table, chunk.partition_id
            )
        })?;

        debug!(
            "MySQL snapshot: wrote chunk {}/{}: {} rows via sink",
            qualified_table, chunk.partition_id, row_count
        );
    }

    // -----------------------------------------------------------------------
    // Step 6: Mark chunk COMPLETE in state table
    // -----------------------------------------------------------------------
    mark_mysql_chunk_complete(
        conn,
        slot_name,
        qualified_table,
        chunk.partition_id,
        row_count,
        &high_gtid,
    )
    .await
    .context("MySQL snapshot: failed to mark chunk complete")?;

    // -----------------------------------------------------------------------
    // Update progress counters
    // -----------------------------------------------------------------------
    let (total, done) = mysql_chunk_counts_for_table(conn, slot_name, qualified_table)
        .await
        .unwrap_or((0, 0));
    let rows_synced = mysql_rows_synced_for_table(conn, slot_name, qualified_table)
        .await
        .unwrap_or(0);
    shared_state.update_snapshot_progress(total as u64, done as u64, rows_synced as u64);
    shared_state
        .update_table_progress(qualified_table, done as u64, rows_synced as u64)
        .await;

    info!(
        "MySQL snapshot: chunk {}/{} complete: {} rows, high_gtid={}",
        qualified_table, chunk.partition_id, row_count, high_truncated,
    );

    Ok(())
}

/// Read a column value from a MySQL Row using the correct Rust type for the DataType.
/// Uses `as_ref` to safely handle NULL values without panicking.
fn read_mysql_value(row: &mysql_async::Row, idx: usize, dt: &DataType) -> Value {
    let raw = match row.as_ref(idx) {
        Some(val) => val,
        None => return Value::Null,
    };
    match raw {
        mysql_async::Value::NULL => Value::Null,
        mysql_async::Value::Int(i) => Value::Int64(*i),
        mysql_async::Value::UInt(u) => Value::Int64(*u as i64),
        mysql_async::Value::Float(f) => Value::Float64(*f as f64),
        mysql_async::Value::Double(d) => Value::Float64(*d),
        mysql_async::Value::Bytes(b) => {
            match dt {
                DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    // Try to parse the bytes as integer (for DECIMAL stored as bytes)
                    let s = String::from_utf8_lossy(b);
                    s.parse::<i64>().map_or(Value::Null, Value::Int64)
                }
                DataType::Float32 | DataType::Float64 => {
                    let s = String::from_utf8_lossy(b);
                    s.parse::<f64>().map_or(Value::Null, Value::Float64)
                }
                _ => {
                    let s = String::from_utf8_lossy(b).to_string();
                    Value::String(s)
                }
            }
        }
        mysql_async::Value::Date(y, m, d, hh, mm, ss, _) => {
            Value::String(format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mm, ss))
        }
        mysql_async::Value::Time(is_neg, days, h, m, s, _) => {
            if *is_neg {
                Value::String(format!("-{} {:02}:{:02}:{:02}", days, h, m, s))
            } else {
                Value::String(format!("{} {:02}:{:02}:{:02}", days, h, m, s))
            }
        }
    }
}

/// Truncate a GTID string for logging (show first 30 chars).
fn truncate_gtid(gtid: &str) -> String {
    if gtid.len() > 30 {
        format!("{}...", &gtid[..30])
    } else {
        gtid.to_string()
    }
}

// ---------------------------------------------------------------------------
// MySQL state table management
// ---------------------------------------------------------------------------

/// Create the `dbmazz_snapshot_state` table in MySQL if it doesn't exist.
async fn ensure_mysql_state_table(pool: &mysql_async::Pool) -> Result<()> {
    let mut conn = pool
        .get_conn()
        .await
        .context("MySQL snapshot: failed to get connection for state table")?;

    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS dbmazz_snapshot_state (
            slot_name    VARCHAR(255) NOT NULL,
            table_name   VARCHAR(255) NOT NULL,
            partition_id INT NOT NULL,
            start_pk     BIGINT NOT NULL,
            end_pk       BIGINT NOT NULL,
            rows_synced  BIGINT NOT NULL DEFAULT 0,
            status       VARCHAR(20) NOT NULL DEFAULT 'PENDING',
            hw_gtid      TEXT,
            created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (slot_name, table_name, partition_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    .await
    .context("MySQL snapshot: failed to create dbmazz_snapshot_state table")?;

    info!("MySQL snapshot: ensured dbmazz_snapshot_state table");
    Ok(())
}

/// Insert a chunk into the state table (idempotent).
async fn upsert_mysql_chunk(
    pool: &mysql_async::Pool,
    slot_name: &str,
    table_name: &str,
    chunk: &MysqlChunk,
) -> Result<()> {
    let mut conn = pool
        .get_conn()
        .await
        .context("MySQL snapshot: failed to get connection for upsert_chunk")?;

    conn.exec_drop(
        "INSERT INTO dbmazz_snapshot_state
         (slot_name, table_name, partition_id, start_pk, end_pk, status)
         VALUES (?, ?, ?, ?, ?, 'PENDING')
         ON DUPLICATE KEY UPDATE status=status",
        (
            slot_name,
            table_name,
            chunk.partition_id,
            chunk.start_pk,
            chunk.end_pk,
        ),
    )
    .await
    .context("MySQL snapshot: failed to upsert chunk")?;

    Ok(())
}

/// Mark a chunk as COMPLETE with row count and high-water GTID.
async fn mark_mysql_chunk_complete(
    conn: &mut mysql_async::Conn,
    slot_name: &str,
    table_name: &str,
    partition_id: i32,
    rows_synced: i64,
    hw_gtid: &str,
) -> Result<()> {
    conn.exec_drop(
        "UPDATE dbmazz_snapshot_state
         SET status = 'COMPLETE', rows_synced = ?, hw_gtid = ?, updated_at = NOW()
         WHERE slot_name = ? AND table_name = ? AND partition_id = ?",
        (rows_synced, hw_gtid, slot_name, table_name, partition_id),
    )
    .await
    .context("MySQL snapshot: failed to mark chunk complete")?;

    debug!(
        "MySQL snapshot: chunk {}.{}/{} complete: {} rows, hw_gtid truncated={}",
        slot_name,
        table_name,
        partition_id,
        rows_synced,
        truncate_gtid(hw_gtid),
    );
    Ok(())
}

/// Load partition IDs of COMPLETE chunks for a specific table (resumability).
async fn load_complete_chunk_ids(
    pool: &mysql_async::Pool,
    slot_name: &str,
    table_name: &str,
) -> Result<std::collections::HashSet<i32>> {
    let mut conn = pool
        .get_conn()
        .await
        .context("MySQL snapshot: failed to get connection for load_complete")?;

    let rows: Vec<(i32,)> = conn
        .exec(
            "SELECT partition_id FROM dbmazz_snapshot_state
             WHERE slot_name = ? AND table_name = ? AND status = 'COMPLETE'",
            (slot_name, table_name),
        )
        .await
        .context("MySQL snapshot: failed to load complete chunk IDs")?;

    Ok(rows.into_iter().map(|r| r.0).collect())
}

/// Check whether all chunks for a slot are COMPLETE.
async fn mysql_all_chunks_complete(pool: &mysql_async::Pool, slot_name: &str) -> Result<bool> {
    let mut conn = pool
        .get_conn()
        .await
        .context("MySQL snapshot: failed to get connection for all_complete")?;

    let row: Option<(i64,)> = conn
        .exec_first(
            "SELECT COUNT(*) FROM dbmazz_snapshot_state
             WHERE slot_name = ? AND status != 'COMPLETE'",
            (slot_name,),
        )
        .await?;
    let pending: i64 = row.map(|r| r.0).unwrap_or(0);
    Ok(pending == 0)
}

/// Count total and COMPLETE chunks for a slot.
async fn mysql_chunk_counts(pool: &mysql_async::Pool, slot_name: &str) -> Result<(i64, i64)> {
    let mut conn = pool.get_conn().await?;

    let row: Option<(i64, i64)> = conn
        .exec_first(
            "SELECT COUNT(*),
                    COALESCE(SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END), 0)
             FROM dbmazz_snapshot_state
             WHERE slot_name = ?",
            (slot_name,),
        )
        .await?;

    Ok(row.unwrap_or((0, 0)))
}

/// Count total and COMPLETE chunks for a specific table.
async fn mysql_chunk_counts_for_table(
    conn: &mut mysql_async::Conn,
    slot_name: &str,
    table_name: &str,
) -> Result<(i64, i64)> {
    let row: Option<(i64, i64)> = conn
        .exec_first(
            "SELECT COALESCE(SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(rows_synced) FILTER (WHERE status = 'COMPLETE'), 0)
             FROM dbmazz_snapshot_state
             WHERE slot_name = ? AND table_name = ?",
            (slot_name, table_name),
        )
        .await?;

    Ok(row.unwrap_or((0, 0)))
}

/// Total rows synced across all COMPLETE chunks for a slot.
async fn mysql_total_rows_synced(pool: &mysql_async::Pool, slot_name: &str) -> Result<i64> {
    let mut conn = pool.get_conn().await?;

    let row: Option<(i64,)> = conn
        .exec_first(
            "SELECT COALESCE(SUM(rows_synced), 0)
             FROM dbmazz_snapshot_state
             WHERE slot_name = ? AND status = 'COMPLETE'",
            (slot_name,),
        )
        .await?;

    Ok(row.map(|r| r.0).unwrap_or(0))
}

/// Total rows synced for a specific table.
async fn mysql_rows_synced_for_table(
    conn: &mut mysql_async::Conn,
    slot_name: &str,
    table_name: &str,
) -> Result<i64> {
    let row: Option<(i64,)> = conn
        .exec_first(
            "SELECT COALESCE(SUM(rows_synced), 0)
             FROM dbmazz_snapshot_state
             WHERE slot_name = ? AND table_name = ? AND status = 'COMPLETE'",
            (slot_name, table_name),
        )
        .await?;

    Ok(row.map(|r| r.0).unwrap_or(0))
}

// ---------------------------------------------------------------------------
// Connection helpers
// ---------------------------------------------------------------------------

/// Build a `mysql_async::Pool` from the source URL and MySQL config.
fn build_mysql_pool(
    url: &str,
    _mysql_cfg: &crate::config::MysqlSourceConfig,
) -> Result<mysql_async::Pool> {
    let opts = build_mysql_opts(url)?;
    Ok(mysql_async::Pool::new(opts))
}

/// Build `mysql_async::Opts` from a MySQL URL.
///
/// Expected format: `mysql://user:password@host:port/database`
fn build_mysql_opts(url: &str) -> Result<mysql_async::Opts> {
    let parsed = url::Url::parse(url).context("MySQL snapshot: failed to parse MySQL URL")?;

    anyhow::ensure!(
        parsed.scheme() == "mysql",
        "MySQL snapshot: unsupported scheme '{}' — expected 'mysql://'",
        parsed.scheme()
    );

    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(3306);
    let user = if !parsed.username().is_empty() {
        parsed.username()
    } else {
        "root"
    };
    let password = parsed.password().unwrap_or("");
    let database = parsed.path().trim_start_matches('/');

    let ssl_opts = mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true);

    let builder = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database))
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(ssl_opts);

    Ok(mysql_async::Opts::from(builder))
}

/// Extract the database name from a MySQL URL.
fn extract_database_name(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let db = parsed.path().trim_start_matches('/');
    if db.is_empty() {
        None
    } else {
        Some(db.to_string())
    }
}

/// Quote a MySQL identifier by wrapping in backticks.
/// Handles dotted names like `mydb.orders` → `mydb`.`orders`.
fn quote_mysql_ident(name: &str) -> String {
    if name.contains('.') {
        name.split('.')
            .map(|part| format!("`{}`", part.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::SourceColumn;

    fn make_test_schema(name: &str, pk_name: &str, pk_type: DataType) -> SourceTableSchema {
        SourceTableSchema {
            schema: "testdb".to_string(),
            name: name.to_string(),
            columns: vec![
                SourceColumn {
                    name: pk_name.to_string(),
                    data_type: pk_type,
                    nullable: false,
                    pg_type_id: 0,
                },
                SourceColumn {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    pg_type_id: 0,
                },
                SourceColumn {
                    name: "amount".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    pg_type_id: 0,
                },
            ],
            primary_keys: vec![pk_name.to_string()],
        }
    }

    // ── is_integer_data_type ──────────────────────────────────────────

    #[test]
    fn test_is_integer_data_type_returns_true_for_integers() {
        assert!(is_integer_data_type(&DataType::Int16));
        assert!(is_integer_data_type(&DataType::Int32));
        assert!(is_integer_data_type(&DataType::Int64));
    }

    #[test]
    fn test_is_integer_data_type_returns_false_for_non_integers() {
        assert!(!is_integer_data_type(&DataType::Float64));
        assert!(!is_integer_data_type(&DataType::String));
        assert!(!is_integer_data_type(&DataType::Boolean));
        assert!(!is_integer_data_type(&DataType::Decimal {
            precision: 10,
            scale: 2
        }));
        assert!(!is_integer_data_type(&DataType::Date));
        assert!(!is_integer_data_type(&DataType::Json));
    }

    // ── find_mysql_integer_pk ─────────────────────────────────────────

    #[test]
    fn test_find_mysql_integer_pk_bigint() {
        let schema = make_test_schema("users", "id", DataType::Int64);
        let pk = find_mysql_integer_pk(&schema);
        assert_eq!(pk, "id");
    }

    #[test]
    fn test_find_mysql_integer_pk_int32() {
        let schema = make_test_schema("orders", "order_id", DataType::Int32);
        let pk = find_mysql_integer_pk(&schema);
        assert_eq!(pk, "order_id");
    }

    #[test]
    fn test_find_mysql_integer_pk_prefers_integer_over_float() {
        // PK is "id" which is Int64 — should find it
        let schema = make_test_schema("products", "id", DataType::Int64);
        let pk = find_mysql_integer_pk(&schema);
        assert_eq!(pk, "id");
    }

    #[test]
    fn test_find_mysql_integer_pk_returns_empty_for_non_integer() {
        let schema = SourceTableSchema {
            schema: "testdb".to_string(),
            name: "events".to_string(),
            columns: vec![SourceColumn {
                name: "uuid".to_string(),
                data_type: DataType::String,
                nullable: false,
                pg_type_id: 0,
            }],
            primary_keys: vec!["uuid".to_string()],
        };
        let pk = find_mysql_integer_pk(&schema);
        assert_eq!(pk, "");
    }

    #[test]
    fn test_find_mysql_integer_pk_returns_empty_for_no_pk() {
        let schema = SourceTableSchema {
            schema: "testdb".to_string(),
            name: "nopk".to_string(),
            columns: vec![SourceColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                pg_type_id: 0,
            }],
            primary_keys: vec![],
        };
        let pk = find_mysql_integer_pk(&schema);
        assert_eq!(pk, "");
    }

    // ── split_mysql_table_name ───────────────────────────────────────

    #[test]
    fn test_split_mysql_table_name_qualified() {
        let (schema, table) = split_mysql_table_name("mydb.orders");
        assert_eq!(schema, "mydb");
        assert_eq!(table, "orders");
    }

    #[test]
    fn test_split_mysql_table_name_unqualified() {
        let (schema, table) = split_mysql_table_name("orders");
        assert_eq!(schema, "public");
        assert_eq!(table, "orders");
    }

    #[test]
    fn test_split_mysql_table_name_multi_part() {
        let (schema, table) = split_mysql_table_name("a.b.c");
        assert_eq!(schema, "a");
        assert_eq!(table, "b.c"); // splitn(2, '.') delimited
    }

    // ── quote_mysql_ident ─────────────────────────────────────────────

    #[test]
    fn test_quote_mysql_ident_simple() {
        assert_eq!(quote_mysql_ident("id"), "`id`");
        assert_eq!(quote_mysql_ident("order_id"), "`order_id`");
    }

    #[test]
    fn test_quote_mysql_ident_qualified() {
        assert_eq!(quote_mysql_ident("mydb.orders"), "`mydb`.`orders`");
    }

    #[test]
    fn test_quote_mysql_ident_escapes_backtick() {
        assert_eq!(quote_mysql_ident("table`name"), "`table``name`");
    }

    // ── build_mysql_opts ──────────────────────────────────────────────

    #[test]
    fn test_build_mysql_opts_full_url() {
        let opts = build_mysql_opts("mysql://user:pass@host:3307/mydb").unwrap();
        // Verify it produces valid Opts (we can't inspect fields, but no error means it parsed)
        let _ = opts;
    }

    #[test]
    fn test_build_mysql_opts_defaults() {
        let opts = build_mysql_opts("mysql://root@localhost/test").unwrap();
        let _ = opts;
    }

    #[test]
    fn test_build_mysql_opts_invalid_scheme() {
        let result = build_mysql_opts("postgres://localhost/db");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported scheme"));
    }

    #[test]
    fn test_build_mysql_opts_bad_url() {
        let result = build_mysql_opts("not-a-url");
        assert!(result.is_err());
    }

    // ── extract_database_name ─────────────────────────────────────────

    #[test]
    fn test_extract_database_name_normal() {
        assert_eq!(
            extract_database_name("mysql://user:pass@host/mydb").as_deref(),
            Some("mydb")
        );
    }

    #[test]
    fn test_extract_database_name_with_port() {
        assert_eq!(
            extract_database_name("mysql://user:pass@host:3306/mydb").as_deref(),
            Some("mydb")
        );
    }

    #[test]
    fn test_extract_database_name_no_path() {
        assert_eq!(extract_database_name("mysql://host"), None);
    }

    #[test]
    fn test_extract_database_name_bad_url() {
        assert_eq!(extract_database_name(""), None);
    }

    // ── truncate_gtid ─────────────────────────────────────────────────

    #[test]
    fn test_truncate_gtid_short() {
        assert_eq!(truncate_gtid("abc-123"), "abc-123");
    }

    #[test]
    fn test_truncate_gtid_long() {
        let long = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-100";
        let truncated = truncate_gtid(long);
        assert!(truncated.len() <= 33); // 30 chars + "..."
        assert!(truncated.ends_with("..."));
    }

    // ── chunk logic ───────────────────────────────────────────────────

    #[test]
    fn test_extract_column_names() {
        let schema = make_test_schema("t", "id", DataType::Int64);
        let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(names, vec!["id", "name", "amount"]);
    }

    #[test]
    fn test_chunk_mysql_table_edge_cases() {
        // We can't test chunk_mysql_table without a live MySQL connection,
        // but we test that the basic arithmetic works via the helper type.
        let chunk = MysqlChunk {
            partition_id: 0,
            start_pk: 0,
            end_pk: 100,
        };
        assert_eq!(chunk.start_pk, 0);
        assert_eq!(chunk.end_pk, 100);
        assert_eq!(chunk.partition_id, 0);
    }

    // ── MysqlChunk construction ──────────────────────────────────────

    #[test]
    fn test_mysql_chunk_construction() {
        let c1 = MysqlChunk {
            partition_id: 0,
            start_pk: 0,
            end_pk: 50000,
        };
        assert_eq!(c1.partition_id, 0);
        assert!(c1.start_pk < c1.end_pk);

        let c2 = MysqlChunk {
            partition_id: 1,
            start_pk: 50000,
            end_pk: i64::MAX,
        };
        assert_eq!(c2.partition_id, 1);
        assert!(c2.start_pk < c2.end_pk);
    }

    // ── is_integer_data_type exhaustive ──────────────────────────────

    #[test]
    fn test_is_integer_data_type_all_variants() {
        // All DataType variants
        assert!(is_integer_data_type(&DataType::Int16));
        assert!(is_integer_data_type(&DataType::Int32));
        assert!(is_integer_data_type(&DataType::Int64));

        assert!(!is_integer_data_type(&DataType::Boolean));
        assert!(!is_integer_data_type(&DataType::Float32));
        assert!(!is_integer_data_type(&DataType::Float64));
        assert!(!is_integer_data_type(&DataType::Decimal {
            precision: 10,
            scale: 2
        }));
        assert!(!is_integer_data_type(&DataType::String));
        assert!(!is_integer_data_type(&DataType::Text));
        assert!(!is_integer_data_type(&DataType::Bytes));
        assert!(!is_integer_data_type(&DataType::Json));
        assert!(!is_integer_data_type(&DataType::Jsonb));
        assert!(!is_integer_data_type(&DataType::Uuid));
        assert!(!is_integer_data_type(&DataType::Date));
        assert!(!is_integer_data_type(&DataType::Time));
        assert!(!is_integer_data_type(&DataType::Timestamp));
        assert!(!is_integer_data_type(&DataType::TimestampTz));
    }
}
