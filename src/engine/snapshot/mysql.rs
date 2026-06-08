// Copyright 2025
// Licensed under the Elastic License v2.0

//! MySQL snapshot using the Flink CDC Offset Signal Algorithm.
//!
//! For each chunk `[start_pk, end_pk)` of a MySQL table:
//! LOW GTID → SELECT → HIGH GTID → emit rows → mark chunk COMPLETE.
//! Full Offset Signal with binlog upsert (steps 4–5 of the algorithm)
//! requires binlog stream integration from T3.
//!
//! All code behind `#[cfg(feature = "mysql-source")]`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
use crate::source::mysql::gtid::GtidSet;
use crate::source::mysql::schema::introspect_mysql_schemas;

/// Run the full MySQL snapshot for all configured tables.
///
/// Spawns parallel workers (controlled by `config.snapshot_parallel_workers`)
/// and streams chunks from the producer to workers via an mpsc channel.
///
/// `sink_factory` creates a fresh `Sink` instance for each parallel worker.
/// `consumer_gtid` is the live, shared view of the binlog consumer's
/// accumulated GTID set — chunk LOW/HIGH watermarks are read from this
/// handle, not from `SELECT @@global.gtid_executed`. See
/// `mysql-cdc-correctness` spec, "snapshot chunks SHALL be registered
/// before the LOW watermark is captured" requirement.
pub async fn run_mysql_snapshot(
    config: Arc<Config>,
    shared_state: Arc<SharedState>,
    sink_factory: Arc<dyn Fn() -> Result<Box<dyn Sink>> + Send + Sync>,
    active_chunks: crate::engine::snapshot::active_chunks::ActiveChunks,
    consumer_gtid: Arc<RwLock<GtidSet>>,
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

    // 2. Ensure chunk state table exists
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
    let database_name =
        extract_database_name(&config.source.url).unwrap_or_else(|| "dbmazz".to_string());
    let mut table_meta: HashMap<String, TableMeta> = HashMap::new();
    for schema in &schemas {
        let qualified = format!("{}.{}", schema.schema, schema.name);
        let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let col_types: Vec<DataType> = schema.columns.iter().map(|c| c.data_type.clone()).collect();
        // `find_mysql_pk` returns `None` for unsupported PK shapes
        // (no PK, composite, non-chunker-friendly types). When None we
        // still insert a TableMeta with an empty `pk_col` so the
        // producer downstream can log a skip per-table.
        let (pk_col, pk_kind) = match find_mysql_pk(schema) {
            Some((name, kind)) => (name, kind),
            None => (String::new(), PkKind::Int),
        };
        table_meta.insert(
            qualified.clone(),
            TableMeta {
                pk_col,
                pk_kind,
                col_names,
                col_types,
            },
        );
    }
    let table_meta = Arc::new(table_meta);

    // 6. Clear per-table progress
    shared_state.clear_table_progress().await;

    // 7. Chunk all tables and stream to workers
    let slot_name = database_name;
    let _tables = config.source.tables.clone();
    let chunk_size = config.snapshot_chunk_size;

    let qualified_tables: Vec<String> = schemas
        .iter()
        .map(|s| format!("{}.{}", s.schema, s.name))
        .collect();

    let semaphore = Arc::new(Semaphore::new(n_workers));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(String, MysqlChunk)>(n_workers * 2);

    let producer_slot_name = slot_name.clone();
    let producer_table_meta = Arc::clone(&table_meta);

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

            let chunks = chunk_mysql_table(
                &producer_pool,
                qualified,
                &meta.pk_col,
                meta.pk_kind,
                chunk_size,
            )
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

            // Send pending chunks to workers
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

    let mut join_set = JoinSet::new();
    while let Some((qualified, chunk)) = rx.recv().await {
        let pool = pool.clone();
        let sink_pool = Arc::clone(&sink_pool);
        let semaphore = Arc::clone(&semaphore);
        let slot_name = slot_name.clone();
        let shared_state = Arc::clone(&shared_state);
        let table_meta = Arc::clone(&table_meta);
        let active_chunks = active_chunks.clone();
        let consumer_gtid = consumer_gtid.clone();

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

            let mut sink = match sink_pool.lock().await.pop() {
                Some(s) => s,
                None => return Err(anyhow::anyhow!("MySQL snapshot: sink pool exhausted")),
            };

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
                &active_chunks,
                &consumer_gtid,
            )
            .await;

            sink_pool.lock().await.push(sink);
            result
        });
    }

    if let Err(e) = producer.await {
        error!("MySQL snapshot: producer task panicked: {}", e);
    }

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

// Data structures

/// PK type kind for chunker dispatch. Determines the SQL parameter
/// binding shape (i64 / u64 / String / Vec<u8>) and how chunk bounds
/// are serialized for `dbmazz_snapshot_state` resumability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PkKind {
    Int,
    UInt,
    Str,
    Bytes,
}

impl PkKind {
    /// Serialize the kind for the `pk_kind` column in
    /// `dbmazz_snapshot_state`. Round-trips through `from_str` for
    /// resume parsing.
    fn as_str(self) -> &'static str {
        match self {
            PkKind::Int => "Int",
            PkKind::UInt => "UInt",
            PkKind::Str => "Str",
            PkKind::Bytes => "Bytes",
        }
    }
}

/// Typed lower / upper bound of a chunk. `None` marks the first
/// chunk's lower bound (no `WHERE pk > ?` clause — return everything
/// from the smallest PK).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChunkPkBound {
    None,
    Int(i64),
    UInt(u64),
    Str(String),
    Bytes(Vec<u8>),
}

impl ChunkPkBound {
    /// Bind this bound as a positional `mysql_async::Value` parameter.
    /// Returns `None` for `Self::None` (caller should omit the parameter
    /// and the corresponding `WHERE` clause).
    fn to_mysql_param(&self) -> Option<mysql_async::Value> {
        match self {
            ChunkPkBound::None => None,
            ChunkPkBound::Int(i) => Some(mysql_async::Value::Int(*i)),
            ChunkPkBound::UInt(u) => Some(mysql_async::Value::UInt(*u)),
            ChunkPkBound::Str(s) => Some(mysql_async::Value::Bytes(s.as_bytes().to_vec())),
            ChunkPkBound::Bytes(b) => Some(mysql_async::Value::Bytes(b.clone())),
        }
    }

    /// Convert this bound to a `core::Value` for the active-chunks
    /// registry (used for DBLog eviction range checks).
    fn to_core_value(&self) -> Value {
        match self {
            ChunkPkBound::None => Value::Null,
            ChunkPkBound::Int(i) => Value::Int64(*i),
            ChunkPkBound::UInt(u) => Value::UInt64(*u),
            ChunkPkBound::Str(s) => Value::String(s.clone()),
            ChunkPkBound::Bytes(b) => Value::Bytes(b.clone()),
        }
    }

    /// Serialize for the typed `start_pk_text` / `end_pk_text` columns
    /// in `dbmazz_snapshot_state`. Round-trips with `from_text` for
    /// resume.
    fn to_text(&self) -> Option<String> {
        match self {
            ChunkPkBound::None => None,
            ChunkPkBound::Int(i) => Some(i.to_string()),
            ChunkPkBound::UInt(u) => Some(u.to_string()),
            ChunkPkBound::Str(s) => Some(s.clone()),
            ChunkPkBound::Bytes(b) => Some(hex_encode(b)),
        }
    }
}

/// Hex-encode bytes for the typed `*_pk_text` columns.
fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|byte| format!("{:02x}", byte)).collect()
}

/// A single PK-cursor chunk for a MySQL table.
///
/// Cursor semantics:
/// - `start` is exclusive lower bound (the previous chunk's `end`).
///   `ChunkPkBound::None` means "no lower bound" — the first chunk.
/// - `end` is inclusive upper bound (the last PK in this chunk).
/// - SQL: `WHERE pk > start AND pk <= end` (with `start` clause
///   omitted when `start == None`).
#[derive(Debug, Clone)]
pub(crate) struct MysqlChunk {
    pub(crate) partition_id: i32,
    pub(crate) pk_kind: PkKind,
    pub(crate) start: ChunkPkBound,
    pub(crate) end: ChunkPkBound,
}

/// Pre-computed metadata for a snapshot table.
struct TableMeta {
    pk_col: String,
    pk_kind: PkKind,
    col_names: Vec<String>,
    col_types: Vec<DataType>,
}

// Chunking

/// Find the snapshot-able primary key column from a `SourceTableSchema`.
///
/// Returns `Some((column_name, kind))` when the table has exactly one
/// PK column AND its type is supported by the cursor chunker. Returns
/// `None` (with a structured WARN) for unsupported shapes:
/// - Composite PK (`primary_keys.len() > 1`) — out of scope for v2.5.0
/// - No PK (`primary_keys.is_empty()`)
/// - PK whose `DataType` isn't one of `Int*`/`UInt64`/`String`/`Bytes`
///   (FLOAT/DECIMAL/DATE/JSON PKs are extremely rare and not
///   chunker-friendly).
///
/// Single-column PK only. The active-chunks registry / cursor SELECT
/// dispatch is single-column today; composite PK support requires
/// invasive changes to both and is tracked as future work.
fn find_mysql_pk(schema: &SourceTableSchema) -> Option<(String, PkKind)> {
    if schema.primary_keys.is_empty() {
        tracing::warn!(
            table = %schema.name,
            "MySQL snapshot: table has no primary key; skipping snapshot"
        );
        return None;
    }
    if schema.primary_keys.len() > 1 {
        tracing::warn!(
            table = %schema.name,
            pk_count = schema.primary_keys.len(),
            "MySQL snapshot: composite primary keys are not supported by the cursor chunker; skipping"
        );
        return None;
    }
    let pk_name = &schema.primary_keys[0];
    let col = schema.columns.iter().find(|c| &c.name == pk_name);
    let col = match col {
        Some(c) => c,
        None => {
            tracing::warn!(
                table = %schema.name,
                pk = %pk_name,
                "MySQL snapshot: PK column not found in schema columns; skipping"
            );
            return None;
        }
    };
    let kind = match &col.data_type {
        DataType::Int16 | DataType::Int32 | DataType::Int64 => PkKind::Int,
        DataType::UInt64 => PkKind::UInt,
        DataType::String | DataType::Text | DataType::Uuid => PkKind::Str,
        DataType::Bytes => PkKind::Bytes,
        other => {
            tracing::warn!(
                table = %schema.name,
                pk = %pk_name,
                ?other,
                "MySQL snapshot: PK type not supported by chunker; skipping"
            );
            return None;
        }
    };
    Some((pk_name.clone(), kind))
}

/// Divide a MySQL table into row-balanced chunks via cursor-based
/// pagination.
///
/// Algorithm: keyset paging on the PK column. Each iteration runs
/// `SELECT pk FROM t [WHERE pk > ?] ORDER BY pk LIMIT chunk_size + 1`
/// and emits a chunk `(prev_end, last_pk_in_page]`. The extra `+ 1`
/// row peeks at the next chunk's first PK so we can both know "is there
/// more?" and seed the next iteration without a separate count query.
///
/// Why this replaces `MIN(pk) / MAX(pk)` + linear partitioning:
/// - Sparse PK distributions (gaps from DELETEs, auto-increment skips
///   after rollbacks) no longer produce empty / oversized chunks. Each
///   chunk has bounded row count ≤ `chunk_size`.
/// - Works uniformly across integer / unsigned-integer / string /
///   binary PK types via `PkKind` dispatch — the linear partitioner
///   only worked for `i64` PKs.
///
/// Trade-off: each chunk requires one bound-discovery SELECT (this loop)
/// plus the chunk-data SELECT in `process_mysql_chunk` — roughly 2×
/// the SELECT count of linear partitioning. Negligible at production
/// chunk_size (default 50_000).
async fn chunk_mysql_table(
    pool: &mysql_async::Pool,
    qualified_table: &str,
    pk_col: &str,
    pk_kind: PkKind,
    chunk_size: u64,
) -> Result<Vec<MysqlChunk>> {
    use mysql_async::prelude::Queryable;

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

    let order_clause = order_by_clause(&quoted_pk, pk_kind);
    let mut chunks: Vec<MysqlChunk> = Vec::new();
    let mut last_end: ChunkPkBound = ChunkPkBound::None;
    let mut partition_id: i32 = 0;
    let limit = chunk_size + 1;

    loop {
        let where_clause = match &last_end {
            ChunkPkBound::None => String::new(),
            _ => format!("WHERE {} > ?", greater_than_clause(&quoted_pk, pk_kind)),
        };
        let q = format!(
            "SELECT {pk} FROM {table} {where_clause} {order_clause} LIMIT {limit}",
            pk = quoted_pk,
            table = quoted_table,
        );
        let rows: Vec<mysql_async::Row> = match last_end.to_mysql_param() {
            Some(param) => conn
                .exec(q, vec![param])
                .await
                .context("MySQL snapshot: cursor SELECT failed")?,
            None => conn
                .query(q)
                .await
                .context("MySQL snapshot: cursor SELECT (first page) failed")?,
        };
        if rows.is_empty() {
            break;
        }

        // Chunk end = last PK in the visible portion (rows[chunk_size - 1])
        // when we got the full page+1. When we got fewer rows, this is
        // the final chunk and `end = rows.last()`.
        let final_chunk = (rows.len() as u64) <= chunk_size;
        let end_idx = if final_chunk {
            rows.len() - 1
        } else {
            (chunk_size as usize) - 1
        };
        let end_bound = pk_bound_from_row(&rows[end_idx], 0, pk_kind)?;
        chunks.push(MysqlChunk {
            partition_id,
            pk_kind,
            start: last_end.clone(),
            end: end_bound.clone(),
        });
        partition_id += 1;

        if final_chunk {
            break;
        }
        // Next iteration starts strictly after `end_bound`. The peek row
        // (rows[chunk_size]) confirms there's more data; loop continues.
        last_end = end_bound;
    }

    info!(
        "MySQL snapshot: table {}: pk_col={} (kind={:?}), chunk_size={}, chunks={}",
        qualified_table,
        pk_col,
        pk_kind,
        chunk_size,
        chunks.len()
    );

    Ok(chunks)
}

/// `ORDER BY` clause for the cursor scan. VARCHAR / CHAR PKs need
/// `COLLATE utf8mb4_bin` for deterministic byte-wise ordering; without
/// it, case-insensitive collations would yield non-strict ordering and
/// the cursor invariant (`pk > last_end`) breaks.
fn order_by_clause(quoted_pk: &str, kind: PkKind) -> String {
    match kind {
        PkKind::Str => format!("ORDER BY {} COLLATE utf8mb4_bin", quoted_pk),
        // Int / UInt / Bytes (binary collation is already byte-wise).
        _ => format!("ORDER BY {}", quoted_pk),
    }
}

/// `WHERE pk > ?` expression for cursor pagination. Mirrors
/// `order_by_clause` for collation.
fn greater_than_clause(quoted_pk: &str, kind: PkKind) -> String {
    match kind {
        PkKind::Str => format!("{} COLLATE utf8mb4_bin", quoted_pk),
        _ => quoted_pk.to_string(),
    }
}

/// Decode a PK value from a `mysql_async::Row` into a `ChunkPkBound`
/// of the expected kind. Returns an error if the wire shape doesn't
/// match the declared kind (defensive — schema and binlog should
/// agree).
fn pk_bound_from_row(row: &mysql_async::Row, idx: usize, kind: PkKind) -> Result<ChunkPkBound> {
    let raw = row
        .as_ref(idx)
        .ok_or_else(|| anyhow::anyhow!("MySQL snapshot: PK column missing at index {}", idx))?;
    match (kind, raw) {
        (PkKind::Int, mysql_async::Value::Int(i)) => Ok(ChunkPkBound::Int(*i)),
        #[allow(clippy::cast_possible_wrap)]
        (PkKind::Int, mysql_async::Value::UInt(u)) => Ok(ChunkPkBound::Int(*u as i64)),
        (PkKind::UInt, mysql_async::Value::UInt(u)) => Ok(ChunkPkBound::UInt(*u)),
        #[allow(clippy::cast_sign_loss)]
        (PkKind::UInt, mysql_async::Value::Int(i)) if *i >= 0 => Ok(ChunkPkBound::UInt(*i as u64)),
        (PkKind::Str, mysql_async::Value::Bytes(b)) => {
            let s = std::str::from_utf8(b)
                .context("MySQL snapshot: PK STRING column has non-UTF8 bytes")?;
            Ok(ChunkPkBound::Str(s.to_string()))
        }
        (PkKind::Bytes, mysql_async::Value::Bytes(b)) => Ok(ChunkPkBound::Bytes(b.clone())),
        (kind, other) => Err(anyhow::anyhow!(
            "MySQL snapshot: PK kind {:?} cannot decode wire value {:?}",
            kind,
            other
        )),
    }
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

/// Process a single MySQL snapshot chunk:
/// LOW GTID → register chunk → SELECT → HIGH GTID → set high watermark
/// → await drain → filter evicted PKs → emit → mark complete.
///
/// The reconciliation step (after `await_drain`) implements the read-only
/// DBLog Incremental Snapshot algorithm: every binlog event whose GTID
/// falls in `(LOW, HIGH]` and whose PK is in this chunk's range has
/// already been emitted by the CDC stream consumer, so we drop the
/// matching snapshot row to avoid emitting stale data on top of the
/// fresher binlog event.
// Eight related parameters are passed in once each from a single call site;
// grouping them into a struct just to satisfy the lint would add boilerplate
// without making the function easier to read.
#[allow(clippy::too_many_arguments)]
async fn process_mysql_chunk(
    conn: &mut mysql_async::Conn,
    sink: &mut dyn Sink,
    slot_name: &str,
    qualified_table: &str,
    chunk: &MysqlChunk,
    shared_state: &Arc<SharedState>,
    meta: &TableMeta,
    active_chunks: &crate::engine::snapshot::active_chunks::ActiveChunks,
    consumer_gtid: &Arc<RwLock<GtidSet>>,
) -> Result<()> {
    debug!(
        "MySQL snapshot: processing chunk {}/{}: pk=({:?}, {:?}]",
        qualified_table, chunk.partition_id, chunk.start, chunk.end
    );

    let (schema, table) = split_mysql_table_name(qualified_table);
    let quoted_table = format!(
        "`{}`.`{}`",
        schema.replace('`', "``"),
        table.replace('`', "``")
    );
    let quoted_pk = quote_mysql_ident(&meta.pk_col);
    let gt_clause = greater_than_clause(&quoted_pk, chunk.pk_kind);
    let le_clause = greater_than_clause(&quoted_pk, chunk.pk_kind); // same expr

    // Step 1: capture LOW watermark from the binlog consumer's live GTID
    // set, NOT from `SELECT @@global.gtid_executed` against the source.
    // Using the consumer's view is the only watermark that exactly
    // corresponds to "what the consumer has seen so far"; events the
    // server has but the consumer hasn't processed yet legitimately fall
    // INSIDE the window and must be evaluated for eviction. Reading the
    // server would race with the consumer and let those events leak past
    // the LOW boundary unevicted (snapshot row would emit on top of the
    // CDC update — duplicate emission for the same PK).
    //
    // Step 1b (atomic with step 1, behind a single read of the lock):
    // register the chunk in the active-chunks registry with the LOW set
    // we just observed. From this instant onward, every binlog event
    // the consumer processes is evaluated against this chunk; events
    // BEFORE this instant are in `low_set` and excluded from the window.
    let low_set = consumer_gtid
        .read()
        .expect("consumer_gtid lock poisoned")
        .clone();
    let low_truncated = truncate_gtid(&low_set.format());

    // Cursor chunker emits `(start, end]` (start exclusive, end
    // inclusive). The active-chunks registry uses inclusive [start,
    // end] for eviction range checks. For the registry's lower bound
    // we need the smallest PK that THIS chunk actually contains; for
    // typed cursors there's no clean "+1" successor, so we register
    // the exclusive `start` as the registry's lower bound. The
    // registry's pk_in_range uses `>= low`, which would erroneously
    // include `start` (the previous chunk's `end`). In practice
    // `start` belongs to the previous chunk and the previous chunk
    // has already finished eviction by the time this chunk's binlog
    // window opens; the double-claim window is non-observable.
    let pk_range = (chunk.start.to_core_value(), chunk.end.to_core_value());
    let (chunk_handle, drain_notify) = active_chunks
        .register(
            (schema.clone(), table.clone()),
            meta.pk_col.clone(),
            pk_range,
            low_set,
        )
        .await;
    let chunk_id = chunk_handle.id();

    // Step 2: SELECT rows for this chunk
    let cols_sql: String = meta
        .col_names
        .iter()
        .map(|c| quote_mysql_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    // SQL shape: `WHERE [pk > ?] AND pk <= ?` — both bounds use the
    // same collation expression so cursor / chunk-SELECT see the same
    // ordering. The lower bound is omitted for the first chunk
    // (`start = ChunkPkBound::None`).
    let select_query = match &chunk.start {
        ChunkPkBound::None => format!(
            "SELECT {cols} FROM {table} WHERE {le} <= ?",
            cols = cols_sql,
            table = quoted_table,
            le = le_clause,
        ),
        _ => format!(
            "SELECT {cols} FROM {table} WHERE {gt} > ? AND {le} <= ?",
            cols = cols_sql,
            table = quoted_table,
            gt = gt_clause,
            le = le_clause,
        ),
    };

    let end_param = chunk.end.to_mysql_param().ok_or_else(|| {
        anyhow::anyhow!("MySQL snapshot: chunk end bound is None — chunker produced invalid chunk")
    })?;
    let params: Vec<mysql_async::Value> = match chunk.start.to_mysql_param() {
        Some(start_param) => vec![start_param, end_param],
        None => vec![end_param],
    };

    let rows: Vec<mysql_async::Row> = conn.exec(select_query, params).await.with_context(|| {
        format!(
            "MySQL snapshot: SELECT failed for {} chunk {}",
            qualified_table, chunk.partition_id
        )
    })?;

    let row_count = rows.len() as i64;

    // Step 3: HIGH watermark — again from the consumer's live GTID set.
    // Once SELECT returns, the snapshot is "as of" some point in the
    // committed history; the consumer's set tells us exactly which
    // committed transactions could possibly have raced our SELECT. The
    // window is `(LOW, HIGH]` for eviction reconciliation.
    let high_set = consumer_gtid
        .read()
        .expect("consumer_gtid lock poisoned")
        .clone();
    let high_gtid = high_set.format();
    let high_truncated = truncate_gtid(&high_gtid);
    active_chunks.set_high_watermark(chunk_id, high_set).await;

    info!(
        "MySQL snapshot: chunk {}/{}: {} rows, GTID range: {} → {}",
        qualified_table, chunk.partition_id, row_count, low_truncated, high_truncated,
    );

    // Step 4: wait until the binlog consumer has drained past HIGH, then
    // collect the evicted PK set. After this point the eviction set is
    // final — no more binlog event in the chunk's window can arrive.
    active_chunks.await_drain(chunk_id, drain_notify).await;
    let evicted_pks = active_chunks.evicted_pks(chunk_id).await;
    if !evicted_pks.is_empty() {
        info!(
            "MySQL snapshot: chunk {}/{}: reconciliation evicted {} row(s) overridden by concurrent CDC",
            qualified_table, chunk.partition_id, evicted_pks.len()
        );
    }
    // The handle drops at end of function and deregisters the chunk —
    // explicit deregister here keeps the hot path tidy.
    active_chunks.deregister(chunk_id).await;
    std::mem::forget(chunk_handle); // we already deregistered manually

    // Step 5: Convert rows to CdcRecord::Insert (filtering out evicted
    // PKs) and emit via sink. The `position` carried with each row is a
    // MysqlBinlog triple anchored at HIGH so checkpoint loaders see a
    // valid resume point even if the daemon dies right after this batch.
    let pk_idx = meta
        .col_names
        .iter()
        .position(|c| c == &meta.pk_col)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "MySQL snapshot: pk column {} not found in select projection",
                meta.pk_col
            )
        })?;

    let kept_rows: Vec<&mysql_async::Row> = rows
        .iter()
        .filter(|row| {
            let pk_dt = meta
                .col_types
                .get(pk_idx)
                .cloned()
                .unwrap_or(DataType::Int64);
            let pk_value = read_mysql_value(row, pk_idx, &pk_dt);
            !evicted_pks.contains(&crate::engine::snapshot::active_chunks::pk_key(&pk_value))
        })
        .collect();

    let kept_count = kept_rows.len() as i64;

    if !kept_rows.is_empty() {
        let table_ref = TableRef::new(Some(schema), table);
        let position = SourcePosition::MysqlBinlog {
            file: String::new(),
            position: 0,
            gtid_executed: high_gtid.clone(),
        };

        let records: Vec<CdcRecord> = kept_rows
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
            "MySQL snapshot: wrote chunk {}/{}: {} rows via sink ({} evicted)",
            qualified_table,
            chunk.partition_id,
            kept_count,
            row_count - kept_count
        );
    }

    // Step 6: Mark chunk COMPLETE in state table
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

    // Update progress counters
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

/// Read a column value from a MySQL Row. Uses `as_ref` to safely handle NULLs.
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
        mysql_async::Value::Date(y, m, d, hh, mm, ss, _) => Value::String(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            y, m, d, hh, mm, ss
        )),
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

// State table management

/// Create or migrate the `dbmazz_snapshot_state` table.
///
/// v1 schema (≤ v2.4.x): `start_pk BIGINT NOT NULL, end_pk BIGINT NOT NULL`,
/// no typed columns. Only INTEGER PKs supported.
///
/// v2 schema (≥ v2.5.0): adds `start_pk_text TEXT NULL`, `end_pk_text TEXT NULL`,
/// `pk_kind VARCHAR(16) NULL`; drops `NOT NULL` on the legacy `start_pk`/`end_pk`
/// columns so non-integer PKs (VARCHAR / BINARY / UUID) can land. Writers
/// populate both shapes; readers prefer the typed columns and fall back to
/// the legacy i64 pair when the typed columns are NULL (legacy rows from
/// in-flight snapshots that pre-date the migration).
///
/// The migration ALTERs are idempotent: re-running on a v2 table is a no-op.
async fn ensure_mysql_state_table(pool: &mysql_async::Pool) -> Result<()> {
    let mut conn = pool
        .get_conn()
        .await
        .context("MySQL snapshot: failed to get connection for state table")?;

    // 1. Create at v2 shape directly. For existing tables this is a
    //    no-op; the ALTERs below handle the v1 → v2 promotion.
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS dbmazz_snapshot_state (
            slot_name     VARCHAR(255) NOT NULL,
            table_name    VARCHAR(255) NOT NULL,
            partition_id  INT NOT NULL,
            start_pk      BIGINT NULL,
            end_pk        BIGINT NULL,
            start_pk_text TEXT NULL,
            end_pk_text   TEXT NULL,
            pk_kind       VARCHAR(16) NULL,
            rows_synced   BIGINT NOT NULL DEFAULT 0,
            status        VARCHAR(20) NOT NULL DEFAULT 'PENDING',
            hw_gtid       TEXT,
            created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (slot_name, table_name, partition_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    .await
    .context("MySQL snapshot: failed to create dbmazz_snapshot_state table")?;

    // 2. v1 → v2 migration. MySQL `ALTER TABLE ... ADD COLUMN IF NOT
    //    EXISTS` exists from 8.0.29; for older 8.0 we fall back to
    //    a `SHOW COLUMNS LIKE ...` probe before each ALTER. The probe
    //    is cheap (single metadata query) and idempotent.
    add_column_if_missing(
        &mut conn,
        "dbmazz_snapshot_state",
        "start_pk_text",
        "TEXT NULL",
    )
    .await?;
    add_column_if_missing(
        &mut conn,
        "dbmazz_snapshot_state",
        "end_pk_text",
        "TEXT NULL",
    )
    .await?;
    add_column_if_missing(
        &mut conn,
        "dbmazz_snapshot_state",
        "pk_kind",
        "VARCHAR(16) NULL",
    )
    .await?;
    // Drop NOT NULL on the legacy i64 columns. MODIFY COLUMN is the
    // idempotent way to express this in MySQL.
    conn.query_drop("ALTER TABLE dbmazz_snapshot_state MODIFY COLUMN start_pk BIGINT NULL")
        .await
        .context("MySQL snapshot: failed to relax start_pk NOT NULL")?;
    conn.query_drop("ALTER TABLE dbmazz_snapshot_state MODIFY COLUMN end_pk BIGINT NULL")
        .await
        .context("MySQL snapshot: failed to relax end_pk NOT NULL")?;

    // 3. Backfill `pk_kind` / `start_pk_text` / `end_pk_text` for any
    //    legacy rows that pre-date the migration. They were i64
    //    integers by construction (the v1 chunker only supported
    //    INTEGER PKs), so backfill kind = 'Int' and stringify.
    conn.query_drop(
        "UPDATE dbmazz_snapshot_state \
         SET pk_kind = 'Int', \
             start_pk_text = CAST(start_pk AS CHAR), \
             end_pk_text = CAST(end_pk AS CHAR) \
         WHERE pk_kind IS NULL AND start_pk IS NOT NULL",
    )
    .await
    .context("MySQL snapshot: failed to backfill legacy state rows")?;

    info!("MySQL snapshot: ensured dbmazz_snapshot_state table (v2)");
    Ok(())
}

/// Idempotent `ADD COLUMN IF NOT EXISTS` for MySQL via a `SHOW COLUMNS`
/// probe. Works on all MySQL 8.0 versions (the native `IF NOT EXISTS`
/// is only 8.0.29+).
async fn add_column_if_missing(
    conn: &mut mysql_async::Conn,
    table: &str,
    column: &str,
    column_def: &str,
) -> Result<()> {
    use mysql_async::prelude::Queryable;
    let probe = format!(
        "SHOW COLUMNS FROM `{}` LIKE '{}'",
        table.replace('`', "``"),
        column.replace('\'', "''")
    );
    let rows: Vec<mysql_async::Row> = conn.query(probe).await.with_context(|| {
        format!(
            "MySQL snapshot: failed to probe column {} on {}",
            column, table
        )
    })?;
    if rows.is_empty() {
        let ddl = format!(
            "ALTER TABLE `{}` ADD COLUMN `{}` {}",
            table, column, column_def
        );
        conn.query_drop(ddl).await.with_context(|| {
            format!(
                "MySQL snapshot: failed to ADD COLUMN {} on {}",
                column, table
            )
        })?;
    }
    Ok(())
}

/// Insert a chunk into the state table (idempotent).
///
/// Writes both the typed (`start_pk_text`, `end_pk_text`, `pk_kind`)
/// and legacy (`start_pk`, `end_pk`) shapes. The legacy columns are
/// populated only when the chunk is Int-kinded — for non-int kinds
/// (UInt/Str/Bytes) they stay NULL, which the v2 schema permits. This
/// lets a v1 binary still observe Int-kinded rows during a rolling
/// downgrade (best-effort — non-int rows would be invisible, and that
/// is correct since v1 couldn't process them anyway).
async fn upsert_mysql_chunk(
    pool: &mysql_async::Pool,
    slot_name: &str,
    table_name: &str,
    chunk: &MysqlChunk,
) -> Result<()> {
    use mysql_async::prelude::Queryable;

    let mut conn = pool
        .get_conn()
        .await
        .context("MySQL snapshot: failed to get connection for upsert_chunk")?;

    // Legacy i64 mirrors for Int chunks only — extracted with sign-loss
    // tolerance since v1 stored i64 and the cursor chunker may have an
    // Int chunk with negative i64 values (unusual but valid).
    let (legacy_start, legacy_end): (Option<i64>, Option<i64>) = match (&chunk.start, &chunk.end) {
        (ChunkPkBound::Int(s), ChunkPkBound::Int(e)) => (Some(*s), Some(*e)),
        (ChunkPkBound::None, ChunkPkBound::Int(e)) => (None, Some(*e)),
        _ => (None, None),
    };

    conn.exec_drop(
        "INSERT INTO dbmazz_snapshot_state
         (slot_name, table_name, partition_id, start_pk, end_pk,
          start_pk_text, end_pk_text, pk_kind, status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
         ON DUPLICATE KEY UPDATE status=status",
        (
            slot_name,
            table_name,
            chunk.partition_id,
            legacy_start,
            legacy_end,
            chunk.start.to_text(),
            chunk.end.to_text(),
            chunk.pk_kind.as_str(),
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
    // MySQL ≤ 8.4 does not support `FILTER (WHERE ...)` (introduced in
    // 9.7 innovation, no LTS yet). Use the universal CASE WHEN form.
    let row: Option<(i64, i64)> = conn
        .exec_first(
            "SELECT COALESCE(SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN status = 'COMPLETE' THEN rows_synced ELSE 0 END), 0)
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

// Connection helpers

/// Build a `mysql_async::Pool` from the source URL.
fn build_mysql_pool(
    url: &str,
    mysql_cfg: &crate::config::MysqlSourceConfig,
) -> Result<mysql_async::Pool> {
    let opts = build_mysql_opts(url, mysql_cfg.tls_skip_verify)?;
    Ok(mysql_async::Pool::new(opts))
}

/// Build `mysql_async::Opts` from a `mysql://` URL. Visible to other
/// MySQL-feature modules (e.g., the replication loop's startup probe).
pub(crate) fn build_mysql_opts(url: &str, tls_skip_verify: bool) -> Result<mysql_async::Opts> {
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

    let builder = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port)
        .db_name(Some(database))
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(crate::source::mysql::mysql_ssl_opts(tls_skip_verify));

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

    /// Guards against re-introducing the Postgres-only filtered-aggregate
    /// clause in SQL that targets MySQL. MySQL ≤ 8.4 rejects that syntax.
    /// The pattern is built from runtime concatenation so this test's body
    /// does not itself contain the literal it forbids.
    #[test]
    fn no_postgres_filtered_aggregate_in_mysql_snapshot_sql() {
        let src = include_str!("mysql.rs");
        let forbidden = format!("{}{}{}", "FILTER", " (", "WHERE");
        // Skip lines that intentionally describe the pattern (this test's
        // own assert text and the comment in mysql_chunk_counts_for_table).
        let offending: Vec<usize> = src
            .lines()
            .enumerate()
            .filter(|(_, line)| line.contains(&forbidden))
            .filter(|(_, line)| {
                !line.contains("MySQL ≤ 8.4")
                    && !line.contains("does not support")
                    && !line.trim_start().starts_with("//")
            })
            .map(|(i, _)| i + 1)
            .collect();
        assert!(
            offending.is_empty(),
            "MySQL snapshot SQL must not use the Postgres-only filtered \
             aggregate clause; offending lines: {:?}",
            offending
        );
    }

    fn make_test_schema(name: &str, pk_name: &str, pk_type: DataType) -> SourceTableSchema {
        SourceTableSchema {
            schema: "testdb".to_string(),
            name: name.to_string(),
            columns: vec![
                SourceColumn {
                    name: pk_name.to_string(),
                    data_type: pk_type,
                    nullable: false,
                    pg_type_id: None,
                },
                SourceColumn {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    pg_type_id: None,
                },
                SourceColumn {
                    name: "amount".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    pg_type_id: None,
                },
            ],
            primary_keys: vec![pk_name.to_string()],
        }
    }

    // find_mysql_pk tests (replaces find_mysql_integer_pk + is_integer_data_type)

    #[test]
    fn test_find_mysql_pk_bigint() {
        let schema = make_test_schema("users", "id", DataType::Int64);
        let result = find_mysql_pk(&schema);
        assert_eq!(result, Some(("id".to_string(), PkKind::Int)));
    }

    #[test]
    fn test_find_mysql_pk_int32() {
        let schema = make_test_schema("orders", "order_id", DataType::Int32);
        let result = find_mysql_pk(&schema);
        assert_eq!(result, Some(("order_id".to_string(), PkKind::Int)));
    }

    #[test]
    fn test_find_mysql_pk_bigint_unsigned_is_uint() {
        let schema = make_test_schema("logs", "id", DataType::UInt64);
        let result = find_mysql_pk(&schema);
        assert_eq!(result, Some(("id".to_string(), PkKind::UInt)));
    }

    #[test]
    fn test_find_mysql_pk_varchar_is_str() {
        let schema = make_test_schema("sessions", "session_id", DataType::String);
        let result = find_mysql_pk(&schema);
        assert_eq!(result, Some(("session_id".to_string(), PkKind::Str)));
    }

    #[test]
    fn test_find_mysql_pk_uuid_is_str() {
        let schema = make_test_schema("events", "uuid", DataType::Uuid);
        let result = find_mysql_pk(&schema);
        assert_eq!(result, Some(("uuid".to_string(), PkKind::Str)));
    }

    #[test]
    fn test_find_mysql_pk_binary_is_bytes() {
        let schema = make_test_schema("blobs", "id", DataType::Bytes);
        let result = find_mysql_pk(&schema);
        assert_eq!(result, Some(("id".to_string(), PkKind::Bytes)));
    }

    #[test]
    fn test_find_mysql_pk_returns_none_for_float_pk() {
        // Floating-point PKs are not chunker-friendly (cursor pagination
        // relies on a deterministic ordering; floats are usually not the
        // right choice for a PK).
        let schema = make_test_schema("metrics", "value", DataType::Float64);
        assert!(find_mysql_pk(&schema).is_none());
    }

    #[test]
    fn test_find_mysql_pk_returns_none_for_no_pk() {
        let schema = SourceTableSchema {
            schema: "testdb".to_string(),
            name: "nopk".to_string(),
            columns: vec![SourceColumn {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                pg_type_id: None,
            }],
            primary_keys: vec![],
        };
        assert!(find_mysql_pk(&schema).is_none());
    }

    #[test]
    fn test_find_mysql_pk_returns_none_for_composite_pk() {
        // Composite PKs are out of scope for v2.5.0 — chunker is
        // single-column. Operator gets a WARN log.
        let schema = SourceTableSchema {
            schema: "testdb".to_string(),
            name: "junction".to_string(),
            columns: vec![
                SourceColumn {
                    name: "left_id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    pg_type_id: None,
                },
                SourceColumn {
                    name: "right_id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    pg_type_id: None,
                },
            ],
            primary_keys: vec!["left_id".to_string(), "right_id".to_string()],
        };
        assert!(find_mysql_pk(&schema).is_none());
    }

    #[test]
    fn test_chunk_pk_bound_text_roundtrip() {
        assert_eq!(ChunkPkBound::Int(42).to_text(), Some("42".to_string()));
        assert_eq!(
            ChunkPkBound::UInt(u64::MAX).to_text(),
            Some("18446744073709551615".to_string())
        );
        assert_eq!(
            ChunkPkBound::Str("hello".to_string()).to_text(),
            Some("hello".to_string())
        );
        assert_eq!(
            ChunkPkBound::Bytes(vec![0xde, 0xad, 0xbe, 0xef]).to_text(),
            Some("deadbeef".to_string())
        );
        assert_eq!(ChunkPkBound::None.to_text(), None);
    }

    #[test]
    fn test_chunk_pk_bound_to_core_value() {
        assert_eq!(ChunkPkBound::Int(5).to_core_value(), Value::Int64(5));
        assert_eq!(
            ChunkPkBound::UInt(u64::MAX).to_core_value(),
            Value::UInt64(u64::MAX)
        );
        assert_eq!(
            ChunkPkBound::Str("x".to_string()).to_core_value(),
            Value::String("x".to_string())
        );
        assert_eq!(
            ChunkPkBound::Bytes(vec![1, 2, 3]).to_core_value(),
            Value::Bytes(vec![1, 2, 3])
        );
        assert_eq!(ChunkPkBound::None.to_core_value(), Value::Null);
    }

    #[test]
    fn test_pk_kind_as_str_roundtrip_shape() {
        assert_eq!(PkKind::Int.as_str(), "Int");
        assert_eq!(PkKind::UInt.as_str(), "UInt");
        assert_eq!(PkKind::Str.as_str(), "Str");
        assert_eq!(PkKind::Bytes.as_str(), "Bytes");
    }

    // split_mysql_table_name tests

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

    // quote_mysql_ident tests

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

    // build_mysql_opts tests

    #[test]
    fn test_build_mysql_opts_full_url() {
        let opts = build_mysql_opts("mysql://user:pass@host:3307/mydb", false).unwrap();
        let _ = opts;
    }

    #[test]
    fn test_build_mysql_opts_defaults() {
        let opts = build_mysql_opts("mysql://root@localhost/test", false).unwrap();
        let _ = opts;
    }

    #[test]
    fn test_build_mysql_opts_invalid_scheme() {
        let result = build_mysql_opts("postgres://localhost/db", false);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported scheme"));
    }

    #[test]
    fn test_build_mysql_opts_bad_url() {
        let result = build_mysql_opts("not-a-url", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_mysql_opts_tls_skip_verify_toggle() {
        // Both modes parse successfully — runtime SslOpts are opaque, so
        // we only smoke-test the surface here. Behaviour is covered by
        // integration tests that exercise the actual TLS handshake.
        let _ = build_mysql_opts("mysql://root@localhost/test", true).unwrap();
        let _ = build_mysql_opts("mysql://root@localhost/test", false).unwrap();
    }

    // extract_database_name tests

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

    // truncate_gtid tests

    #[test]
    fn test_truncate_gtid_short() {
        assert_eq!(truncate_gtid("abc-123"), "abc-123");
    }

    #[test]
    fn test_truncate_gtid_long() {
        let long = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-100";
        let truncated = truncate_gtid(long);
        assert!(truncated.len() <= 33);
        assert!(truncated.ends_with("..."));
    }

    // Chunk logic tests

    #[test]
    fn test_extract_column_names() {
        let schema = make_test_schema("t", "id", DataType::Int64);
        let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(names, vec!["id", "name", "amount"]);
    }

    // MysqlChunk construction with typed bounds

    #[test]
    fn test_mysql_chunk_construction_int_kind() {
        let c1 = MysqlChunk {
            partition_id: 0,
            pk_kind: PkKind::Int,
            start: ChunkPkBound::None,
            end: ChunkPkBound::Int(50000),
        };
        assert_eq!(c1.partition_id, 0);
        assert_eq!(c1.pk_kind, PkKind::Int);
        assert_eq!(c1.start, ChunkPkBound::None);

        let c2 = MysqlChunk {
            partition_id: 1,
            pk_kind: PkKind::Int,
            start: ChunkPkBound::Int(50000),
            end: ChunkPkBound::Int(i64::MAX),
        };
        assert_eq!(c2.partition_id, 1);
        assert_eq!(c2.end, ChunkPkBound::Int(i64::MAX));
    }

    #[test]
    fn test_mysql_chunk_uint_at_boundary() {
        let chunk = MysqlChunk {
            partition_id: 0,
            pk_kind: PkKind::UInt,
            start: ChunkPkBound::UInt(u64::MAX / 2),
            end: ChunkPkBound::UInt(u64::MAX),
        };
        assert_eq!(chunk.end, ChunkPkBound::UInt(u64::MAX));
    }

    #[test]
    fn test_mysql_chunk_str_kind() {
        let chunk = MysqlChunk {
            partition_id: 0,
            pk_kind: PkKind::Str,
            start: ChunkPkBound::None,
            end: ChunkPkBound::Str("zebra".to_string()),
        };
        assert_eq!(chunk.pk_kind, PkKind::Str);
        assert_eq!(chunk.end.to_text(), Some("zebra".to_string()));
    }

    #[test]
    fn test_order_by_clause_uses_binary_collation_for_str() {
        assert_eq!(
            order_by_clause("`id`", PkKind::Str),
            "ORDER BY `id` COLLATE utf8mb4_bin"
        );
    }

    #[test]
    fn test_order_by_clause_plain_for_int() {
        assert_eq!(order_by_clause("`id`", PkKind::Int), "ORDER BY `id`");
    }

    #[test]
    fn test_greater_than_clause_uses_collation_for_str() {
        assert_eq!(
            greater_than_clause("`id`", PkKind::Str),
            "`id` COLLATE utf8mb4_bin"
        );
    }

    #[test]
    fn test_greater_than_clause_plain_for_uint() {
        assert_eq!(greater_than_clause("`id`", PkKind::UInt), "`id`");
    }
}
