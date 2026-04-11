// Copyright 2025
// Licensed under the Elastic License v2.0

//! Async normalizer: processes raw table batches → MERGE into target tables.
//!
//! Runs as a background tokio task. Wakes up on notification or every 2 seconds
//! (polling handles snapshot workers that don't share the notify handle).
//!
//! Processes ALL pending batches in ONE MERGE pass per wake-up to eliminate
//! the per-batch serial latency that caused the previous 44-second CDC delay.
//! Each pass runs in a single transaction:
//! MERGE (range) + metadata update + raw table cleanup (atomic).

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info, warn};

use super::merge_generator;
use crate::core::traits::SourceTableSchema;

/// Metadata schema name
const METADATA_SCHEMA: &str = "_dbmazz";

/// Polling interval when no notification received (seconds)
const POLL_INTERVAL_SECS: u64 = 2;

/// Normalizer configuration
pub struct NormalizerConfig {
    pub url: String,
    pub target_schema: String,
    pub job_name: String,
    pub raw_table: String,
    pub table_schemas: Vec<SourceTableSchema>,
}

/// Start the normalizer as a background task.
///
/// Returns a `(Notify, AtomicBool, JoinHandle)` triple:
/// - `Arc<Notify>` — call `.notify_one()` after each batch write to wake the normalizer immediately.
/// - `Arc<AtomicBool>` — set to `true` to request a graceful shutdown (normalizer flushes then exits).
/// - `JoinHandle<()>` — await to confirm the normalizer has stopped.
pub fn spawn_normalizer(
    config: NormalizerConfig,
) -> (Arc<Notify>, Arc<AtomicBool>, JoinHandle<()>) {
    let notify = Arc::new(Notify::new());
    let shutdown = Arc::new(AtomicBool::new(false));

    let notify_clone = Arc::clone(&notify);
    let shutdown_clone = Arc::clone(&shutdown);

    let handle = tokio::spawn(async move {
        if let Err(e) = normalizer_loop(config, notify_clone, shutdown_clone).await {
            error!("Normalizer exited with error: {}", e);
        }
    });

    (notify, shutdown, handle)
}

/// Connect to target PostgreSQL.
async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .context("Normalizer: failed to connect to target PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Normalizer connection error: {}", e);
        }
    });

    Ok(client)
}

/// Main normalizer loop.
async fn normalizer_loop(
    config: NormalizerConfig,
    notify: Arc<Notify>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let mut client = connect(&config.url).await?;

    info!("Normalizer started (job: {})", config.job_name);

    let schema_map: HashMap<String, &SourceTableSchema> = config
        .table_schemas
        .iter()
        .map(|s| (format!("{}.{}", s.schema, s.name), s))
        .collect();

    loop {
        // Wait for an explicit notification OR fall back to polling.
        // Polling is necessary because snapshot workers use their own sink instances
        // and do not share the notify handle.
        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(POLL_INTERVAL_SECS)) => {}
        }

        if let Err(e) = process_pending(&mut client, &config, &schema_map).await {
            error!("Normalizer: process_pending error: {}", e);
            // Reconnect on any DB error; next loop iteration will retry the work.
            match connect(&config.url).await {
                Ok(new_client) => {
                    client = new_client;
                    info!("Normalizer: reconnected to target PostgreSQL");
                }
                Err(reconnect_err) => {
                    error!("Normalizer: reconnect failed: {}", reconnect_err);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }

        // Check shutdown AFTER processing so the final flush always happens.
        if shutdown.load(Ordering::Relaxed) {
            info!("Normalizer: shutdown requested, final flush...");
            let _ = process_pending(&mut client, &config, &schema_map).await;
            info!("Normalizer: shutdown complete");
            break;
        }
    }

    Ok(())
}

/// Process all pending batches in one MERGE pass.
///
/// Reads `(normalize_batch_id, sync_batch_id)` from metadata and issues a single
/// `normalize_batch_range` call covering the entire `(normalize_id, sync_id]` range,
/// eliminating the serial per-batch loop that caused the 44-second delay.
async fn process_pending(
    client: &mut Client,
    config: &NormalizerConfig,
    schema_map: &HashMap<String, &SourceTableSchema>,
) -> Result<()> {
    let (normalize_id, sync_id) = get_batch_range(client, &config.job_name).await?;

    if normalize_id >= sync_id {
        return Ok(());
    }

    let pending = sync_id - normalize_id;
    debug!(
        "Normalizer: {} pending batches ({}..{})",
        pending,
        normalize_id + 1,
        sync_id
    );

    // Process the ENTIRE range in one MERGE pass.
    let tables_processed = normalize_batch_range(
        client,
        &config.raw_table,
        &config.target_schema,
        &config.job_name,
        schema_map,
        normalize_id,
        sync_id,
    )
    .await?;

    if tables_processed > 0 || pending > 0 {
        info!(
            "Normalizer: {} batches done ({}..{}), {} tables merged",
            pending,
            normalize_id + 1,
            sync_id,
            tables_processed
        );
    }

    Ok(())
}

/// Normalize a batch range `(from_batch_id, to_batch_id]`: MERGE raw table → target tables.
///
/// Runs entirely within a single transaction (MERGE + metadata update + cleanup = atomic).
async fn normalize_batch_range(
    client: &mut Client,
    raw_table: &str,
    target_schema: &str,
    job_name: &str,
    schema_map: &HashMap<String, &SourceTableSchema>,
    from_batch_id: i64,
    to_batch_id: i64,
) -> Result<usize> {
    // Discover all destination tables that have rows in the range.
    let table_rows = client
        .query(
            &format!(
                "SELECT DISTINCT _dst_table FROM {} WHERE _batch_id > $1 AND _batch_id <= $2",
                raw_table
            ),
            &[&from_batch_id, &to_batch_id],
        )
        .await
        .with_context(|| format!("Failed to query distinct tables from {}", raw_table))?;

    if table_rows.is_empty() {
        // Empty range — just advance the metadata pointer.
        client
            .execute(
                &format!(
                    "UPDATE {}.\"_metadata\" SET normalize_batch_id = $1 WHERE job_name = $2",
                    METADATA_SCHEMA
                ),
                &[&to_batch_id, &job_name],
            )
            .await
            .context("Failed to update normalize_batch_id for empty range")?;
        return Ok(0);
    }

    // Collect MERGE statements for all tables in this range.
    let mut merge_statements: Vec<String> = Vec::with_capacity(table_rows.len());

    for row in &table_rows {
        let dst_table: String = row.get(0);

        let source_schema = match schema_map.get(&dst_table) {
            Some(s) => *s,
            None => {
                warn!("Normalizer: no schema for '{}', skipping", dst_table);
                continue;
            }
        };

        // Get unique TOAST column combinations across the entire range for this table.
        let toast_rows = client
            .query(
                &format!(
                    "SELECT DISTINCT COALESCE(_toast_columns, '') FROM {}
                     WHERE _batch_id > $1
                       AND _batch_id <= $2
                       AND _dst_table = $3
                       AND _record_type != 2",
                    raw_table
                ),
                &[&from_batch_id, &to_batch_id, &dst_table],
            )
            .await
            .context("Failed to get TOAST combinations")?;

        let mut toast_combinations: Vec<String> =
            toast_rows.iter().map(|r| r.get::<_, String>(0)).collect();

        if toast_combinations.is_empty() || !toast_combinations.contains(&String::new()) {
            toast_combinations.insert(0, String::new());
        }

        let merge_sql = merge_generator::generate_merge_range(
            raw_table,
            target_schema,
            source_schema,
            &toast_combinations,
            from_batch_id,
            to_batch_id,
        );

        merge_statements.push(merge_sql);
    }

    // Execute everything in a single transaction.
    let tx = client
        .transaction()
        .await
        .context("Failed to begin normalize transaction")?;

    for stmt in &merge_statements {
        tx.batch_execute(stmt).await.with_context(|| {
            format!(
                "MERGE failed for range ({}..{}]",
                from_batch_id, to_batch_id
            )
        })?;
    }

    // Advance the metadata pointer to the top of the processed range.
    tx.execute(
        &format!(
            "UPDATE {}.\"_metadata\" SET normalize_batch_id = $1 WHERE job_name = $2",
            METADATA_SCHEMA
        ),
        &[&to_batch_id, &job_name],
    )
    .await
    .context("Failed to update normalize_batch_id")?;

    // Clean up processed rows from the raw table.
    tx.execute(
        &format!(
            "DELETE FROM {} WHERE _batch_id > $1 AND _batch_id <= $2",
            raw_table
        ),
        &[&from_batch_id, &to_batch_id],
    )
    .await
    .context("Failed to cleanup raw table")?;

    tx.commit()
        .await
        .context("Failed to commit normalize transaction")?;

    debug!(
        "Normalizer: MERGE range ({}..{}] ({} tables)",
        from_batch_id,
        to_batch_id,
        merge_statements.len()
    );

    Ok(merge_statements.len())
}

/// Get current batch range from metadata.
async fn get_batch_range(client: &Client, job_name: &str) -> Result<(i64, i64)> {
    let row = client
        .query_one(
            &format!(
                "SELECT normalize_batch_id, sync_batch_id FROM {}.\"_metadata\" WHERE job_name = $1",
                METADATA_SCHEMA
            ),
            &[&job_name],
        )
        .await
        .context("Failed to read metadata")?;

    Ok((row.get(0), row.get(1)))
}
