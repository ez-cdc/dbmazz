// Copyright 2025
// Licensed under the Elastic License v2.0

//! Async normalizer: processes raw table batches → MERGE into target tables.
//!
//! Runs as a background tokio task. Wakes up on notification or every 2 seconds
//! (polling handles snapshot workers that don't share the notify channel).
//!
//! Processes batches ONE AT A TIME (like PeerDB) to avoid dedup issues with
//! MERGE across multiple batches. Each batch runs in a single transaction:
//! MERGE + metadata update + raw table cleanup (atomic).

use anyhow::{Context, Result};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info, warn};

use super::merge_generator;
use crate::core::traits::SourceTableSchema;

/// Metadata schema name
const METADATA_SCHEMA: &str = "_dbmazz";

/// Maximum backoff between retries (5 minutes)
const MAX_BACKOFF_SECS: u64 = 300;

/// Maximum retry attempts per batch before skipping
const MAX_RETRIES: u32 = 20;

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
/// Returns a sender for batch notifications.
pub fn spawn_normalizer(config: NormalizerConfig) -> mpsc::Sender<i64> {
    let (tx, rx) = mpsc::channel::<i64>(64);

    tokio::spawn(async move {
        if let Err(e) = normalizer_loop(config, rx).await {
            error!("Normalizer exited with error: {}", e);
        }
    });

    tx
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
    mut notify_rx: mpsc::Receiver<i64>,
) -> Result<()> {
    let mut client = connect(&config.url).await?;

    info!("Normalizer started (job: {})", config.job_name);

    let schema_map: HashMap<String, &SourceTableSchema> = config
        .table_schemas
        .iter()
        .map(|s| (format!("{}.{}", s.schema, s.name), s))
        .collect();

    loop {
        // Wait for notification OR poll every POLL_INTERVAL_SECS.
        // Polling is needed because snapshot workers use their own sink instances
        // (via sink_factory) and don't share the notify channel.
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(POLL_INTERVAL_SECS),
            notify_rx.recv(),
        )
        .await
        {
            Ok(Some(_)) => {} // Notification received
            Ok(None) => {
                // Channel closed → shutdown
                info!("Normalizer: channel closed, flushing remaining batches");
                let _ = process_pending(&mut client, &config, &schema_map).await;
                break;
            }
            Err(_) => {} // Timeout → poll
        }

        // Drain queued notifications
        while notify_rx.try_recv().is_ok() {}

        // Process all pending batches (one at a time)
        if let Err(e) = process_pending(&mut client, &config, &schema_map).await {
            error!("Normalizer error: {}, reconnecting...", e);
            match connect(&config.url).await {
                Ok(new_client) => {
                    client = new_client;
                    info!("Normalizer: reconnected successfully");
                }
                Err(reconnect_err) => {
                    error!("Normalizer: reconnection failed: {}", reconnect_err);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    info!("Normalizer shutdown complete");
    Ok(())
}

/// Process all pending batches (normalize_batch_id < sync_batch_id).
/// Processes ONE BATCH AT A TIME to avoid dedup issues with MERGE.
/// Retries each batch with exponential backoff on failure.
async fn process_pending(
    client: &mut Client,
    config: &NormalizerConfig,
    schema_map: &HashMap<String, &SourceTableSchema>,
) -> Result<()> {
    let (mut normalize_id, sync_id) = get_batch_range(client, &config.job_name).await?;

    if normalize_id >= sync_id {
        return Ok(()); // Nothing to do
    }

    let pending = sync_id - normalize_id;
    debug!(
        "Normalizer: {} pending batches ({} → {})",
        pending,
        normalize_id + 1,
        sync_id
    );

    // Process each batch individually (like PeerDB)
    while normalize_id < sync_id {
        let batch_id = normalize_id + 1;
        let mut attempt = 0u32;

        loop {
            match normalize_batch(
                client,
                &config.raw_table,
                &config.target_schema,
                &config.job_name,
                schema_map,
                batch_id,
            )
            .await
            {
                Ok(tables_processed) => {
                    if tables_processed > 0 {
                        debug!(
                            "Normalizer: batch {} done ({} tables)",
                            batch_id, tables_processed
                        );
                    }
                    break;
                }
                Err(e) => {
                    attempt += 1;

                    if attempt >= MAX_RETRIES {
                        error!(
                            "Normalizer: batch {} permanently failed after {} attempts, skipping. Last error: {}",
                            batch_id, attempt, e
                        );
                        // Advance metadata to unblock subsequent batches
                        let _ = client
                            .execute(
                                &format!(
                                    "UPDATE {}.\"_metadata\" SET normalize_batch_id = $1 WHERE job_name = $2",
                                    METADATA_SCHEMA
                                ),
                                &[&batch_id, &config.job_name],
                            )
                            .await;
                        break;
                    }

                    let backoff =
                        std::cmp::min((2u64.pow(attempt.min(8))) * 1000, MAX_BACKOFF_SECS * 1000);
                    error!(
                        "Normalizer: batch {} failed (attempt {}/{}): {}. Retrying in {}ms",
                        batch_id, attempt, MAX_RETRIES, e, backoff
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff)).await;
                }
            }
        }

        normalize_id = batch_id;
    }

    info!(
        "Normalizer: batches {} → {} done ({} batches)",
        sync_id - pending + 1,
        sync_id,
        pending
    );

    Ok(())
}

/// Normalize a single batch: MERGE raw table → target tables.
/// Runs entirely within a single transaction (MERGE + metadata + cleanup = atomic).
async fn normalize_batch(
    client: &mut Client,
    raw_table: &str,
    target_schema: &str,
    job_name: &str,
    schema_map: &HashMap<String, &SourceTableSchema>,
    batch_id: i64,
) -> Result<usize> {
    // Build all SQL statements first, then execute in one transaction
    let table_rows = client
        .query(
            &format!(
                "SELECT DISTINCT _dst_table FROM {} WHERE _batch_id = $1",
                raw_table
            ),
            &[&batch_id],
        )
        .await
        .with_context(|| format!("Failed to query distinct tables from {}", raw_table))?;

    if table_rows.is_empty() {
        // Empty batch — just advance metadata
        client
            .execute(
                &format!(
                    "UPDATE {}.\"_metadata\" SET normalize_batch_id = $1 WHERE job_name = $2",
                    METADATA_SCHEMA
                ),
                &[&batch_id, &job_name],
            )
            .await
            .context("Failed to update normalize_batch_id for empty batch")?;
        return Ok(0);
    }

    // Collect MERGE statements for all tables in this batch
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

        // Get unique TOAST column combinations for this batch + table
        let toast_rows = client
            .query(
                &format!(
                    "SELECT DISTINCT COALESCE(_toast_columns, '') FROM {}
                     WHERE _batch_id = $1
                       AND _dst_table = $2
                       AND _record_type != 2",
                    raw_table
                ),
                &[&batch_id, &dst_table],
            )
            .await
            .context("Failed to get TOAST combinations")?;

        let mut toast_combinations: Vec<String> =
            toast_rows.iter().map(|r| r.get::<_, String>(0)).collect();

        if toast_combinations.is_empty() || !toast_combinations.contains(&String::new()) {
            toast_combinations.insert(0, String::new());
        }

        let merge_sql = merge_generator::generate_merge(
            raw_table,
            target_schema,
            source_schema,
            &toast_combinations,
            batch_id,
        );

        merge_statements.push(merge_sql);
    }

    // Execute everything in a single transaction using tokio_postgres transaction API.
    // This ensures proper rollback on error and keeps the connection healthy.
    let tx = client
        .transaction()
        .await
        .context("Failed to begin normalize transaction")?;

    for stmt in &merge_statements {
        tx.batch_execute(stmt)
            .await
            .with_context(|| format!("MERGE failed for batch {}", batch_id))?;
    }

    // Update metadata (parameterized)
    tx.execute(
        &format!(
            "UPDATE {}.\"_metadata\" SET normalize_batch_id = $1 WHERE job_name = $2",
            METADATA_SCHEMA
        ),
        &[&batch_id, &job_name],
    )
    .await
    .context("Failed to update normalize_batch_id")?;

    // Cleanup raw table rows for this batch (parameterized)
    tx.execute(
        &format!("DELETE FROM {} WHERE _batch_id = $1", raw_table),
        &[&batch_id],
    )
    .await
    .context("Failed to cleanup raw table")?;

    tx.commit()
        .await
        .context("Failed to commit normalize transaction")?;

    debug!(
        "Normalizer: MERGE batch {} ({} tables)",
        batch_id,
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
