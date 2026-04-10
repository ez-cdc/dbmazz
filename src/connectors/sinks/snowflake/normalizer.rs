// Copyright 2025
// Licensed under the Elastic License v2.0

//! Async normalizer for Snowflake: processes raw table batches → MERGE into target tables.
//!
//! Runs as a background tokio task. Same pattern as PostgresSink normalizer:
//! event-driven (mpsc notification) + polling (2s timeout).
//!
//! Processes batches ONE AT A TIME to avoid MERGE dedup issues across batches.

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::client::SnowflakeClient;
use super::config::SnowflakeSinkConfig;
use super::merge_generator;
use crate::core::traits::SourceTableSchema;

/// Maximum backoff between retries (5 minutes)
const MAX_BACKOFF_SECS: u64 = 300;

/// Maximum retry attempts per batch before skipping
const MAX_RETRIES: u32 = 20;

/// Polling interval when no notification received (seconds)
const POLL_INTERVAL_SECS: u64 = 2;

/// Normalizer configuration
pub struct NormalizerConfig {
    pub client_config: SnowflakeSinkConfig,
    pub database: String,
    pub target_schema: String,
    pub job_name: String,
    pub table_schemas: Vec<SourceTableSchema>,
    #[allow(dead_code)]
    pub merge_interval_ms: u64,
    pub soft_delete: bool,
}

/// Start the normalizer as a background task.
/// Returns a sender for batch notifications.
pub fn spawn_normalizer(config: NormalizerConfig) -> mpsc::Sender<i64> {
    let (tx, rx) = mpsc::channel::<i64>(64);

    tokio::spawn(async move {
        if let Err(e) = normalizer_loop(config, rx).await {
            error!("Snowflake normalizer exited with error: {}", e);
        }
    });

    tx
}

/// Main normalizer loop: wait for notification → normalize pending batches.
async fn normalizer_loop(config: NormalizerConfig, mut rx: mpsc::Receiver<i64>) -> Result<()> {
    info!("Snowflake normalizer started");

    // Connect to Snowflake
    let client = Arc::new(
        SnowflakeClient::connect(&config.client_config)
            .await
            .context("Normalizer: failed to connect to Snowflake")?,
    );

    let raw_table = format!("_DBMAZZ._RAW_{}", config.job_name);

    loop {
        // Wait for notification or poll timeout
        let batch_id = tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(id) => Some(id),
                    None => {
                        info!("Normalizer channel closed, draining remaining batches...");
                        // Process any remaining batches then exit
                        if let Err(e) = drain_pending_batches(
                            &client, &config, &raw_table,
                        ).await {
                            error!("Error draining final batches: {}", e);
                        }
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS)) => {
                None // Poll timeout — check for pending batches
            }
        };

        debug!("Normalizer wakeup: batch_id={:?}", batch_id);

        // Process pending batches (from normalize_batch_id to sync_batch_id)
        if let Err(e) = drain_pending_batches(&client, &config, &raw_table).await {
            error!("Normalizer error: {}", e);
            // Continue loop — don't exit on transient errors
        }
    }

    info!("Snowflake normalizer stopped");
    Ok(())
}

/// Process all pending batches sequentially.
async fn drain_pending_batches(
    client: &SnowflakeClient,
    config: &NormalizerConfig,
    raw_table: &str,
) -> Result<()> {
    // Get current sync and normalize batch IDs from metadata
    let result = client
        .execute(&format!(
            "SELECT SYNC_BATCH_ID, NORMALIZE_BATCH_ID \
             FROM {}._DBMAZZ._METADATA \
             WHERE JOB_NAME = '{}'",
            config.database, config.job_name
        ))
        .await
        .context("Failed to read metadata")?;

    let (sync_batch_id, normalize_batch_id) = parse_batch_ids(&result)?;

    if normalize_batch_id >= sync_batch_id {
        return Ok(()); // Nothing to normalize
    }

    debug!(
        "Normalizer: sync={}, normalize={}, pending={}",
        sync_batch_id,
        normalize_batch_id,
        sync_batch_id - normalize_batch_id
    );

    // Process each pending batch sequentially
    for batch_id in (normalize_batch_id + 1)..=sync_batch_id {
        normalize_single_batch(client, config, raw_table, batch_id).await?;
    }

    Ok(())
}

/// Normalize a single batch: MERGE per table, then cleanup.
async fn normalize_single_batch(
    client: &SnowflakeClient,
    config: &NormalizerConfig,
    raw_table: &str,
    batch_id: i64,
) -> Result<()> {
    let mut retries = 0u32;

    loop {
        match normalize_batch_inner(client, config, raw_table, batch_id).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                retries += 1;
                if retries >= MAX_RETRIES {
                    error!(
                        "Normalizer: batch {} failed after {} retries, skipping: {:#}",
                        batch_id, MAX_RETRIES, e
                    );
                    // Update metadata to skip this batch
                    client
                        .execute(&format!(
                            "UPDATE {}._DBMAZZ._METADATA SET NORMALIZE_BATCH_ID = {} WHERE JOB_NAME = '{}'",
                            config.database, batch_id, config.job_name
                        ))
                        .await
                        .ok();
                    return Ok(());
                }

                let backoff = std::cmp::min(2u64.pow(retries), MAX_BACKOFF_SECS);
                warn!(
                    "Normalizer: batch {} retry {}/{} in {}s: {:#}",
                    batch_id, retries, MAX_RETRIES, backoff, e
                );
                tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            }
        }
    }
}

/// Inner normalize: get tables, get toast combos, MERGE per table, cleanup raw.
async fn normalize_batch_inner(
    client: &SnowflakeClient,
    config: &NormalizerConfig,
    raw_table: &str,
    batch_id: i64,
) -> Result<()> {
    // 1. Get distinct destination tables in this batch
    let tables_result = client
        .execute(&format!(
            "SELECT DISTINCT _DST_TABLE FROM {}.{} WHERE _BATCH_ID = {}",
            config.database, raw_table, batch_id
        ))
        .await
        .context("Failed to get distinct tables")?;

    let tables = parse_string_column(&tables_result);

    if tables.is_empty() {
        debug!("Batch {} has no records, skipping", batch_id);
        // Update metadata
        client
            .execute(&format!(
                "UPDATE {}._DBMAZZ._METADATA SET NORMALIZE_BATCH_ID = {} WHERE JOB_NAME = '{}'",
                config.database, batch_id, config.job_name
            ))
            .await?;
        return Ok(());
    }

    // 2. For each table: get TOAST combinations, generate MERGE, execute
    for dst_table in &tables {
        // Find the source schema for this table
        let table_name = dst_table.split('.').next_back().unwrap_or(dst_table);
        let source_schema = config.table_schemas.iter().find(|s| s.name == table_name);

        let source_schema = match source_schema {
            Some(s) => s,
            None => {
                warn!(
                    "No schema found for table {}, skipping in batch {}",
                    dst_table, batch_id
                );
                continue;
            }
        };

        // Get unique TOAST combinations for this table+batch
        let toast_result = client
            .execute(&format!(
                "SELECT DISTINCT COALESCE(_TOAST_COLUMNS, '') \
                 FROM {}.{} \
                 WHERE _BATCH_ID = {} AND _DST_TABLE = '{}'",
                config.database, raw_table, batch_id, dst_table
            ))
            .await
            .context("Failed to get TOAST combinations")?;

        let toast_combos = parse_string_column(&toast_result);
        let toast_combos = if toast_combos.is_empty() {
            vec!["".to_string()]
        } else {
            toast_combos
        };

        // Generate and execute MERGE
        let merge_sql = merge_generator::generate_merge(
            &config.database,
            raw_table,
            &config.target_schema,
            source_schema,
            &toast_combos,
            batch_id,
            config.soft_delete,
        );

        debug!("MERGE for {}.{} batch {}", dst_table, table_name, batch_id);
        client
            .execute(&merge_sql)
            .await
            .with_context(|| format!("MERGE failed for table {} batch {}", dst_table, batch_id))?;
    }

    // 3. Delete processed records from raw table
    client
        .execute(&format!(
            "DELETE FROM {}.{} WHERE _BATCH_ID = {}",
            config.database, raw_table, batch_id
        ))
        .await
        .context("Failed to cleanup raw table")?;

    // 4. Update metadata
    client
        .execute(&format!(
            "UPDATE {}._DBMAZZ._METADATA SET NORMALIZE_BATCH_ID = {} WHERE JOB_NAME = '{}'",
            config.database, batch_id, config.job_name
        ))
        .await
        .context("Failed to update metadata")?;

    info!("Normalized batch {} ({} tables)", batch_id, tables.len());
    Ok(())
}

/// Parse sync_batch_id and normalize_batch_id from query result.
fn parse_batch_ids(result: &super::client::QueryResult) -> Result<(i64, i64)> {
    if let Some(ref data) = result.data {
        if let Some(rows) = data.as_array() {
            if let Some(row) = rows.first() {
                if let Some(row_arr) = row.as_array() {
                    let sync = row_arr
                        .first()
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    let normalize = row_arr
                        .get(1)
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(0);
                    return Ok((sync, normalize));
                }
            }
        }
    }
    Ok((0, 0))
}

/// Parse a single string column from query result rows.
fn parse_string_column(result: &super::client::QueryResult) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(ref data) = result.data {
        if let Some(rows) = data.as_array() {
            for row in rows {
                if let Some(row_arr) = row.as_array() {
                    if let Some(val) = row_arr.first().and_then(|v| v.as_str()) {
                        values.push(val.to_string());
                    }
                }
            }
        }
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_batch_ids() {
        let result = super::super::client::QueryResult {
            success: true,
            data: Some(serde_json::json!([["5", "3"]])),
            query_id: "test".to_string(),
            message: String::new(),
        };

        let (sync, normalize) = parse_batch_ids(&result).unwrap();
        assert_eq!(sync, 5);
        assert_eq!(normalize, 3);
    }

    #[test]
    fn test_parse_batch_ids_empty() {
        let result = super::super::client::QueryResult {
            success: true,
            data: Some(serde_json::json!([])),
            query_id: "test".to_string(),
            message: String::new(),
        };

        let (sync, normalize) = parse_batch_ids(&result).unwrap();
        assert_eq!(sync, 0);
        assert_eq!(normalize, 0);
    }

    #[test]
    fn test_parse_string_column() {
        let result = super::super::client::QueryResult {
            success: true,
            data: Some(serde_json::json!([["public.orders"], ["public.items"],])),
            query_id: "test".to_string(),
            message: String::new(),
        };

        let values = parse_string_column(&result);
        assert_eq!(values, vec!["public.orders", "public.items"]);
    }
}
