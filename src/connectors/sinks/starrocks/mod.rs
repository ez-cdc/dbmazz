// Copyright 2025
// Licensed under the Elastic License v2.0

//! # StarRocks Sink Connector
//!
//! This module implements a CDC sink for StarRocks, an OLAP database that
//! provides excellent real-time analytics capabilities. The sink uses
//! StarRocks' Stream Load HTTP API for high-throughput data ingestion.
//!
//! ## Features
//!
//! - **Streaming ingestion**: Uses Stream Load HTTP API with `Expect: 100-continue`
//! - **Upsert support**: Primary Key tables support automatic upsert/delete
//! - **Partial updates**: TOAST column optimization for PostgreSQL large values
//! - **Schema evolution**: Automatic column addition when source schema changes
//! - **Soft deletes**: CDC audit columns track operation type and deletion status
//!
//! ## Architecture
//!
//! ```text
//! CdcRecord ---> StarRocksSink ---> StreamLoadClient ---> StarRocks FE
//!                     |                    |                   |
//!                     v                    v                   v
//!               types.rs            stream_load.rs        HTTP 8040
//!               (mapping)           (curl loader)         (redirect)
//!                                                              |
//!                                                              v
//!                                                         StarRocks BE
//!                                                         HTTP 8040
//! ```
//!
//! ## CDC Audit Columns
//!
//! The sink adds audit columns to track CDC operations:
//! - `dbmazz_op_type`: Operation type (0=INSERT, 1=UPDATE, 2=DELETE)
//! - `dbmazz_is_deleted`: Soft delete flag for deletions
//! - `dbmazz_synced_at`: Timestamp when record was synced
//! - `dbmazz_cdc_version`: Source LSN/position for ordering
//!
//! ## Usage
//!
//! ```rust,ignore
//! use dbmazz::connectors::sinks::starrocks::StarRocksSink;
//! use dbmazz::config::SinkConfig;
//!
//! let sink = StarRocksSink::new(&config)?;
//! sink.validate_connection().await?;
//!
//! // Write batch of CDC records
//! let result = sink.write_batch(records).await?;
//! info!("Wrote {} records", result.records_written);
//! ```

mod config;
mod setup;
pub mod stream_load;
pub(crate) mod types;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::config::SinkConfig;
use crate::core::{
    CdcRecord, ColumnValue, LoadingModel, Sink, SinkCapabilities, SinkResult, SourcePosition,
};

pub use self::config::StarRocksSinkConfig;
use self::stream_load::{StreamLoadClient, StreamLoadOptions};
use self::types::TypeMapper;

/// CDC audit columns added to all tables
const AUDIT_COLUMNS: &[&str] = &[
    "dbmazz_op_type",
    "dbmazz_is_deleted",
    "dbmazz_synced_at",
    "dbmazz_cdc_version",
];

/// Tables internal to dbmazz that should not be replicated
fn is_internal_table(table_name: &str) -> bool {
    table_name.starts_with("dbmazz_")
        || table_name.starts_with("_dbmazz_")
        || table_name == "dbmazz_checkpoints"
}

/// StarRocks sink connector implementing the Sink trait.
///
/// This sink writes CDC records to StarRocks using the Stream Load HTTP API.
/// It supports upserts, deletes, and schema evolution through the Primary Key
/// table model.
pub struct StarRocksSink {
    /// Configuration for the StarRocks connection
    #[allow(dead_code)]
    config: StarRocksSinkConfig,
    /// HTTP Stream Load client
    stream_load: StreamLoadClient,
    /// Type mapper for converting CDC types to StarRocks types
    type_mapper: TypeMapper,
}

impl StarRocksSink {
    /// Creates a new StarRocks sink from the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Sink configuration containing connection details
    ///
    /// # Returns
    ///
    /// A new `StarRocksSink` instance ready for use
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn new(config: &SinkConfig) -> Result<Self> {
        let sr_config = StarRocksSinkConfig::from_sink_config(config)?;
        let stream_load = StreamLoadClient::new(
            sr_config.http_url.clone(),
            sr_config.database.clone(),
            sr_config.user.clone(),
            sr_config.password.clone(),
        );

        info!("StarRocksSink initialized:");
        info!("  HTTP URL: {}", sr_config.http_url);
        info!("  Database: {}", sr_config.database);
        info!("  MySQL Port: {}", sr_config.mysql_port);

        Ok(Self {
            config: sr_config,
            stream_load,
            type_mapper: TypeMapper::new(),
        })
    }

    /// Converts CDC records to JSON rows grouped by table.
    ///
    /// Returns a map of table name to (rows, optional partial columns).
    fn records_to_json_batches(
        &self,
        records: &[CdcRecord],
        synced_at: &str,
    ) -> Result<HashMap<String, (Vec<serde_json::Value>, Option<Vec<String>>)>> {
        let mut batches: HashMap<String, (Vec<serde_json::Value>, Option<Vec<String>>)> =
            HashMap::new();

        for record in records {
            match record {
                CdcRecord::Insert { table, columns, position } => {
                    if is_internal_table(&table.name) {
                        continue;
                    }

                    let mut row = self.columns_to_json(columns)?;
                    self.add_audit_columns(&mut row, 0, false, synced_at, position);

                    let key = table.qualified_name();
                    batches
                        .entry(key)
                        .or_insert_with(|| (Vec::new(), None))
                        .0
                        .push(row);
                }

                CdcRecord::Update { table, new_columns, position, .. } => {
                    if is_internal_table(&table.name) {
                        continue;
                    }

                    // Check for TOAST (unchanged) columns
                    let has_unchanged = new_columns.iter().any(|c| c.value.is_unchanged());

                    let (mut row, partial_cols) = if has_unchanged {
                        // Partial update: exclude unchanged columns
                        let (row, mut cols) = self.columns_to_json_selective(new_columns, true)?;
                        // Add audit columns to partial update list
                        cols.extend(AUDIT_COLUMNS.iter().map(|s| s.to_string()));
                        (row, Some(cols))
                    } else {
                        (self.columns_to_json(new_columns)?, None)
                    };

                    self.add_audit_columns(&mut row, 1, false, synced_at, position);

                    let key = table.qualified_name();
                    let entry = batches
                        .entry(key)
                        .or_insert_with(|| (Vec::new(), partial_cols.clone()));
                    entry.0.push(row);
                }

                CdcRecord::Delete { table, columns, position } => {
                    if is_internal_table(&table.name) {
                        continue;
                    }

                    let mut row = self.columns_to_json(columns)?;
                    self.add_audit_columns(&mut row, 2, true, synced_at, position);

                    let key = table.qualified_name();
                    batches
                        .entry(key)
                        .or_insert_with(|| (Vec::new(), None))
                        .0
                        .push(row);
                }

                // Schema changes, transactions, heartbeats don't need sink writes
                CdcRecord::SchemaChange { .. }
                | CdcRecord::Begin { .. }
                | CdcRecord::Commit { .. }
                | CdcRecord::Heartbeat { .. } => {}
            }
        }

        Ok(batches)
    }

    /// Converts column values to a JSON object.
    fn columns_to_json(&self, columns: &[ColumnValue]) -> Result<serde_json::Value> {
        let mut obj = serde_json::Map::new();
        for col in columns {
            let json_value = self.type_mapper.value_to_json(&col.value);
            obj.insert(col.name.clone(), json_value);
        }
        Ok(serde_json::Value::Object(obj))
    }

    /// Converts column values to JSON, optionally excluding unchanged (TOAST) values.
    /// Returns (json_object, included_column_names).
    fn columns_to_json_selective(
        &self,
        columns: &[ColumnValue],
        exclude_unchanged: bool,
    ) -> Result<(serde_json::Value, Vec<String>)> {
        let mut obj = serde_json::Map::new();
        let mut included = Vec::with_capacity(columns.len());

        for col in columns {
            if exclude_unchanged && col.value.is_unchanged() {
                continue;
            }
            let json_value = self.type_mapper.value_to_json(&col.value);
            obj.insert(col.name.clone(), json_value);
            included.push(col.name.clone());
        }

        Ok((serde_json::Value::Object(obj), included))
    }

    /// Adds CDC audit columns to a JSON row.
    fn add_audit_columns(
        &self,
        row: &mut serde_json::Value,
        op_type: i8,
        is_deleted: bool,
        synced_at: &str,
        position: &SourcePosition,
    ) {
        if let serde_json::Value::Object(obj) = row {
            obj.insert("dbmazz_op_type".to_string(), serde_json::json!(op_type));
            obj.insert("dbmazz_is_deleted".to_string(), serde_json::json!(is_deleted));
            obj.insert("dbmazz_synced_at".to_string(), serde_json::json!(synced_at));

            let version = match position {
                SourcePosition::Lsn(lsn) => *lsn as i64,
                SourcePosition::Offset(offset) => *offset,
                _ => 0,
            };
            obj.insert("dbmazz_cdc_version".to_string(), serde_json::json!(version));
        }
    }

    /// Sends a batch with exponential backoff retry.
    async fn send_with_retry(
        &self,
        table: &str,
        body: Arc<Vec<u8>>,
        partial_columns: Option<Vec<String>>,
        max_retries: u32,
    ) -> Result<u64> {
        let mut attempt = 0;

        loop {
            let options = StreamLoadOptions {
                partial_columns: partial_columns.clone(),
                max_filter_ratio: Some(0.2),
            };

            match self.stream_load.send(table, body.clone(), options).await {
                Ok(result) => return Ok(result.loaded_rows),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_retries {
                        return Err(anyhow!(
                            "Failed after {} attempts: {}",
                            max_retries,
                            e
                        ));
                    }

                    info!(
                        "Retry {}/{} for {}: {}",
                        attempt, max_retries, table, e
                    );

                    // Exponential backoff: 100ms, 200ms, 400ms...
                    tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(attempt))).await;
                }
            }
        }
    }
}

#[async_trait]
impl Sink for StarRocksSink {
    fn name(&self) -> &'static str {
        "starrocks"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: true,
            supports_transactions: false,
            loading_model: LoadingModel::Streaming,
            min_batch_size: Some(1),
            max_batch_size: Some(100_000),
            optimal_flush_interval_ms: 5000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        self.stream_load.verify_connection().await
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult {
                records_written: 0,
                bytes_written: 0,
                last_position: None,
            });
        }

        // Cache timestamp for entire batch
        let synced_at = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        // Track last position from records
        let last_position = records.iter().rev().find_map(|r| match r {
            CdcRecord::Insert { position, .. }
            | CdcRecord::Update { position, .. }
            | CdcRecord::Delete { position, .. }
            | CdcRecord::Commit { position, .. }
            | CdcRecord::Heartbeat { position, .. } => Some(position.clone()),
            _ => None,
        });

        // Convert records to JSON batches grouped by table
        let batches = self.records_to_json_batches(&records, &synced_at)?;

        let mut total_written = 0u64;
        let mut total_bytes = 0u64;

        // Send each table batch
        for (table, (rows, partial_cols)) in batches {
            if rows.is_empty() {
                continue;
            }

            // Serialize to JSON array
            let body = serde_json::to_vec(&rows)?;
            let body_len = body.len() as u64;
            let body = Arc::new(body);

            // Extract table name (remove schema prefix if present)
            let table_name = table.split('.').next_back().unwrap_or(&table);

            let written = self.send_with_retry(table_name, body, partial_cols, 3).await?;

            total_written += written;
            total_bytes += body_len;
        }

        Ok(SinkResult {
            records_written: total_written as usize,
            bytes_written: total_bytes,
            last_position,
        })
    }

    async fn close(&mut self) -> Result<()> {
        // Stream Load is stateless, nothing to close
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkType};
    use crate::config::StarRocksSinkConfig as ConfigStarRocksSinkConfig;

    fn test_config() -> SinkConfig {
        SinkConfig {
            sink_type: SinkType::StarRocks,
            url: "http://starrocks:8040".to_string(),
            port: 9030,
            database: "test_db".to_string(),
            user: "root".to_string(),
            password: "".to_string(),
            starrocks: Some(ConfigStarRocksSinkConfig {}),
        }
    }

    #[test]
    fn test_sink_creation() {
        let config = test_config();
        let sink = StarRocksSink::new(&config);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_capabilities() {
        let config = test_config();
        let sink = StarRocksSink::new(&config).unwrap();
        let caps = sink.capabilities();

        assert!(caps.supports_upsert);
        assert!(caps.supports_delete);
        assert!(caps.supports_schema_evolution);
        assert!(!caps.supports_transactions);
        assert!(matches!(caps.loading_model, LoadingModel::Streaming));
    }

    #[test]
    fn test_is_internal_table() {
        assert!(is_internal_table("dbmazz_checkpoints"));
        assert!(is_internal_table("dbmazz_metadata"));
        assert!(is_internal_table("_dbmazz_internal"));
        assert!(!is_internal_table("orders"));
        assert!(!is_internal_table("user_data"));
    }
}
