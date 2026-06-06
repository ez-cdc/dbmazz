// Copyright 2025
// Licensed under the Elastic License v2.0

//! # Oracle Sink Connector
//!
//! This module implements a CDC sink for Oracle Database, an OLTP RDBMS.
//! The sink uses Oracle's MERGE INTO statement for idempotent upserts and
//! the pure Rust `oracle-rs` driver for async database connectivity.
//!
//! ## Features
//!
//! - **Upsert support**: Uses Oracle `MERGE INTO ... USING DUAL` pattern for
//!   idempotent writes (at-least-once delivery safe)
//! - **Delete support**: Propagates CDC delete events as hard deletes
//! - **Schema evolution**: Automatic column addition when source schema changes
//!   via `ALTER TABLE ADD`
//! - **Batch writes**: Configurable batch size for efficient bulk operations
//! - **CDC audit columns**: Tracks operation type, deletion status, sync time,
//!   and CDC version
//!
//! ## Architecture
//!
//! ```text
//! CdcRecord ---> OracleSink ---> OracleClient ---> Oracle Database
//!                     |               |
//!                     v               v
//!               types.rs         client.rs
//!               (mapping)        (Oracle driver)
//! ```
//!
//! ## CDC Audit Columns
//!
//! The sink adds audit columns to track CDC operations:
//! - `dbmazz_op_type`: Operation type (0=INSERT, 1=UPDATE, 2=DELETE)
//! - `dbmazz_is_deleted`: Soft delete flag for deletions (1=deleted)
//! - `dbmazz_synced_at`: Timestamp when record was synced
//! - `dbmazz_cdc_version`: Source LSN/position for ordering

mod config;
mod client;
pub(crate) mod schema_evolution;
mod setup;
pub(crate) mod types;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::SinkConfig;
use crate::connectors::sinks::schema_evolution::compute_schema_evolution_plan;
use crate::core::traits::SourceTableSchema;
use crate::core::{
    CdcRecord, ColumnValue, LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult,
};

pub use self::config::OracleSinkConfig;
use self::client::OracleClient;
use self::schema_evolution::OracleSchemaEvolution;
use self::types::TypeMapper;

/// Tables internal to dbmazz that should not be replicated
fn is_internal_table(table_name: &str) -> bool {
    table_name.starts_with("dbmazz_")
        || table_name.starts_with("_dbmazz_")
        || table_name == "dbmazz_checkpoints"
}

/// Oracle sink connector implementing the Sink trait.
///
/// This sink writes CDC records to Oracle using the MERGE INTO pattern
/// for idempotent upserts, with batch execution for performance.
pub struct OracleSink {
    /// Configuration for the Oracle connection
    config: OracleSinkConfig,
    /// Oracle database client
    client: OracleClient,
    /// Type mapper for converting CDC types to Oracle types
    type_mapper: TypeMapper,
    /// Schema-evolution machinery: target column cache, runtime ALTER application
    schema_evolution: OracleSchemaEvolution,
    /// In-memory snapshot of the schema dbmazz currently believes is in
    /// effect on the target. Keyed by `<schema>.<table>`.
    schema_cache: Arc<RwLock<HashMap<String, SourceTableSchema>>>,
}

impl OracleSink {
    /// Creates a new Oracle sink from the provided configuration.
    pub fn new(config: &SinkConfig, _mode: SinkMode) -> Result<Self> {
        let oracle_config = OracleSinkConfig::from_sink_config(config)?;
        let client = OracleClient::new(&oracle_config);
        let schema_evolution = OracleSchemaEvolution::new(&oracle_config)?;

        info!("OracleSink initialized:");
        info!("  Host: {}:{}", oracle_config.host, oracle_config.port);
        info!("  Service: {}", oracle_config.service_name);
        info!("  Schema: {}", oracle_config.schema);
        info!("  Batch Size: {}", oracle_config.batch_size);

        Ok(Self {
            config: oracle_config,
            client,
            type_mapper: TypeMapper::new(),
            schema_evolution,
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Builds an Oracle MERGE INTO SQL statement for upsert operations.
    ///
    /// Oracle MERGE pattern:
    /// ```sql
    /// MERGE INTO schema.table t
    /// USING (SELECT col1, col2 FROM dual) s
    /// ON (t.pk = s.pk)
    /// WHEN MATCHED THEN UPDATE SET col1 = s.col1, col2 = s.col2
    /// WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (s.col1, s.col2)
    /// ```
    fn build_merge_sql(
        &self,
        table_name: &str,
        columns: &[ColumnValue],
        pk_columns: &[String],
        op_type: &str,
    ) -> Result<Option<String>> {
        if columns.is_empty() {
            return Ok(None);
        }

        let all_column_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let non_pk_columns: Vec<&&str> = all_column_names
            .iter()
            .filter(|c| !pk_columns.contains(&c.to_string()))
            .collect();

        match op_type {
            "insert" | "upsert" => {
                // MERGE INTO for idempotent insert/update
                let select_exprs: Vec<String> = columns
                    .iter()
                    .map(|c| {
                        format!(
                            "{} AS \"{}\"",
                            self.type_mapper.value_to_sql_literal(&c.value),
                            c.name
                        )
                    })
                    .collect();

                let audit_select = vec![
                    format!("{} AS \"dbmazz_op_type\"", if op_type == "insert" { "0" } else { "1" }),
                    "\"0\" AS \"dbmazz_is_deleted\"".to_string(),
                    format!(
                        "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS') AS \"dbmazz_synced_at\"",
                        Utc::now().format("%Y-%m-%d %H:%M:%S")
                    ),
                    "0 AS \"dbmazz_cdc_version\"".to_string(),
                ];

                let on_clause: Vec<String> = pk_columns
                    .iter()
                    .map(|pk| format!("t.\"{}\" = s.\"{}\"", pk, pk))
                    .collect();

                let update_set: Vec<String> = non_pk_columns
                    .iter()
                    .map(|c| format!("\"{}\" = s.\"{}\"", c, c))
                    .chain(std::iter::once("\"dbmazz_op_type\" = 1".to_string()))
                    .chain(std::iter::once(
                        "\"dbmazz_synced_at\" = SYSTIMESTAMP".to_string(),
                    ))
                    .collect();

                let insert_cols: Vec<String> = all_column_names
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .chain(
                        vec!["\"dbmazz_op_type\"", "\"dbmazz_is_deleted\"",
                             "\"dbmazz_synced_at\"", "\"dbmazz_cdc_version\""]
                            .iter()
                            .map(|c| c.to_string()),
                    )
                    .collect();

                let insert_vals: Vec<String> = all_column_names
                    .iter()
                    .map(|c| format!("s.\"{}\"", c))
                    .chain(vec![
                        format!("{}", if op_type == "insert" { "0" } else { "1" }),
                        "0".to_string(),
                        "SYSTIMESTAMP".to_string(),
                        "0".to_string(),
                    ])
                    .collect();

                let sql = format!(
                    "MERGE INTO \"{}\".\"{}\" t\n\
                     USING (SELECT {} FROM dual) s\n\
                     ON ({})\n\
                     WHEN MATCHED THEN UPDATE SET {}\n\
                     WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                    self.config.schema,
                    table_name,
                    select_exprs
                        .iter()
                        .chain(audit_select.iter())
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", "),
                    on_clause.join(" AND "),
                    update_set.join(", "),
                    insert_cols.join(", "),
                    insert_vals.join(", "),
                );

                Ok(Some(sql))
            }
            "delete" => {
                // DELETE via MERGE WHERE matched condition
                let select_exprs: Vec<String> = pk_columns
                    .iter()
                    .map(|pk| {
                        let col = columns.iter().find(|c| c.name == *pk).ok_or_else(|| {
                            anyhow!("PK column '{}' not found in record columns", pk)
                        })?;
                        Ok(format!(
                            "{} AS \"{}\"",
                            self.type_mapper.value_to_sql_literal(&col.value),
                            col.name
                        ))
                    })
                    .collect::<Result<Vec<String>>>()?;

                let on_clause: Vec<String> = pk_columns
                    .iter()
                    .map(|pk| format!("t.\"{}\" = s.\"{}\"", pk, pk))
                    .collect();

                let sql = format!(
                    "DELETE FROM \"{}\".\"{}\" t WHERE EXISTS (\n\
                     SELECT 1 FROM (SELECT {} FROM dual) s\n\
                     WHERE {})",
                    self.config.schema,
                    table_name,
                    select_exprs.join(", "),
                    on_clause.join(" AND "),
                );

                Ok(Some(sql))
            }
            _ => Ok(None),
        }
    }

    /// Converts CDC records to Oracle SQL statements grouped by table.
    fn records_to_sql_batches(
        &self,
        records: &[CdcRecord],
        pk_map: &HashMap<String, Vec<String>>,
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut batches: HashMap<String, Vec<String>> = HashMap::new();

        for record in records {
            match record {
                CdcRecord::Insert {
                    table,
                    columns,
                    ..
                } => {
                    if is_internal_table(&table.name) {
                        continue;
                    }

                    let pks = pk_map
                        .get(&table.qualified_name())
                        .cloned()
                        .unwrap_or_default();

                    if let Some(sql) = self.build_merge_sql(
                        &table.name,
                        columns,
                        &pks,
                        "insert",
                    )? {
                        batches
                            .entry(table.qualified_name())
                            .or_default()
                            .push(sql);
                    }
                }

                CdcRecord::Update {
                    table,
                    new_columns,
                    ..
                } => {
                    if is_internal_table(&table.name) {
                        continue;
                    }

                    let pks = pk_map
                        .get(&table.qualified_name())
                        .cloned()
                        .unwrap_or_default();

                    if let Some(sql) = self.build_merge_sql(
                        &table.name,
                        new_columns,
                        &pks,
                        "upsert",
                    )? {
                        batches
                            .entry(table.qualified_name())
                            .or_default()
                            .push(sql);
                    }
                }

                CdcRecord::Delete {
                    table,
                    columns,
                    ..
                } => {
                    if is_internal_table(&table.name) {
                        continue;
                    }

                    let pks = pk_map
                        .get(&table.qualified_name())
                        .cloned()
                        .unwrap_or_default();

                    if let Some(sql) = self.build_merge_sql(
                        &table.name,
                        columns,
                        &pks,
                        "delete",
                    )? {
                        batches
                            .entry(table.qualified_name())
                            .or_default()
                            .push(sql);
                    }
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

    /// Build a primary key lookup map from schema cache.
    fn build_pk_map(&self, cache: &HashMap<String, SourceTableSchema>) -> HashMap<String, Vec<String>> {
        cache
            .iter()
            .map(|(_k, v)| {
                let qualified = format!("{}.{}", v.schema, v.name);
                (qualified, v.primary_keys.clone())
            })
            .collect()
    }
}

#[async_trait]
impl Sink for OracleSink {
    fn name(&self) -> &'static str {
        "oracle"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: true,
            supports_transactions: false,
            loading_model: LoadingModel::Streaming,
            min_batch_size: Some(1),
            max_batch_size: Some(10_000),
            optimal_flush_interval_ms: 5000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        self.client.verify_connection().await
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        let oracle_setup = setup::OracleSetup::new(&self.config);
        oracle_setup.run(source_schemas).await?;

        self.schema_evolution
            .reconcile_target_schema(source_schemas)
            .await?;

        // Seed the cache with the introspected source state
        {
            let mut cache = self.schema_cache.write().await;
            for src in source_schemas {
                cache.insert(format!("{}.{}", src.schema, src.name), src.clone());
            }
        }

        info!("[OK] OracleSink setup complete");
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult::default());
        }

        // Phase 1 — schema evolution pre-pass
        let snapshot = self.schema_cache.read().await.clone();
        let (working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);
        let mut schema_evolution_skipped: u64 = 0;

        // Apply schema evolution diffs
        if !pending_diffs.is_empty() {
            for (table, diff) in &pending_diffs {
                let table_name = table.name.as_str();
                if self
                    .schema_evolution
                    .is_schema_evolution_enabled(table_name)
                    .await
                {
                    self.schema_evolution
                        .apply_diff(table_name, diff)
                        .await
                        .with_context(|| {
                            format!(
                                "oracle_sink: schema evolution failed for table {}",
                                table_name
                            )
                        })?;
                } else {
                    for added in &diff.added {
                        warn!(
                            table = %table_name,
                            column = %added.name,
                            "oracle_sink: schema change skipped"
                        );
                        schema_evolution_skipped += 1;
                    }
                }
            }
        }

        // Update cache
        {
            let mut cache = self.schema_cache.write().await;
            *cache = working;
        }

        // Phase 2 — data writes

        // Track last position from records
        let last_position = records.iter().rev().find_map(|r| match r {
            CdcRecord::Insert { position, .. }
            | CdcRecord::Update { position, .. }
            | CdcRecord::Delete { position, .. }
            | CdcRecord::Commit { position, .. }
            | CdcRecord::Heartbeat { position, .. } => Some(position.clone()),
            _ => None,
        });

        // Build PK map from cache
        let cache = self.schema_cache.read().await;
        let pk_map = self.build_pk_map(&cache);
        drop(cache);

        // Convert records to SQL batches grouped by table
        let batches = self.records_to_sql_batches(&records, &pk_map)?;

        if batches.is_empty() {
            return Ok(SinkResult {
                records_written: 0,
                bytes_written: 0,
                last_position,
                schema_evolution_skipped,
            });
        }

        let mut total_written = 0u64;
        let mut total_bytes = 0u64;

        // Ensure connection to Oracle
        self.client.ensure_connection().await?;

        // Execute each SQL statement grouped by table
        for (_table, statements) in &batches {
            for sql in statements {
                total_bytes += sql.len() as u64;
                let count = self
                    .client
                    .execute_dml(sql, &[])
                    .await
                    .with_context(|| format!("oracle_sink: SQL execution failed: {}", sql))?;
                total_written += count;
            }
        }

        // Commit the transaction
        self.client.commit().await?;

        Ok(SinkResult {
            records_written: total_written as usize,
            bytes_written: total_bytes,
            last_position,
            schema_evolution_skipped,
        })
    }

    async fn close(&mut self) -> Result<()> {
        self.client.close().await?;
        info!("OracleSink: closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};
    use crate::core::record::{ColumnValue, TableRef, Value};
    use crate::core::position::SourcePosition;

    fn test_config() -> SinkConfig {
        SinkConfig {
            sink_type: SinkType::Oracle,
            url: "localhost:1521/ORCLCDB".to_string(),
            port: 1521,
            database: "CDC_SCHEMA".to_string(),
            user: "cdc_user".to_string(),
            password: "cdc_pass".to_string(),
            specific: SinkSpecificConfig::Oracle,
        }
    }

    #[test]
    fn test_sink_creation() {
        let config = test_config();
        let sink = OracleSink::new(&config, SinkMode::Primary);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_capabilities() {
        let config = test_config();
        let sink = OracleSink::new(&config, SinkMode::Primary).unwrap();
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

    #[test]
    fn test_build_merge_sql_insert() {
        let config = test_config();
        let sink = OracleSink::new(&config, SinkMode::Primary).unwrap();

        let columns = vec![
            ColumnValue::new("id".to_string(), Value::Int64(1)),
            ColumnValue::new("name".to_string(), Value::String("Alice".to_string())),
        ];
        let pks = vec!["id".to_string()];

        let sql = sink
            .build_merge_sql("users", &columns, &pks, "insert")
            .unwrap()
            .unwrap();

        assert!(sql.contains("MERGE INTO \"CDC_SCHEMA\".\"users\" t"));
        assert!(sql.contains("USING (SELECT"));
        assert!(sql.contains("ON (t.\"id\" = s.\"id\")"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn test_build_merge_sql_delete() {
        let config = test_config();
        let sink = OracleSink::new(&config, SinkMode::Primary).unwrap();

        let columns = vec![
            ColumnValue::new("id".to_string(), Value::Int64(1)),
        ];
        let pks = vec!["id".to_string()];

        let sql = sink
            .build_merge_sql("users", &columns, &pks, "delete")
            .unwrap()
            .unwrap();

        assert!(sql.contains("DELETE FROM \"CDC_SCHEMA\".\"users\" t"));
        assert!(sql.contains("WHERE EXISTS"));
    }

    #[test]
    fn test_records_to_sql_batches() {
        let config = test_config();
        let sink = OracleSink::new(&config, SinkMode::Primary).unwrap();

        let mut pk_map = HashMap::new();
        pk_map.insert(
            "public.users".to_string(),
            vec!["id".to_string()],
        );

        let records = vec![
            CdcRecord::Insert {
                table: TableRef::new(Some("public".to_string()), "users".to_string()),
                columns: vec![
                    ColumnValue::new("id".to_string(), Value::Int64(1)),
                    ColumnValue::new("name".to_string(), Value::String("Alice".to_string())),
                ],
                position: SourcePosition::Offset(100),
            },
        ];

        let batches = sink.records_to_sql_batches(&records, &pk_map).unwrap();
        assert!(!batches.is_empty());

        let sqls = batches.get("public.users").unwrap();
        assert_eq!(sqls.len(), 1);
        assert!(sqls[0].contains("MERGE INTO"));
    }
}
