// Copyright 2025
// Licensed under the Elastic License v2.0

//! # Iceberg Sink Connector
//!
//! CDC sink for S3-compatible object stores using Apache Iceberg table format.
//!
//! Writes CDC records to Parquet files, uploads them to S3 staging,
//! and commits them as Iceberg snapshots.
//!
//! ## Architecture
//!
//! ```text
//! CdcRecord → Parquet → S3 (staging prefix) → Iceberg commit → Data prefix
//! ```

pub mod catalog;
pub mod client;
pub mod commit;
pub mod config;
pub mod parquet_writer;
pub mod setup;
pub mod types;

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::config::SinkConfig;
use crate::connectors::sinks::schema_evolution::compute_schema_evolution_plan;
use crate::core::traits::{SourceTableSchema, StageFormat};
use crate::core::{
    CdcRecord, DataType, LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult,
};

use self::catalog::IcebergCatalog;
use self::client::S3Client;
use self::commit::{CommitManager, StagedFiles};
pub use self::config::IcebergSinkConfig;

/// A single table's schema difference for `apply_schema_evolution`.
pub struct TableDiff {
    pub table_name: String,
    pub new_columns: Vec<(String, DataType)>,
}

/// Apply schema evolution diffs to an Iceberg table via the catalog.
pub async fn apply_schema_evolution(catalog: &IcebergCatalog, diffs: &[TableDiff]) -> Result<()> {
    for diff in diffs {
        let parts: Vec<&str> = diff.table_name.splitn(2, '.').collect();
        let (namespace, table) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("default", parts[0])
        };

        for (col_name, col_type) in &diff.new_columns {
            catalog
                .add_column(namespace, table, col_name, col_type)
                .await
                .with_context(|| {
                    format!(
                        "Failed to add column '{}' to '{}.{}'",
                        col_name, namespace, table
                    )
                })?;
            info!(
                "Schema evolution: added column {}.{}.{} ({:?})",
                namespace, table, col_name, col_type
            );
        }
    }
    Ok(())
}

/// Shared schema state — keyed by `<schema>.<table>`.
pub(crate) type SchemaState = Arc<RwLock<HashMap<String, SourceTableSchema>>>;

/// Default flush thresholds
const DEFAULT_FLUSH_FILES: usize = 20;
const DEFAULT_FLUSH_BYTES: u64 = 100 * 1024 * 1024; // 100 MB

/// Iceberg sink connector implementing the Sink trait.
pub struct IcebergSink {
    config: IcebergSinkConfig,
    _mode: SinkMode,
    s3_client: Option<S3Client>,
    catalog: Option<IcebergCatalog>,
    commit_manager: Option<CommitManager>,
    batch_counter: AtomicI64,
    /// Per-table staged file accumulation
    staged_tables: HashMap<String, StagedFiles>,
    flush_threshold_files: usize,
    flush_threshold_bytes: u64,
    /// Shared source schema state
    schema_state: SchemaState,
}

impl IcebergSink {
    pub fn new(config: &SinkConfig, mode: SinkMode) -> Result<Self> {
        let ice_config = IcebergSinkConfig::from_sink_config(config)?;
        info!("IcebergSink initialized:");
        info!("  Bucket: {}", ice_config.bucket);
        info!("  Prefix: {}", ice_config.prefix);
        info!("  Region: {}", ice_config.region);
        info!("  Endpoint: {:?}", ice_config.endpoint);
        info!("  Catalog URI: {:?}", ice_config.catalog_uri);
        info!("  Warehouse: {}", ice_config.warehouse);
        info!("  Mode: {:?}", mode);

        let flush_files = std::env::var("ICEBERG_FLUSH_FILES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_FLUSH_FILES);

        let flush_bytes = std::env::var("ICEBERG_FLUSH_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_FLUSH_BYTES);

        Ok(Self {
            config: ice_config,
            _mode: mode,
            s3_client: None,
            catalog: None,
            commit_manager: None,
            batch_counter: AtomicI64::new(0),
            staged_tables: HashMap::new(),
            flush_threshold_files: flush_files,
            flush_threshold_bytes: flush_bytes,
            schema_state: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Lazily initialize S3 client.
    async fn ensure_s3_client(&mut self) -> Result<&S3Client> {
        if self.s3_client.is_none() {
            let client = S3Client::new(&self.config)
                .await
                .context("Failed to create S3 client")?;
            self.s3_client = Some(client);
        }
        Ok(self.s3_client.as_ref().unwrap())
    }

    /// Lazily initialize Iceberg catalog.
    async fn ensure_catalog(&mut self) -> Result<&IcebergCatalog> {
        if self.catalog.is_none() {
            let cat = IcebergCatalog::new(&self.config)
                .await
                .context("Failed to create Iceberg catalog")?;
            self.catalog = Some(cat);
        }
        Ok(self.catalog.as_ref().unwrap())
    }

    /// Lazily initialize commit manager.
    async fn ensure_commit_manager(&mut self) -> Result<&mut CommitManager> {
        if self.commit_manager.is_none() {
            let s3 = self.ensure_s3_client().await?;
            self.commit_manager = Some(CommitManager::new(s3.clone(), self.config.clone()));
        }
        Ok(self.commit_manager.as_mut().unwrap())
    }

    /// Flush staged files for all tables.
    async fn flush_all_staged(&mut self) -> Result<()> {
        // Initialize lazy fields first (each takes &mut self separately)
        self.ensure_catalog().await?;
        self.ensure_commit_manager().await?;

        let mut staged_tables = std::mem::take(&mut self.staged_tables);
        let cat = self.catalog.as_ref().unwrap();
        let cm = self.commit_manager.as_mut().unwrap();

        for staged in staged_tables.values_mut() {
            if !staged.is_empty() {
                cm.commit_staged_files(staged, cat).await?;
            }
        }
        self.staged_tables = staged_tables;
        Ok(())
    }

    fn next_batch_id(&self) -> i64 {
        self.batch_counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[async_trait]
impl Sink for IcebergSink {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: true,
            supports_transactions: false,
            loading_model: LoadingModel::StagedBatch {
                stage_format: StageFormat::Parquet,
            },
            min_batch_size: Some(100),
            max_batch_size: Some(500_000),
            optimal_flush_interval_ms: 30_000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        // Validate S3 connectivity
        let client = S3Client::new(&self.config)
            .await
            .context("Failed to create S3 client for validation")?;
        client
            .validate_connection()
            .await
            .context("S3 connection validation failed")?;

        // Validate catalog connectivity if configured
        if !self.config.catalog_uri.is_empty() {
            let _catalog = IcebergCatalog::new(&self.config)
                .await
                .context("Failed to create Iceberg catalog for validation")?;
            info!(
                "Iceberg catalog connection validated via {}",
                self.config.catalog_uri
            );
        }

        info!("Iceberg sink connection validated");
        Ok(())
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        let catalog = self.ensure_catalog().await?;

        // Create Iceberg tables from source schemas
        setup::run_setup(catalog, source_schemas)
            .await
            .context("Iceberg setup failed")?;

        // Seed schema state
        {
            let mut state = self.schema_state.write().await;
            for src in source_schemas {
                state.insert(format!("{}.{}", src.schema, src.name), src.clone());
            }
        }

        info!("Iceberg setup complete");
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult::default());
        }

        let snapshot = self.schema_state.read().await.clone();
        let _s3 = self.ensure_s3_client().await?;
        let catalog = self.ensure_catalog().await?;
        let (new_working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);

        if !pending_diffs.is_empty() {
            // Convert from shared SchemaDiff to Iceberg TableDiff
            let table_diffs: Vec<TableDiff> = pending_diffs
                .iter()
                .map(|(table_ref, diff)| TableDiff {
                    table_name: table_ref.qualified_name(),
                    new_columns: diff
                        .added
                        .iter()
                        .map(|col| (col.name.clone(), col.data_type.clone()))
                        .collect(),
                })
                .collect();

            apply_schema_evolution(catalog, &table_diffs)
                .await
                .context("Iceberg schema evolution failed")?;

            let mut state = self.schema_state.write().await;
            *state = new_working;
        }

        // Phase 2 — data writes
        let batch_id = self.next_batch_id();

        // Group records by table
        let mut table_records: HashMap<String, Vec<CdcRecord>> = HashMap::new();
        for record in records {
            let table = match &record {
                CdcRecord::Insert { table, .. } => table.qualified_name(),
                CdcRecord::Update { table, .. } => table.qualified_name(),
                CdcRecord::Delete { table, .. } => table.qualified_name(),
                CdcRecord::SchemaChange { table, .. } => table.qualified_name(),
                _ => continue,
            };
            table_records.entry(table).or_default().push(record);
        }

        let mut total_records = 0;
        let mut total_bytes = 0;

        for (table_name, table_recs) in &table_records {
            // Get current schema for this table
            let state = self.schema_state.read().await;
            let columns = match state.get(table_name) {
                Some(schema) => schema.columns.clone(),
                _ => {
                    // Derive columns from records
                    derive_columns(table_recs)
                }
            };
            drop(state);

            if columns.is_empty() {
                continue;
            }

            // Serialize to Parquet
            let (parquet_bytes, data_count) =
                parquet_writer::records_to_parquet(table_recs, &columns, batch_id)
                    .context("Failed to serialize records to Parquet")?;

            if data_count == 0 {
                continue;
            }

            let file_size = parquet_bytes.len() as u64;
            let file_name = format!("batch_{}_{}.parquet", batch_id, uuid::Uuid::new_v4());

            // Upload to S3 staging
            let s3 = self.s3_client.as_ref().unwrap();
            let staging_key = S3Client::staging_key(table_name, &file_name);
            s3.put_object(&staging_key, bytes::Bytes::from(parquet_bytes))
                .await
                .context("S3 staging upload failed")?;

            // Track staged files per table
            let staged = self
                .staged_tables
                .entry(table_name.clone())
                .or_insert_with(|| StagedFiles::new(table_name.clone()));
            staged.add_file(file_name, data_count, file_size, false);

            total_records += data_count;
            total_bytes += file_size;
        }

        // Flush if thresholds reached for any table
        let flush_keys: Vec<String> = self
            .staged_tables
            .iter()
            .filter(|(_, staged)| {
                staged.file_count() >= self.flush_threshold_files
                    || staged.total_bytes >= self.flush_threshold_bytes
            })
            .map(|(name, _)| name.clone())
            .collect();

        if !flush_keys.is_empty() {
            // Initialize lazy fields first (each takes &mut self separately)
            self.ensure_catalog().await?;
            self.ensure_commit_manager().await?;

            let mut staged_tables = std::mem::take(&mut self.staged_tables);
            let cat = self.catalog.as_ref().unwrap();
            let cm = self.commit_manager.as_mut().unwrap();

            for table_name in &flush_keys {
                if let Some(staged) = staged_tables.get_mut(table_name) {
                    cm.commit_staged_files(staged, cat).await?;
                }
            }
            self.staged_tables = staged_tables;
        }

        Ok(SinkResult {
            records_written: total_records,
            bytes_written: total_bytes,
            schema_evolution_skipped: 0,
        })
    }

    async fn close(&mut self) -> Result<()> {
        // Flush any remaining staged files
        self.flush_all_staged().await?;

        info!("Iceberg sink closed");
        Ok(())
    }
}

/// Infer `DataType` from a `Value`.
fn infer_data_type(value: &crate::core::record::Value) -> crate::core::record::DataType {
    use crate::core::record::Value as V;
    match value {
        V::Null | V::Unchanged => crate::core::record::DataType::String,
        V::Bool(_) => crate::core::record::DataType::Boolean,
        V::Int64(_) => crate::core::record::DataType::Int64,
        V::UInt64(_) => crate::core::record::DataType::UInt64,
        V::Float64(_) => crate::core::record::DataType::Float64,
        V::String(_) => crate::core::record::DataType::String,
        V::Bytes(_) => crate::core::record::DataType::Bytes,
        V::Json(_) => crate::core::record::DataType::Json,
        V::Timestamp(_) => crate::core::record::DataType::Timestamp,
        V::Decimal(_) => crate::core::record::DataType::Decimal {
            precision: 38,
            scale: 0,
        },
        V::Uuid(_) => crate::core::record::DataType::Uuid,
    }
}

/// Derive SourceColumn definitions from CDC records for a table.
fn derive_columns(records: &[CdcRecord]) -> Vec<crate::core::traits::SourceColumn> {
    use std::collections::HashSet;
    let mut seen: HashSet<String> = HashSet::new();
    let mut columns = Vec::new();

    for record in records {
        let cols = match record {
            CdcRecord::Insert { columns, .. } => columns,
            CdcRecord::Update { new_columns, .. } => new_columns,
            CdcRecord::Delete { columns, .. } => columns,
            _ => continue,
        };
        for col in cols {
            if seen.insert(col.name.clone()) {
                columns.push(crate::core::traits::SourceColumn {
                    name: col.name.clone(),
                    data_type: infer_data_type(&col.value),
                    nullable: true,
                    pg_type_id: None,
                });
            }
        }
    }
    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};
    use crate::core::position::SourcePosition;
    use crate::core::record::TableRef;
    use crate::core::record::{CdcRecord, ColumnValue, Value};

    #[test]
    fn test_derive_columns_from_records() {
        let records = vec![CdcRecord::Insert {
            table: TableRef::new(Some("public".to_string()), "users".to_string()),
            columns: vec![
                ColumnValue {
                    name: "id".to_string(),
                    value: Value::Int64(1),
                },
                ColumnValue {
                    name: "name".to_string(),
                    value: Value::String("Alice".to_string()),
                },
            ],
            position: SourcePosition::Offset(0),
        }];

        let cols = derive_columns(&records);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[1].name, "name");
    }

    #[test]
    fn test_iceberg_sink_default_capabilities() {
        // Create a minimal config - just check capabilities don't panic
        let config = SinkConfig {
            sink_type: SinkType::Iceberg,
            url: String::new(),
            port: 0,
            database: "test".to_string(),
            user: String::new(),
            password: String::new(),
            specific: SinkSpecificConfig::Iceberg(IcebergSinkConfig::default()),
        };

        // Setting env var to avoid panic on missing S3_BUCKET during new()
        std::env::set_var("S3_BUCKET", "test-bucket");
        std::env::set_var("S3_PREFIX", "test");

        // This will fail because S3 client can't connect in test,
        // but the constructor should work
        let result = IcebergSink::new(&config, SinkMode::Primary);
        assert!(result.is_ok());

        std::env::remove_var("S3_BUCKET");
        std::env::remove_var("S3_PREFIX");
    }
}
