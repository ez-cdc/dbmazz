// Copyright 2026
// Licensed under the Elastic License v2.0

//! # Iceberg Sink Connector
//!
//! CDC sink for Apache Iceberg tables on S3-compatible object stores,
//! via an Iceberg REST catalog.
//!
//! ## Model
//!
//! Append-only changelog: every INSERT/UPDATE/DELETE becomes one row in
//! the target table, carrying the source columns plus `_cdc_op`,
//! `_cdc_position`, and `_cdc_ts` metadata columns. Downstream consumers
//! materialize current state (e.g. `MERGE`/dedup by primary key on read).
//!
//! ## Durability
//!
//! Each `write_batch()` writes Parquet data files through iceberg-rust's
//! writer (field-ids and stats included) and commits a FastAppend
//! snapshot per touched table **before returning**. `Ok` therefore means
//! the batch is durable in the table — the LSN checkpoint invariant the
//! pipeline relies on. There is no staging window and nothing to recover
//! on restart: files from a crashed run that never got committed are
//! simply unreferenced.

pub mod arrow_convert;
pub mod catalog;
pub mod config;
pub mod schema;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tracing::{info, warn};

use crate::config::SinkConfig;
use crate::core::traits::{SourceTableSchema, StageFormat};
use crate::core::{CdcRecord, LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult};

use self::catalog::CatalogHandle;
pub use self::config::IcebergSinkConfig;

/// Iceberg sink connector implementing the `Sink` trait.
pub struct IcebergSink {
    config: IcebergSinkConfig,
    catalog: Option<CatalogHandle>,
    /// Loaded table handles keyed by pipeline qualified name
    /// (`schema.table`). Refreshed after every commit.
    tables: HashMap<String, Table>,
}

impl IcebergSink {
    pub fn new(config: &SinkConfig, _mode: SinkMode) -> Result<Self> {
        let ice_config = IcebergSinkConfig::from_sink_config(config)?;
        info!(
            catalog_uri = %ice_config.catalog_uri,
            warehouse = %ice_config.warehouse,
            namespace = %ice_config.namespace,
            "IcebergSink initialized"
        );
        Ok(Self {
            config: ice_config,
            catalog: None,
            tables: HashMap::new(),
        })
    }

    async fn ensure_catalog(&mut self) -> Result<&CatalogHandle> {
        if self.catalog.is_none() {
            self.catalog = Some(CatalogHandle::connect(&self.config).await?);
        }
        Ok(self.catalog.as_ref().expect("catalog just initialized"))
    }

    /// Get (and cache) the table handle for a qualified name. Snapshot
    /// worker sinks don't go through `setup()`, so this loads lazily —
    /// the primary sink's setup created the tables beforehand.
    async fn table_for(&mut self, qualified_name: &str) -> Result<Table> {
        if let Some(table) = self.tables.get(qualified_name) {
            return Ok(table.clone());
        }
        self.ensure_catalog().await?;
        let catalog = self.catalog.as_ref().expect("catalog initialized above");
        let table = catalog.load_table(qualified_name).await?;
        self.tables
            .insert(qualified_name.to_string(), table.clone());
        Ok(table)
    }

    /// Write one table's records as Parquet data files and commit them as
    /// a FastAppend snapshot. Returns (rows, bytes) on success.
    async fn write_table_batch(
        &mut self,
        qualified_name: &str,
        records: &[CdcRecord],
        write_ts_micros: i64,
    ) -> Result<(usize, u64)> {
        let table = self.table_for(qualified_name).await?;

        let table_schema = table.metadata().current_schema().clone();
        let arrow_schema = Arc::new(
            schema_to_arrow_schema(&table_schema)
                .map_err(|e| anyhow::anyhow!("Arrow schema conversion failed: {}", e))
                .with_context(|| format!("table '{}'", qualified_name))?,
        );

        let converted = arrow_convert::records_to_batch(
            qualified_name,
            &arrow_schema,
            records,
            write_ts_micros,
        )?;

        if !converted.skipped_columns.is_empty() {
            warn!(
                table = qualified_name,
                columns = ?converted.skipped_columns,
                "Source columns missing from Iceberg table schema; values skipped \
                 (schema evolution is not auto-applied by this sink)"
            );
        }
        if converted.unchanged_as_null > 0 {
            warn!(
                table = qualified_name,
                count = converted.unchanged_as_null,
                "Unchanged TOAST values written as NULL (changelog model has no \
                 previous row image)"
            );
        }

        let rows = converted.batch.num_rows();
        if rows == 0 {
            return Ok((0, 0));
        }

        // Write Parquet data files via iceberg-rust's writer (field-ids,
        // stats, and file locations handled by the library).
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
            .build();
        let parquet_builder = ParquetWriterBuilder::new(writer_props, table_schema);
        let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
            .map_err(|e| anyhow::anyhow!("Location generator failed: {}", e))?;
        let file_name_gen = DefaultFileNameGenerator::new(
            "dbmazz".to_string(),
            Some(uuid::Uuid::new_v4().simple().to_string()),
            DataFileFormat::Parquet,
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            table.file_io().clone(),
            location_gen,
            file_name_gen,
        );
        let mut writer = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to build data file writer: {}", e))?;

        writer
            .write(converted.batch)
            .await
            .map_err(|e| anyhow::anyhow!("Parquet write failed for '{}': {}", qualified_name, e))?;
        let data_files = writer
            .close()
            .await
            .map_err(|e| anyhow::anyhow!("Parquet close failed for '{}': {}", qualified_name, e))?;

        let bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes()).sum();

        // Commit. On transient failures, reload the table (another writer
        // or maintenance job may have advanced it) and retry the append.
        let mut attempt = 0u32;
        let mut current_table = table;
        loop {
            let tx = Transaction::new(&current_table);
            let action = tx.fast_append().add_data_files(data_files.clone());
            let tx = action
                .apply(tx)
                .map_err(|e| anyhow::anyhow!("Failed to apply FastAppend: {}", e))?;

            let catalog = self
                .catalog
                .as_ref()
                .expect("catalog initialized in table_for");

            match tx.commit(catalog.as_catalog()).await {
                Ok(updated) => {
                    self.tables.insert(qualified_name.to_string(), updated);
                    info!(
                        table = qualified_name,
                        rows,
                        bytes,
                        files = data_files.len(),
                        "Committed Iceberg snapshot"
                    );
                    return Ok((rows, bytes));
                }
                Err(e) if attempt < self.config.commit_retries => {
                    attempt += 1;
                    warn!(
                        table = qualified_name,
                        attempt,
                        max = self.config.commit_retries,
                        error = %e,
                        "Iceberg commit failed; refreshing table and retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(200 * u64::from(attempt)))
                        .await;
                    let catalog = self
                        .catalog
                        .as_ref()
                        .expect("catalog initialized in table_for");
                    current_table = catalog.load_table(qualified_name).await?;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Iceberg commit failed for '{}' after {} attempts: {}",
                        qualified_name,
                        attempt + 1,
                        e
                    ));
                }
            }
        }
    }
}

#[async_trait]
impl Sink for IcebergSink {
    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            // Append-only changelog: no upsert. Deletes are delivered as
            // changelog rows (`_cdc_op = 'D'`), not physical deletes.
            supports_upsert: false,
            supports_delete: true,
            // iceberg-rust 0.9 transactions cannot update schemas.
            supports_schema_evolution: false,
            // Commits are atomic per table, not across tables in a batch.
            supports_transactions: false,
            loading_model: LoadingModel::StagedBatch {
                stage_format: StageFormat::Parquet,
            },
            min_batch_size: Some(100),
            max_batch_size: Some(500_000),
            // Every batch is one snapshot commit per touched table — a
            // longer window bounds snapshot/metadata growth.
            optimal_flush_interval_ms: 60_000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        let catalog = CatalogHandle::connect(&self.config).await?;
        catalog.probe().await?;
        info!("Iceberg catalog connection validated");
        Ok(())
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        self.ensure_catalog().await?;
        let catalog = self.catalog.as_ref().expect("catalog initialized above");

        catalog.ensure_namespace().await?;

        let mut tables = HashMap::new();
        for source in source_schemas {
            let table = catalog.ensure_table(source).await?;
            tables.insert(format!("{}.{}", source.schema, source.name), table);
        }
        self.tables.extend(tables);

        info!(tables = source_schemas.len(), "Iceberg setup complete");
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult::default());
        }

        let mut by_table: HashMap<String, Vec<CdcRecord>> = HashMap::new();
        let mut schema_evolution_skipped = 0u64;

        for record in records {
            match &record {
                CdcRecord::Insert { table, .. }
                | CdcRecord::Update { table, .. }
                | CdcRecord::Delete { table, .. } => {
                    by_table
                        .entry(table.qualified_name())
                        .or_default()
                        .push(record);
                }
                CdcRecord::SchemaChange { table, .. } => {
                    schema_evolution_skipped += 1;
                    warn!(
                        table = %table.qualified_name(),
                        "SchemaChange skipped: the Iceberg sink does not auto-apply \
                         schema evolution (iceberg-rust 0.9 limitation)"
                    );
                }
                // Transaction markers and heartbeats carry no data.
                CdcRecord::Begin { .. }
                | CdcRecord::Commit { .. }
                | CdcRecord::Heartbeat { .. } => {}
            }
        }

        let write_ts_micros = chrono::Utc::now().timestamp_micros();
        let mut records_written = 0usize;
        let mut bytes_written = 0u64;

        for (qualified_name, table_records) in &by_table {
            let (rows, bytes) = self
                .write_table_batch(qualified_name, table_records, write_ts_micros)
                .await?;
            records_written += rows;
            bytes_written += bytes;
        }

        Ok(SinkResult {
            records_written,
            bytes_written,
            schema_evolution_skipped,
        })
    }

    async fn close(&mut self) -> Result<()> {
        // Per-batch commits mean nothing is buffered here by design.
        info!("Iceberg sink closed (no pending data — commits are per batch)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkSpecificConfig, SinkType};

    fn sink_config() -> SinkConfig {
        SinkConfig {
            sink_type: SinkType::Iceberg,
            url: String::new(),
            port: 0,
            database: "analytics".to_string(),
            user: String::new(),
            password: String::new(),
            specific: SinkSpecificConfig::Iceberg(IcebergSinkConfig {
                catalog_uri: "http://localhost:8181/catalog".to_string(),
                warehouse: "warehouse".to_string(),
                namespace: "analytics".to_string(),
                s3_region: String::new(),
                s3_endpoint: String::new(),
                s3_access_key_id: String::new(),
                s3_secret_access_key: String::new(),
                s3_path_style: false,
                commit_retries: 3,
            }),
        }
    }

    #[test]
    fn capabilities_are_honest() {
        let sink = IcebergSink::new(&sink_config(), SinkMode::Primary).unwrap();
        let caps = sink.capabilities();
        assert!(!caps.supports_upsert);
        assert!(!caps.supports_schema_evolution);
        assert!(caps.supports_delete);
        assert_eq!(caps.optimal_flush_interval_ms, 60_000);
    }

    #[test]
    fn new_does_not_connect() {
        // Constructing the sink must not require a reachable catalog.
        assert!(IcebergSink::new(&sink_config(), SinkMode::SnapshotWorker).is_ok());
    }

    #[tokio::test]
    async fn write_batch_of_only_markers_is_a_noop() {
        let mut sink = IcebergSink::new(&sink_config(), SinkMode::Primary).unwrap();
        let result = sink
            .write_batch(vec![
                CdcRecord::Begin { xid: 1 },
                CdcRecord::Heartbeat {
                    position: crate::core::SourcePosition::Lsn(7),
                },
            ])
            .await
            .unwrap();
        assert_eq!(result.records_written, 0);
        assert_eq!(result.schema_evolution_skipped, 0);
    }
}
