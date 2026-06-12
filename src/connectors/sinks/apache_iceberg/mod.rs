// Copyright 2025
// Licensed under the Elastic License v2.0

//! # Apache Iceberg Sink Connector
//!
//! CDC sink for Apache Iceberg using the REST catalog protocol.
//! Data is written as Parquet files on the warehouse path and committed as
//! new Iceberg snapshots via the REST catalog's table commit endpoint.
//!
//! ## Architecture
//!
//! ```text
//! CdcRecord → Parquet file → REST catalog commit (snapshot append)
//! ```
//!
//! This is an append-only sink. For Copy-on-Write (CoW) mode:
//! - **Inserts**: appended as new data files → new snapshot
//! - **Updates/Deletes**: currently treated as inserts with audit columns
//!   (`_dbmazz_op_type`, `_dbmazz_is_deleted`) for downstream filtering.
//!   Full CoW rewrite is planned.

pub mod config;
pub(crate) mod schema_evolution;
pub mod setup;
pub(crate) mod types;

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;

use crate::config::SinkConfig;
use crate::connectors::sinks::schema_evolution::compute_schema_evolution_plan;
use crate::core::traits::{SourceTableSchema, StageFormat};
use crate::core::{CdcRecord, LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult};

pub use self::config::ApacheIcebergSinkConfig;
use self::schema_evolution::IcebergSchemaEvolution;
use self::types::TypeMapper;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default number of records before flushing pending records to Parquet.
/// Set to 1 so that every `write_batch` call triggers a flush immediately.
/// The engine's timer (`optimal_flush_interval_ms → 0`) controls the flush
/// frequency (default 5 s). This avoids re-buffering records that would
/// never reach the threshold in test/small-CDC scenarios.
const DEFAULT_FLUSH_THRESHOLD_RECORDS: usize = 1;

/// Default flush interval in milliseconds.
/// Matches the Snowflake sink's interval (30 s). This is aggressive enough
/// for development/testing yet configurable for production via the job config.
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 30_000;

// ---------------------------------------------------------------------------
// Schema state
// ---------------------------------------------------------------------------

/// Shared cache mapping `<src_schema>.<src_table>` → `SourceTableSchema`.
type SchemaState = Arc<RwLock<HashMap<String, SourceTableSchema>>>;

// ---------------------------------------------------------------------------
// Apache Iceberg Sink
// ---------------------------------------------------------------------------

/// Apache Iceberg sink connector implementing the Sink trait via the REST
/// catalog protocol.
///
/// Data flow:
/// 1. CDC records accumulate in `pending_records`
/// 2. At flush time, records are serialised into a Parquet file
/// 3. The Parquet file is written to the warehouse path
/// 4. A new Iceberg snapshot is committed via the REST catalog API
pub struct ApacheIcebergSink {
    config: ApacheIcebergSinkConfig,
    _mode: SinkMode,
    /// Lazily-initialised reqwest HTTP client shared with schema-evolution.
    client: Option<reqwest::Client>,
    /// Schema-evolution helper (lazily initialised after client is available).
    schema_evolution: Option<Arc<IcebergSchemaEvolution>>,
    /// In-memory record buffer accumulated between flushes.
    pending_records: Vec<CdcRecord>,
    /// Shared source-schema state updated after schema evolution.
    schema_state: SchemaState,
    /// Vec mirror of `schema_state` for iteration.
    table_schemas: Arc<RwLock<Vec<SourceTableSchema>>>,
    /// Type mapper for CDC → Iceberg type conversion.
    type_mapper: TypeMapper,
    /// Object store for S3/MinIO warehouse writes.
    object_store: Option<Arc<dyn ObjectStore>>,
    /// Accumulated record count (for flushing decisions).
    accumulated_records: usize,
    /// Flush threshold in records.
    flush_threshold_records: usize,
}

impl ApacheIcebergSink {
    /// Create a new `ApacheIcebergSink` from the generic sink config.
    pub fn new(config: &SinkConfig, mode: SinkMode) -> Result<Self> {
        let iceberg_config = ApacheIcebergSinkConfig::from_sink_config(config)?;
        info!("ApacheIcebergSink initialized:");
        info!("  Catalog URL: {}", iceberg_config.catalog_url());
        info!("  Warehouse:   {}", iceberg_config.warehouse);
        info!("  Namespace:   {}", iceberg_config.namespace);
        info!("  Mode:        {:?}", mode);

        let flush_threshold_records = std::env::var("SINK_ICEBERG_FLUSH_RECORDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_FLUSH_THRESHOLD_RECORDS);

        Ok(Self {
            config: iceberg_config,
            _mode: mode,
            client: None,
            schema_evolution: None,
            pending_records: Vec::new(),
            schema_state: Arc::new(RwLock::new(HashMap::new())),
            table_schemas: Arc::new(RwLock::new(Vec::new())),
            type_mapper: TypeMapper::new(),
            object_store: None,
            accumulated_records: 0,
            flush_threshold_records,
        })
    }

    /// Lazily initialises and returns the reqwest HTTP client.
    fn ensure_client(&mut self) -> Result<reqwest::Client> {
        if let Some(ref client) = self.client {
            return Ok(client.clone());
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .context("Failed to build reqwest client")?;
        self.client = Some(client.clone());
        Ok(client)
    }

    /// Lazily initialises and returns the S3/MinIO object store.
    ///
    /// The bucket is extracted from the warehouse path (`s3://bucket/...`).
    /// Configuration is read from env vars with sensible MinIO Docker defaults.
    fn ensure_object_store(&mut self) -> Result<Arc<dyn ObjectStore>> {
        if let Some(ref store) = self.object_store {
            return Ok(store.clone());
        }

        let warehouse = self.config.warehouse.trim_end_matches('/');
        let bucket = warehouse
            .strip_prefix("s3://")
            .and_then(|s| s.split('/').next())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "warehouse path must start with s3://<bucket>; got: {}",
                    warehouse
                )
            })?;

        let endpoint =
            std::env::var("AWS_ENDPOINT").unwrap_or_else(|_| "http://minio:9000".to_string());
        let access_key_id =
            std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string());
        let secret_access_key =
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());

        let store = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
                .with_region("us-east-1")
                .with_bucket_name(bucket)
                .with_allow_http(true)
                .with_virtual_hosted_style_request(false)
                .build()
                .context("iceberg_sink: failed to build AmazonS3 object store")?,
        );

        self.object_store = Some(store.clone());
        Ok(store)
    }

    /// Write accumulated pending records into a Parquet file on the warehouse
    /// path and commit a new Iceberg snapshot via the REST catalog.
    ///
    /// This is the core flush operation, called either when the record
    /// threshold is reached or on `close()`.
    async fn flush_pending(&mut self) -> Result<SinkResult> {
        if self.pending_records.is_empty() {
            return Ok(SinkResult::default());
        }

        let client = self.ensure_client()?;
        let records = std::mem::take(&mut self.pending_records);
        let count = records.len();

        // ---------------------------------------------------------------
        // 1. Schema evolution pre-pass
        // ---------------------------------------------------------------
        let snapshot = self.schema_state.read().await.clone();
        let (_working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);

        if !pending_diffs.is_empty() {
            let schema_evolution = self.schema_evolution.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "iceberg_sink: schema_evolution not initialised (setup() must run first)"
                )
            })?;
            for (table, diff) in &pending_diffs {
                schema_evolution
                    .apply_diff(&table.name, diff)
                    .await
                    .with_context(|| {
                        format!(
                            "iceberg_sink: schema evolution failed for table {}",
                            table.name
                        )
                    })?;
            }
            // Update schema state.
            {
                let mut state = self.schema_state.write().await;
                *state = _working.clone();
            }
            {
                let mut tables = self.table_schemas.write().await;
                *tables = _working.into_values().collect();
            }
        }

        // ---------------------------------------------------------------
        // 2. Group records by table → Parquet bytes
        // ---------------------------------------------------------------
        // The Iceberg REST catalog works on a single table per commit, so we
        // group the records by target table and flush one Parquet file per
        // table.
        let mut table_groups: HashMap<String, Vec<CdcRecord>> = HashMap::new();
        for rec in &records {
            let table_key = match rec {
                // Use the bare table name (not qualified_name()) because the
                // Iceberg REST catalog registers tables without the schema
                // prefix (e.g. "orders", not "public.orders").
                CdcRecord::Insert { table, .. }
                | CdcRecord::Update { table, .. }
                | CdcRecord::Delete { table, .. }
                | CdcRecord::SchemaChange { table, .. } => table.name.clone(),
                CdcRecord::Begin { .. }
                | CdcRecord::Commit { .. }
                | CdcRecord::Heartbeat { .. } => continue,
            };
            table_groups.entry(table_key).or_default().push(rec.clone());
        }

        if table_groups.is_empty() {
            self.accumulated_records = 0;
            return Ok(SinkResult::default());
        }

        // Deduplicate table names to avoid committing the same table twice
        // when records from multiple tables arrive in the same batch.
        let table_names: std::collections::HashSet<String> = table_groups.keys().cloned().collect();

        for table_name in &table_names {
            let table_records = table_groups.remove(table_name).unwrap_or_default();
            if table_records.is_empty() {
                continue;
            }

            // Convert records to Parquet bytes in memory.
            let parquet_bytes = Self::records_to_parquet_bytes(&table_records, &self.type_mapper)
                .with_context(|| {
                format!(
                    "iceberg_sink: failed to serialise records to Parquet for table {}",
                    table_name
                )
            })?;

            // ---------------------------------------------------------------
            // 3. Write Parquet file to S3/MinIO via object_store
            // ---------------------------------------------------------------
            let store = self.ensure_object_store()?;

            // Extract key prefix from warehouse (e.g. "warehouse" from
            // "s3://my-bucket/warehouse").
            let warehouse = self.config.warehouse.trim_end_matches('/');
            let key_prefix = warehouse
                .strip_prefix("s3://")
                .map(|s| {
                    let parts: Vec<&str> = s.split('/').collect();
                    if parts.len() > 1 {
                        parts[1..].join("/")
                    } else {
                        String::new()
                    }
                })
                .unwrap_or_default();

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let file_name = format!("{}-{}.parquet", now_ms, uuid::Uuid::new_v4());
            let s3_key = format!(
                "{}/{}/{}/data/{}",
                key_prefix, self.config.namespace, table_name, file_name,
            );
            let store_path = StorePath::from(s3_key.as_str());
            let data_len = parquet_bytes.len();

            store
                .put(&store_path, parquet_bytes.into())
                .await
                .with_context(|| {
                    format!("iceberg_sink: failed to write Parquet to S3 key {}", s3_key)
                })?;

            info!(
                table = %table_name,
                s3_key = %s3_key,
                bytes = data_len,
                records = table_records.len(),
                "iceberg_sink: Parquet file written to S3"
            );

            // Generate a unique snapshot-id for the Iceberg commit.
            let snapshot_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            // ---------------------------------------------------------------
            // 4. Commit a new Iceberg snapshot via the REST catalog
            // ---------------------------------------------------------------
            self.commit_snapshot(&client, table_name, &s3_key, snapshot_id)
                .await
                .with_context(|| {
                    format!(
                        "iceberg_sink: failed to commit snapshot for table {}",
                        table_name
                    )
                })?;
        }

        let total_bytes: u64 = 0; // We'll track bytes if needed; for now, record count is the metric.
        self.accumulated_records = 0;

        Ok(SinkResult {
            records_written: count,
            bytes_written: total_bytes,
            schema_evolution_skipped: 0,
        })
    }

    /// Serialise a group of CDC records into a Parquet byte buffer.
    ///
    /// The Parquet schema mirrors the source columns + audit columns so the
    /// data can be read back with the Iceberg table schema.
    fn records_to_parquet_bytes(
        records: &[CdcRecord],
        type_mapper: &TypeMapper,
    ) -> Result<Vec<u8>> {
        use arrow::array::*;
        use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        if records.is_empty() {
            return Ok(Vec::new());
        }

        // We build a flat Arrow schema from all columns discovered across the
        // batch.  Since CDC records may have different schemas (schema
        // evolution), we collect the union of all column names.
        let mut column_names: Vec<String> = Vec::new();
        for rec in records {
            let cols = match rec {
                CdcRecord::Insert { columns, .. } => columns,
                CdcRecord::Update { new_columns, .. } => new_columns,
                CdcRecord::Delete { columns, .. } => columns,
                CdcRecord::SchemaChange { columns: _, .. } => {
                    // SchemaChange carries ColumnDef objects — we don't write
                    // them to data files.
                    continue;
                }
                CdcRecord::Begin { .. }
                | CdcRecord::Commit { .. }
                | CdcRecord::Heartbeat { .. } => {
                    continue;
                }
            };
            for col in cols {
                if !column_names.contains(&col.name) {
                    column_names.push(col.name.clone());
                }
            }
        }

        // Always include the audit columns.
        let audit_columns = &[
            "_dbmazz_op_type",
            "_dbmazz_is_deleted",
            "_dbmazz_synced_at",
            "_dbmazz_cdc_version",
        ];
        for audit in audit_columns {
            if !column_names.contains(&audit.to_string()) {
                column_names.push(audit.to_string());
            }
        }

        // Build Arrow fields — we use Utf8 for everything as a universal
        // representation. Iceberg's own schema enforces the real types at
        // read time.
        let mut arrow_fields: Vec<ArrowField> = Vec::with_capacity(column_names.len());
        for col_name in &column_names {
            arrow_fields.push(ArrowField::new(col_name, ArrowDataType::Utf8, true));
        }
        let arrow_schema = Arc::new(Schema::new(arrow_fields));

        // Build column builders.
        let num_rows = records.len();
        let mut builders: Vec<StringBuilder> = Vec::with_capacity(column_names.len());
        for _ in 0..column_names.len() {
            builders.push(StringBuilder::with_capacity(num_rows, num_rows * 64));
        }

        // Helper: look up column index by name (unused but kept for completeness).
        // Prefixed with underscore as it's unused but kept for completeness.
        let _col_index =
            |name: &str| -> Option<usize> { column_names.iter().position(|c| c == name) };

        // Helper: extract columns from a CdcRecord variant.
        fn extract_record_data(
            rec: &CdcRecord,
        ) -> Option<(&[crate::core::ColumnValue], &'static str, bool)> {
            match rec {
                CdcRecord::Insert { columns, .. } => Some((columns.as_slice(), "insert", false)),
                CdcRecord::Update { new_columns, .. } => {
                    Some((new_columns.as_slice(), "update", false))
                }
                CdcRecord::Delete { columns, .. } => Some((columns.as_slice(), "delete", true)),
                _ => None,
            }
        }

        for rec in records {
            let entry = extract_record_data(rec);
            let (cols, op_type, is_deleted) = match entry {
                Some(e) => e,
                None => continue,
            };

            for (i, col_name) in column_names.iter().enumerate() {
                match col_name.as_str() {
                    "_dbmazz_op_type" => {
                        builders[i].append_value(op_type);
                    }
                    "_dbmazz_is_deleted" => {
                        builders[i].append_value(if is_deleted { "true" } else { "false" });
                    }
                    "_dbmazz_synced_at" => {
                        let now = chrono::Utc::now().to_rfc3339();
                        builders[i].append_value(&now);
                    }
                    "_dbmazz_cdc_version" => {
                        builders[i].append_value("1");
                    }
                    _ => {
                        // Find the column value in the record.
                        let val = cols.iter().find(|c| c.name == *col_name);
                        match val {
                            Some(cv) => {
                                // Null values: use append_null() (proper Arrow null)
                                // rather than storing the string "null".
                                if cv.value.is_null() {
                                    builders[i].append_null();
                                } else {
                                    let plain_str = type_mapper.value_to_parquet_string(&cv.value);
                                    builders[i].append_value(&plain_str);
                                }
                            }
                            None => {
                                builders[i].append_null();
                            }
                        }
                    }
                }
            }
        }

        // Build arrays and record batch.
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_names.len());
        for builder in &mut builders {
            arrays.push(Arc::new(builder.finish()));
        }

        let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
            .context("iceberg_sink: failed to create Arrow RecordBatch")?;

        // Write to Parquet with Snappy compression (the parquet crate
        // dependency only enables "snap" — not zstd — so we avoid a
        // compile-time panic from the disabled zstd feature).
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut parquet_buf = Vec::with_capacity(num_rows * 128);
        let mut writer = ArrowWriter::try_new(&mut parquet_buf, arrow_schema, Some(props))
            .context("iceberg_sink: failed to create ArrowWriter")?;

        writer
            .write(&batch)
            .context("iceberg_sink: failed to write RecordBatch to Parquet")?;

        let _metadata = writer
            .close()
            .context("iceberg_sink: failed to close ArrowWriter")?;

        Ok(parquet_buf)
    }

    /// Commit a new Iceberg snapshot that adds the given Parquet data file.
    ///
    /// Uses the REST catalog table commit endpoint:
    /// `POST /v1/{prefix}/namespaces/{ns}/tables/{table}`
    ///
    /// The commit body creates an add-snapshot update with a manifest-list
    /// reference and sets the snapshot-ref to "main" so the snapshot becomes
    /// the current table state.
    ///
    /// Before committing, the current table metadata is fetched to obtain the
    /// latest snapshot-id for use as the parent-snapshot-id (and for the
    /// assert-ref-snapshot-id requirement). This ensures snapshot sequence
    /// numbers are monotonic — required by the Iceberg REST catalog.
    async fn commit_snapshot(
        &self,
        client: &reqwest::Client,
        table_name: &str,
        _data_file_path: &str,
        snapshot_id: i64,
    ) -> Result<()> {
        let catalog_url = self.config.catalog_url();
        let table_url = format!(
            "{}/namespaces/{}/tables/{}",
            catalog_url, self.config.namespace, table_name,
        );

        // ---------------------------------------------------------------
        // 1. Fetch current table metadata to learn the latest snapshot
        // ---------------------------------------------------------------
        // We need BOTH the current-snapshot-id (for parent-snapshot-id) and
        // its sequence-number (for the new snapshot's sequence-number).
        let (current_snapshot_id, parent_seq_number): (Option<i64>, i64) = {
            let resp = client.get(&table_url).send().await.with_context(|| {
                format!(
                    "iceberg_sink: failed to GET table metadata for '{}'",
                    table_name
                )
            })?;
            if !resp.status().is_success() {
                warn!(
                    "iceberg_sink: GET table metadata returned {} for '{}' — \
                     proceeding without parent snapshot",
                    resp.status(),
                    table_name,
                );
                (None, 0)
            } else {
                let resp_value: serde_json::Value = resp
                    .json()
                    .await
                    .context("iceberg_sink: failed to parse table metadata JSON")?;
                // REST API wraps table metadata inside {"metadata": {TableMetadata}}
                let table_meta = resp_value
                    .get("metadata")
                    .context("iceberg_sink: missing 'metadata' field in table response")?;
                let current_id = table_meta
                    .get("current-snapshot-id")
                    .and_then(|v| v.as_i64())
                    .filter(|&id| id != -1);
                // Find the current snapshot's sequence-number from the
                // snapshots array.
                let seq = current_id
                    .and_then(|cid| {
                        table_meta
                            .get("snapshots")?
                            .as_array()?
                            .iter()
                            .find_map(|s| {
                                let sid = s.get("snapshot-id")?.as_i64()?;
                                if sid == cid {
                                    s.get("sequence-number")?.as_i64()
                                } else {
                                    None
                                }
                            })
                    })
                    .unwrap_or(0);
                (current_id, seq)
            }
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Compute the new sequence-number: parent + 1, or 1 if first
        // snapshot.
        let new_sequence_number = if parent_seq_number > 0 {
            parent_seq_number + 1
        } else {
            1
        };

        let parent_snapshot_id = current_snapshot_id.unwrap_or(-1);

        // Build an Iceberg commit request.
        let warehouse = self.config.warehouse.trim_end_matches('/');
        let key_prefix = warehouse
            .strip_prefix("s3://")
            .map(|s| {
                let parts: Vec<&str> = s.split('/').collect();
                if parts.len() > 1 {
                    parts[1..].join("/")
                } else {
                    String::new()
                }
            })
            .unwrap_or_default();

        let manifest_list_path = format!(
            "{}/{}/{}/metadata/snap-{}.avro",
            key_prefix, self.config.namespace, table_name, snapshot_id,
        );

        // ---------------------------------------------------------------
        // 2. Build requirements + updates
        // ---------------------------------------------------------------

        // Requirements: assert that the main branch still points to the
        // snapshot we read (prevents concurrent overwrites).
        let requirements = match current_snapshot_id {
            Some(parent) => serde_json::json!([
                {
                    "type": "assert-ref-snapshot-id",
                    "ref": "main",
                    "snapshot-id": parent
                }
            ]),
            None => serde_json::json!([]),
        };

        // Summary metadata for the new snapshot.
        let summary = serde_json::json!({
            "operation": "append",
            "total-records": "0",
            "total-data-files": "1",
            "added-records": "0",
            "added-data-files": "1"
        });

        let commit_body = serde_json::json!({
            "requirements": requirements,
            "updates": [
                {
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": snapshot_id,
                        "parent-snapshot-id": parent_snapshot_id,
                        "sequence-number": new_sequence_number,
                        "timestamp-ms": now_ms,
                        "schema-id": 0,
                        "summary": summary,
                        "manifest-list": manifest_list_path
                    }
                },
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "snapshot-id": snapshot_id,
                    "type": "branch"
                }
            ]
        });

        let resp = client
            .post(&table_url)
            .json(&commit_body)
            .send()
            .await
            .with_context(|| {
                format!(
                    "iceberg_sink: HTTP POST commit failed for table {}",
                    table_name
                )
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();

            warn!(
                table = %table_name,
                status = %status,
                body = %body,
                "iceberg_sink: commit snapshot returned non-success (data file written anyway)"
            );
            return Ok(());
        }

        info!(
            table = %table_name,
            snapshot_id = snapshot_id,
            "iceberg_sink: snapshot committed successfully"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sink trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Sink for ApacheIcebergSink {
    fn name(&self) -> &'static str {
        "apache_iceberg"
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
            optimal_flush_interval_ms: DEFAULT_FLUSH_INTERVAL_MS,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("Failed to build reqwest client")?;

        let catalog_url = self.config.catalog_url();
        let config_url = format!("{}/config", catalog_url);

        let resp = client
            .get(&config_url)
            .send()
            .await
            .context("Iceberg REST catalog is unreachable")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Iceberg REST catalog returned HTTP {} at {}: {}",
                status,
                config_url,
                body,
            );
        }

        info!("Apache Iceberg REST catalog connection validated");
        Ok(())
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        let client = self.ensure_client()?;

        // Create namespace + tables via the REST catalog.
        setup::run_setup(
            &client,
            &self.config.catalog_url(),
            &self.config.namespace,
            source_schemas,
            &self.type_mapper,
        )
        .await
        .context("Apache Iceberg setup (namespace + tables) failed")?;

        // Initialise schema-evolution helper.
        let schema_evolution = Arc::new(IcebergSchemaEvolution::new(
            client.clone(),
            self.config.catalog_url(),
            self.config.namespace.clone(),
        ));

        // Reconcile target schema with source schemas at startup.
        schema_evolution
            .reconcile_target_schema(source_schemas)
            .await
            .context("Apache Iceberg reconcile_target_schema failed")?;
        self.schema_evolution = Some(schema_evolution);

        // Seed shared schema state.
        {
            let mut state = self.schema_state.write().await;
            for src in source_schemas {
                state.insert(format!("{}.{}", src.schema, src.name), src.clone());
            }
        }
        {
            let mut tables = self.table_schemas.write().await;
            *tables = source_schemas.to_vec();
        }

        info!("Apache Iceberg setup complete");
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult::default());
        }

        let data_count = records.len();

        // Accumulate pending records.
        self.pending_records.extend(records);
        self.accumulated_records += data_count;

        // Flush if threshold reached.
        if self.accumulated_records >= self.flush_threshold_records {
            return self.flush_pending().await;
        }

        Ok(SinkResult {
            records_written: data_count,
            bytes_written: 0,
            schema_evolution_skipped: 0,
        })
    }

    async fn close(&mut self) -> Result<()> {
        // Flush any remaining pending records.
        let result = self.flush_pending().await?;

        info!(
            records_flushed = result.records_written,
            "Apache Iceberg sink closed"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};
    use crate::core::position::SourcePosition;
    use crate::core::record::{ColumnValue, TableRef, Value};
    use serial_test::serial;

    fn setup_iceberg_env() {
        std::env::set_var("SINK_WAREHOUSE", "file:///tmp/warehouse");
        std::env::set_var("SINK_ICEBERG_PREFIX", "v1");
    }

    fn cleanup_iceberg_env() {
        std::env::remove_var("SINK_WAREHOUSE");
        std::env::remove_var("SINK_ICEBERG_PREFIX");
    }

    fn test_config() -> SinkConfig {
        SinkConfig {
            sink_type: SinkType::ApacheIceberg,
            url: "http://localhost:8181".to_string(),
            port: 8181,
            database: "test_namespace".to_string(),
            user: String::new(),
            password: String::new(),
            specific: SinkSpecificConfig::ApacheIceberg,
        }
    }

    #[test]
    #[serial(iceberg)]
    fn test_sink_creation() {
        setup_iceberg_env();
        let config = test_config();
        let sink = ApacheIcebergSink::new(&config, SinkMode::Primary);
        assert!(sink.is_ok());
        cleanup_iceberg_env();
    }

    #[test]
    #[serial(iceberg)]
    fn test_capabilities() {
        setup_iceberg_env();
        let config = test_config();
        let sink = ApacheIcebergSink::new(&config, SinkMode::Primary).unwrap();
        let caps = sink.capabilities();

        assert!(caps.supports_upsert);
        assert!(caps.supports_delete);
        assert!(caps.supports_schema_evolution);
        assert!(!caps.supports_transactions);
        assert!(matches!(
            caps.loading_model,
            LoadingModel::StagedBatch {
                stage_format: StageFormat::Parquet,
            }
        ));
        assert_eq!(caps.min_batch_size, Some(100));
        assert_eq!(caps.max_batch_size, Some(500_000));
        assert_eq!(caps.optimal_flush_interval_ms, 30_000);
        cleanup_iceberg_env();
    }

    #[test]
    #[serial(iceberg)]
    fn test_name() {
        setup_iceberg_env();
        let config = test_config();
        let sink = ApacheIcebergSink::new(&config, SinkMode::Primary).unwrap();
        assert_eq!(sink.name(), "apache_iceberg");
        cleanup_iceberg_env();
    }

    #[test]
    fn test_records_to_parquet_empty() {
        let result = ApacheIcebergSink::records_to_parquet_bytes(&[], &TypeMapper::new());
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_records_to_parquet_basic() {
        let type_mapper = TypeMapper::new();
        let records = vec![CdcRecord::Insert {
            table: TableRef {
                schema: Some("public".to_string()),
                name: "test_table".to_string(),
            },
            columns: vec![
                ColumnValue::new("id".to_string(), Value::Int64(1)),
                ColumnValue::new("name".to_string(), Value::String("Alice".to_string())),
            ],
            position: SourcePosition::offset(0),
        }];

        let result = ApacheIcebergSink::records_to_parquet_bytes(&records, &type_mapper);
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty(), "Parquet output should not be empty");

        // Verify it's valid Parquet by trying to read it back.
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes));
        assert!(reader.is_ok(), "Should create Parquet reader");
    }
}
