// Copyright 2025
// Licensed under the Elastic License v2.0

//! # Snowflake Sink Connector
//!
//! CDC sink for Snowflake using PUT + COPY INTO with Parquet files and a
//! two-phase pattern (staging raw table + MERGE normalizer).
//!
//! Communication with Snowflake is 100% via HTTP — no Java SDK or sidecar.
//!
//! ## Architecture
//!
//! ```text
//! CdcRecord → Parquet → PUT (stage) → COPY INTO (raw table) → MERGE (target)
//! ```

pub mod client;
pub mod config;
pub mod merge_generator;
pub mod normalizer;
pub mod parquet_writer;
pub mod setup;
pub mod stage;
pub(crate) mod types;

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::SinkConfig;
use crate::core::traits::{SourceTableSchema, StageFormat};
use crate::core::{CdcRecord, LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult};

use self::client::SnowflakeClient;
pub use self::config::SnowflakeSinkConfig;
use self::stage::StageManager;

/// Default file accumulation threshold before triggering COPY INTO.
const DEFAULT_FLUSH_THRESHOLD_FILES: usize = 20;

/// Default byte accumulation threshold (100 MB).
const DEFAULT_FLUSH_THRESHOLD_BYTES: u64 = 100 * 1024 * 1024;

/// Snowflake sink connector implementing the Sink trait.
pub struct SnowflakeSink {
    config: SnowflakeSinkConfig,
    mode: SinkMode,
    client: Option<Arc<SnowflakeClient>>,
    stage_manager: Option<StageManager>,
    normalize_tx: Option<mpsc::Sender<i64>>,
    batch_counter: AtomicI64,
    // File accumulation buffer (snapshot optimization)
    staged_files: Vec<String>,
    staged_bytes: u64,
    staged_records: u64,
    flush_threshold_files: usize,
    flush_threshold_bytes: u64,
    // Schema cache for normalizer
    source_schemas: Vec<SourceTableSchema>,
}

impl SnowflakeSink {
    pub fn new(config: &SinkConfig, mode: SinkMode) -> Result<Self> {
        let sf_config = SnowflakeSinkConfig::from_sink_config(config)?;
        info!("SnowflakeSink initialized:");
        info!("  Account: {}", sf_config.account);
        info!("  Database: {}", sf_config.database);
        info!("  Schema: {}", sf_config.schema);
        info!("  Warehouse: {}", sf_config.warehouse);
        info!(
            "  Auth: {}",
            if sf_config.use_jwt_auth() {
                "JWT key-pair"
            } else {
                "password"
            }
        );
        info!("  Mode: {:?}", mode);

        Ok(Self {
            config: sf_config,
            mode,
            client: None,
            stage_manager: None,
            normalize_tx: None,
            batch_counter: AtomicI64::new(0),
            staged_files: Vec::new(),
            staged_bytes: 0,
            staged_records: 0,
            flush_threshold_files: DEFAULT_FLUSH_THRESHOLD_FILES,
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
            source_schemas: Vec::new(),
        })
    }

    /// Lazily connects to Snowflake. Returns the shared client.
    async fn ensure_client(&mut self) -> Result<Arc<SnowflakeClient>> {
        if let Some(ref client) = self.client {
            return Ok(client.clone());
        }

        let client = Arc::new(
            SnowflakeClient::connect(&self.config)
                .await
                .context("Failed to connect to Snowflake")?,
        );

        let stage_name = format!(
            "{}.{}.STAGE_{}",
            self.config.database,
            "_DBMAZZ",
            self.config.safe_job_name()
        );

        self.stage_manager = Some(StageManager::new(client.clone(), stage_name));
        self.client = Some(client.clone());
        Ok(client)
    }

    /// Flushes all accumulated staged files to the raw table via COPY INTO.
    async fn flush_staged_files(&mut self) -> Result<()> {
        if self.staged_files.is_empty() {
            return Ok(());
        }

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Client not connected"))?;

        let db = &self.config.database;
        let job = self.config.safe_job_name();

        // COPY INTO loads ALL staged files at once (Snowflake parallelizes internally)
        let copy_sql = format!(
            r#"COPY INTO {db}._DBMAZZ._RAW_{job}
            FROM (
                SELECT
                    $1:_timestamp::NUMBER(20,0),
                    $1:_dst_table::VARCHAR,
                    PARSE_JSON($1:_data::VARCHAR),
                    $1:_record_type::NUMBER(3,0),
                    TRY_PARSE_JSON($1:_match_data::VARCHAR),
                    $1:_batch_id::NUMBER(20,0),
                    $1:_toast_columns::VARCHAR
                FROM @{db}._DBMAZZ.STAGE_{job}
            )
            FILE_FORMAT = (TYPE = PARQUET)
            PURGE = TRUE"#,
        );

        client
            .execute(&copy_sql)
            .await
            .context("COPY INTO raw table failed")?;

        info!(
            "COPY INTO: {} files, {} records, {} bytes → raw table",
            self.staged_files.len(),
            self.staged_records,
            self.staged_bytes
        );

        // Notify normalizer with the current batch_id
        if let Some(ref tx) = self.normalize_tx {
            let batch_id = self.batch_counter.load(Ordering::Relaxed);
            let _ = tx.try_send(batch_id);
        }

        self.staged_files.clear();
        self.staged_bytes = 0;
        self.staged_records = 0;
        Ok(())
    }

    fn next_batch_id(&self) -> i64 {
        self.batch_counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[async_trait]
impl Sink for SnowflakeSink {
    fn name(&self) -> &'static str {
        "snowflake"
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
        let client = SnowflakeClient::connect(&self.config)
            .await
            .context("Snowflake connection validation failed")?;

        client
            .execute("SELECT 1")
            .await
            .context("Snowflake test query failed")?;

        info!("Snowflake connection validated");
        Ok(())
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        let client = self.ensure_client().await?;

        // Run DDL setup
        setup::run_setup(
            &client,
            &self.config.database,
            &self.config.schema,
            &self.config.safe_job_name(),
            source_schemas,
            self.config.soft_delete,
        )
        .await
        .context("Snowflake setup failed")?;

        self.source_schemas = source_schemas.to_vec();

        // Spawn normalizer (Primary mode only)
        if self.mode == SinkMode::Primary {
            let normalizer_config = normalizer::NormalizerConfig {
                client_config: self.config.clone(),
                database: self.config.database.clone(),
                target_schema: self.config.schema.clone(),
                job_name: self.config.safe_job_name(),
                table_schemas: source_schemas.to_vec(),
                merge_interval_ms: self.config.merge_interval_ms,
                soft_delete: self.config.soft_delete,
            };
            self.normalize_tx = Some(normalizer::spawn_normalizer(normalizer_config));
            info!(
                "Snowflake normalizer spawned (interval: {}ms)",
                self.config.merge_interval_ms
            );
        }

        info!("Snowflake setup complete");
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult {
                records_written: 0,
                bytes_written: 0,
                last_position: None,
            });
        }

        let _client = self.ensure_client().await?;

        // Track last position
        let last_position = records.iter().rev().find_map(|r| match r {
            CdcRecord::Insert { position, .. }
            | CdcRecord::Update { position, .. }
            | CdcRecord::Delete { position, .. }
            | CdcRecord::Commit { position, .. }
            | CdcRecord::Heartbeat { position, .. } => Some(position.clone()),
            _ => None,
        });

        // Generate batch_id
        let batch_id = self.next_batch_id();

        // Serialize to Parquet
        let (parquet_bytes, data_count) = parquet_writer::records_to_parquet(&records, batch_id)
            .context("Failed to serialize records to Parquet")?;

        if data_count == 0 {
            return Ok(SinkResult {
                records_written: 0,
                bytes_written: 0,
                last_position,
            });
        }

        let file_size = parquet_bytes.len() as u64;
        let file_name = format!("batch_{}.parquet", batch_id);

        // Upload to stage via PUT protocol
        let stage_manager = self
            .stage_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Stage manager not initialized"))?;

        stage_manager
            .upload(&file_name, parquet_bytes)
            .await
            .context("Stage upload failed")?;

        // Track accumulated files
        self.staged_files.push(file_name);
        self.staged_bytes += file_size;
        self.staged_records += data_count as u64;

        // Flush to raw table if accumulation threshold reached
        if self.staged_files.len() >= self.flush_threshold_files
            || self.staged_bytes >= self.flush_threshold_bytes
        {
            self.flush_staged_files().await?;
        }

        Ok(SinkResult {
            records_written: data_count,
            bytes_written: file_size,
            last_position,
        })
    }

    async fn close(&mut self) -> Result<()> {
        // Flush any remaining staged files
        self.flush_staged_files().await?;

        // Drop normalizer channel → normalizer drains pending and exits
        self.normalize_tx = None;

        // Give normalizer time to process final batch
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        info!("Snowflake sink closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};
    use serial_test::serial;

    fn setup_snowflake_env() {
        std::env::set_var("SINK_SNOWFLAKE_ACCOUNT", "test_account");
        std::env::set_var("SINK_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH");
    }

    fn cleanup_snowflake_env() {
        std::env::remove_var("SINK_SNOWFLAKE_ACCOUNT");
        std::env::remove_var("SINK_SNOWFLAKE_WAREHOUSE");
        std::env::remove_var("SINK_SNOWFLAKE_ROLE");
        std::env::remove_var("SINK_SNOWFLAKE_PRIVATE_KEY_PATH");
        std::env::remove_var("SINK_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE");
        std::env::remove_var("SINK_SNOWFLAKE_MERGE_INTERVAL_MS");
        std::env::remove_var("SINK_SNOWFLAKE_SOFT_DELETE");
        std::env::remove_var("SINK_SCHEMA");
        std::env::remove_var("SOURCE_SLOT_NAME");
    }

    fn test_config() -> SinkConfig {
        SinkConfig {
            sink_type: SinkType::Snowflake,
            url: "test_account.snowflakecomputing.com".to_string(),
            port: 443,
            database: "test_db".to_string(),
            user: "test_user".to_string(),
            password: "test_pass".to_string(),
            specific: SinkSpecificConfig::Snowflake,
        }
    }

    #[test]
    #[serial]
    fn test_sink_creation() {
        setup_snowflake_env();
        let config = test_config();
        let sink = SnowflakeSink::new(&config, SinkMode::Primary);
        assert!(sink.is_ok());
        cleanup_snowflake_env();
    }

    #[test]
    #[serial]
    fn test_capabilities() {
        setup_snowflake_env();
        let config = test_config();
        let sink = SnowflakeSink::new(&config, SinkMode::Primary).unwrap();
        let caps = sink.capabilities();

        assert!(caps.supports_upsert);
        assert!(caps.supports_delete);
        assert!(caps.supports_schema_evolution);
        assert!(!caps.supports_transactions);
        assert!(matches!(
            caps.loading_model,
            LoadingModel::StagedBatch {
                stage_format: StageFormat::Parquet
            }
        ));
        cleanup_snowflake_env();
    }

    #[test]
    #[serial]
    fn test_batch_counter() {
        setup_snowflake_env();
        let config = test_config();
        let sink = SnowflakeSink::new(&config, SinkMode::Primary).unwrap();

        assert_eq!(sink.next_batch_id(), 1);
        assert_eq!(sink.next_batch_id(), 2);
        assert_eq!(sink.next_batch_id(), 3);
        cleanup_snowflake_env();
    }
}
