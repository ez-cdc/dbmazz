// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL sink connector.
//!
//! Replicates CDC changes to a target PostgreSQL database using a
//! raw table + normalize pattern (MERGE, PG >= 15).
//!
//! ## CDC Flow
//!
//! ```text
//! write_batch(Vec<CdcRecord>)
//!   ├── COPY INTO _dbmazz._raw_{job} (staging)
//!   ├── UPDATE _dbmazz._metadata (lsn, sync_batch_id)
//!   ├── COMMIT (atomic)
//!   └── Notify normalizer (async background task)
//!         ├── MERGE INTO dst_table (PG >= 15)
//!         ├── DELETE FROM raw_table (cleanup)
//!         └── UPDATE metadata (normalize_batch_id)
//! ```

mod merge_generator;
mod normalizer;
mod raw_table;
mod setup;
mod types;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};
use tracing::info;

use crate::config::SinkConfig;
use crate::core::position::SourcePosition;
use crate::core::record::CdcRecord;
use crate::core::traits::{
    LoadingModel, Sink, SinkCapabilities, SinkResult, SourceTableSchema, StageFormat,
};

/// PostgreSQL sink — writes CDC records to a target PostgreSQL database.
pub struct PostgresSink {
    /// Target PostgreSQL connection URL
    url: String,
    /// Target schema (default: "public")
    schema: String,
    /// Job name for raw table naming and metadata tracking
    job_name: String,
    /// Database name (for logging)
    #[allow(dead_code)]
    database: String,
    /// Lazy connection — established on connect()
    client: Option<Client>,
    /// Full name of the raw table (e.g., _dbmazz._raw_dbmazz_slot)
    raw_table_name: String,
    /// Channel to notify normalizer of new batches
    normalize_tx: Option<mpsc::Sender<i64>>,
    /// Skip normalizer spawn (set for snapshot worker instances created via sink_factory)
    pub(crate) skip_normalizer: bool,
}

impl PostgresSink {
    /// Create a new PostgresSink from the provided configuration.
    /// Connection is lazy — established on first use.
    pub fn new(config: &SinkConfig) -> Result<Self> {
        let pg_config = config
            .postgres
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PostgresSinkConfig is required for postgres sink"))?;

        let safe_job = pg_config
            .job_name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();
        let raw_table_name = format!("_dbmazz._raw_{}", safe_job);

        info!(
            "PostgresSink initialized: db={}, schema={}, job={}",
            config.database, pg_config.schema, pg_config.job_name
        );

        Ok(Self {
            url: config.url.clone(),
            schema: pg_config.schema.clone(),
            job_name: pg_config.job_name.clone(),
            database: config.database.clone(),
            client: None,
            raw_table_name,
            normalize_tx: None,
            skip_normalizer: false,
        })
    }

    /// Get or establish connection to target PostgreSQL
    async fn connect(&mut self) -> Result<&mut Client> {
        if self.client.is_none() {
            let (client, connection) = tokio_postgres::connect(&self.url, NoTls)
                .await
                .context("PostgresSink: failed to connect to target PostgreSQL")?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("PostgresSink connection error: {}", e);
                }
            });

            self.client = Some(client);
        }
        Ok(self.client.as_mut().unwrap())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: false,
            supports_transactions: true,
            loading_model: LoadingModel::StagedBatch {
                stage_format: StageFormat::Json,
            },
            min_batch_size: Some(1),
            max_batch_size: Some(250_000),
            optimal_flush_interval_ms: 10_000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.url, NoTls)
            .await
            .context("PostgresSink: connection validation failed")?;

        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Verify PG >= 15 (required for MERGE)
        let version_row = client
            .query_one("SHOW server_version_num", &[])
            .await
            .context("PostgresSink: failed to get server version")?;
        let version_str: String = version_row.get(0);
        let version_num: i32 = version_str.trim().parse().unwrap_or(0);
        if version_num < 150000 {
            anyhow::bail!(
                "PostgresSink requires PostgreSQL >= 15 for MERGE support (found version: {})",
                version_str
            );
        }

        info!(
            "PostgresSink: connection OK (version: {}, schema: {})",
            version_str, self.schema
        );
        Ok(())
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        let schema = self.schema.clone();
        let job_name = self.job_name.clone();
        let client = self.connect().await?;
        setup::run_setup(&*client, &schema, &job_name, source_schemas).await?;

        // Spawn normalizer background task (skip for snapshot worker instances —
        // the primary sink's normalizer handles all batches via polling)
        if !self.skip_normalizer {
            let normalizer_config = normalizer::NormalizerConfig {
                url: self.url.clone(),
                target_schema: self.schema.clone(),
                job_name: self.job_name.clone(),
                raw_table: self.raw_table_name.clone(),
                table_schemas: source_schemas.to_vec(),
            };
            let tx = normalizer::spawn_normalizer(normalizer_config);
            self.normalize_tx = Some(tx);

            info!(
                "PostgresSink: setup complete ({} tables, normalizer started)",
                source_schemas.len()
            );
        } else {
            info!(
                "PostgresSink: setup complete ({} tables, normalizer skipped — snapshot worker)",
                source_schemas.len()
            );
        }
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

        // Track last position for checkpoint
        let last_position = records.iter().rev().find_map(|r| match r {
            CdcRecord::Insert { position, .. }
            | CdcRecord::Update { position, .. }
            | CdcRecord::Delete { position, .. }
            | CdcRecord::Commit { position, .. }
            | CdcRecord::Heartbeat { position, .. } => Some(position.clone()),
            _ => None,
        });

        // Extract LSN for metadata
        let lsn = match &last_position {
            Some(SourcePosition::Lsn(l)) => *l,
            _ => 0,
        };

        // Clone values needed after mutable borrow
        let raw_table = self.raw_table_name.clone();
        let job_name = self.job_name.clone();

        let client = self.connect().await?;
        let (records_written, bytes_written) =
            raw_table::write_batch_to_raw(client, &raw_table, &job_name, lsn, &records).await?;

        // Notify normalizer (non-blocking — if channel is full, normalizer will catch up)
        if let Some(ref tx) = self.normalize_tx {
            let _ = tx.try_send(0);
        }

        Ok(SinkResult {
            records_written,
            bytes_written,
            last_position,
        })
    }

    async fn close(&mut self) -> Result<()> {
        info!("PostgresSink: closing...");

        // Drop normalizer channel → normalizer will drain pending and exit
        self.normalize_tx = None;

        // Give normalizer a moment to process remaining batches
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        self.client = None;
        info!("PostgresSink: closed");
        Ok(())
    }
}
