// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL sink connector.

mod merge_generator;
mod normalizer;
mod raw_table;
mod setup;
mod types;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

use crate::config::SinkConfig;
use crate::core::position::SourcePosition;
use crate::core::record::CdcRecord;
use crate::core::traits::{
    LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult, SourceTableSchema, StageFormat,
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
    /// Notify handle to wake the normalizer when new batches are written.
    normalize_notify: Option<Arc<Notify>>,
    /// Shutdown flag for the normalizer background task.
    normalize_shutdown: Option<Arc<AtomicBool>>,
    /// JoinHandle for the normalizer task — awaited in close().
    normalizer_handle: Option<JoinHandle<()>>,
    /// Sink operating mode
    mode: SinkMode,
}

impl PostgresSink {
    /// Create a new PostgresSink from the provided configuration.
    /// Connection is lazy — established on first use.
    pub fn new(config: &SinkConfig, mode: SinkMode) -> Result<Self> {
        let pg_config = config.postgres_config()?;

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
            normalize_notify: None,
            normalize_shutdown: None,
            normalizer_handle: None,
            mode,
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

        let conn_handle = tokio::spawn(async move {
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
            drop(client);
            conn_handle.abort();
            anyhow::bail!(
                "PostgresSink requires PostgreSQL >= 15 for MERGE support (found version: {})",
                version_str
            );
        }

        info!(
            "PostgresSink: connection OK (version: {}, schema: {})",
            version_str, self.schema
        );
        drop(client);
        conn_handle.abort();
        Ok(())
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        let schema = self.schema.clone();
        let job_name = self.job_name.clone();
        let client = self.connect().await?;
        setup::run_setup(&*client, &schema, &job_name, source_schemas).await?;

        // Spawn normalizer background task (skip for snapshot worker instances —
        // the primary sink's normalizer handles all batches via polling)
        if self.mode == SinkMode::Primary {
            let normalizer_config = normalizer::NormalizerConfig {
                url: self.url.clone(),
                target_schema: self.schema.clone(),
                job_name: self.job_name.clone(),
                raw_table: self.raw_table_name.clone(),
                table_schemas: source_schemas.to_vec(),
            };
            let (notify, shutdown, handle) = normalizer::spawn_normalizer(normalizer_config);
            self.normalize_notify = Some(notify);
            self.normalize_shutdown = Some(shutdown);
            self.normalizer_handle = Some(handle);

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

        // Notify normalizer (non-blocking — if it's already awake it will catch up)
        if let Some(ref notify) = self.normalize_notify {
            notify.notify_one();
        }

        Ok(SinkResult {
            records_written,
            bytes_written,
            last_position,
        })
    }

    async fn close(&mut self) -> Result<()> {
        info!("PostgresSink: closing...");

        // Signal normalizer to shut down and process remaining batches.
        if let Some(ref shutdown) = self.normalize_shutdown {
            shutdown.store(true, Ordering::Relaxed);
        }
        if let Some(ref notify) = self.normalize_notify {
            notify.notify_one();
        }

        // Await normalizer completion with a 30-second timeout.
        if let Some(handle) = self.normalizer_handle.take() {
            match tokio::time::timeout(Duration::from_secs(30), handle).await {
                Ok(Ok(())) => {
                    info!("PostgresSink: normalizer shut down cleanly");
                }
                Ok(Err(e)) => {
                    error!("PostgresSink: normalizer task panicked: {}", e);
                }
                Err(_) => {
                    warn!(
                        "PostgresSink: normalizer did not finish within 30s, proceeding with shutdown"
                    );
                }
            }
        }

        self.normalize_notify = None;
        self.normalize_shutdown = None;
        self.client = None;
        info!("PostgresSink: closed");
        Ok(())
    }
}
