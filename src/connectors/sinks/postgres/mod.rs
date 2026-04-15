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
//!   └── Wake normalizer via watch::Sender (async background task)
//!         ├── MERGE INTO dst_table (PG >= 15)
//!         ├── DELETE FROM raw_table (cleanup)
//!         └── UPDATE metadata (normalize_batch_id)
//! ```

mod merge_generator;
mod normalizer;
mod raw_table;
pub mod schema_tracking;
mod setup;
mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

use crate::config::SinkConfig;
use crate::core::position::SourcePosition;
use crate::core::record::CdcRecord;
use crate::core::traits::{
    LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult, SourceTableSchema, StageFormat,
};
use schema_tracking::SchemaState;

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
    /// Shutdown flag for the normalizer background task.
    normalize_shutdown: Option<Arc<AtomicBool>>,
    /// JoinHandle for the normalizer task — awaited in close().
    normalizer_handle: Option<JoinHandle<()>>,
    /// Watch sender for schema state updates. Populated after setup(). The
    /// normalizer holds the corresponding Receiver. Dropped after the
    /// normalizer has joined in close().
    schema_tx: Option<watch::Sender<SchemaState>>,
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

        // Snapshot workers never have `setup()` called on them (the engine only
        // calls setup() on the primary sink) and never see SchemaChange events
        // (snapshot phase reads source via SELECT, not WAL decode). Wire a no-op
        // watch channel here so `write_batch_to_raw`'s Phase 1 snapshot read and
        // Phase 3 broadcast both have a valid sender to talk to. The receiver is
        // dropped immediately — `send_replace` is infallible regardless of
        // receiver count, and `pending_diffs` is always empty for snapshot
        // workers because their batches never contain SchemaChange records.
        // For Primary mode, schema_tx stays None until setup() populates it.
        let schema_tx = match mode {
            SinkMode::SnapshotWorker => {
                let (tx, _rx) = watch::channel(Arc::new(HashMap::new()));
                Some(tx)
            }
            SinkMode::Primary => None,
        };

        Ok(Self {
            url: config.url.clone(),
            schema: pg_config.schema.clone(),
            job_name: pg_config.job_name.clone(),
            database: config.database.clone(),
            client: None,
            raw_table_name,
            normalize_shutdown: None,
            normalizer_handle: None,
            schema_tx,
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
            supports_schema_evolution: true,
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
        let target_schema = self.schema.clone();
        let job_name = self.job_name.clone();
        let url = self.url.clone();
        let raw_table = self.raw_table_name.clone();
        let mode = self.mode;

        // Step 1: run structural setup (raw table, metadata table, target tables).
        // We take an immutable borrow first via &*client, then drop it before the
        // mutable borrow needed by initialize_schema_state.
        {
            let client = self.connect().await?;
            setup::run_setup(&*client, &target_schema, &job_name, source_schemas)
                .await
                .context("PostgresSink: setup failed")?;
        }

        // Step 2: bootstrap schema tracking state (requires &mut Client for txns).
        let initial_map = {
            let client = self.connect().await?;
            setup::initialize_schema_state(client, &target_schema, &job_name, source_schemas)
                .await
                .context("PostgresSink: initialize_schema_state failed")?
        };

        // Step 3: wrap in watch::channel so the normalizer can receive updates.
        let initial_state: SchemaState = Arc::new(initial_map);
        let (schema_tx, schema_rx) = watch::channel(initial_state);
        self.schema_tx = Some(schema_tx);

        // Step 4: spawn normalizer only in Primary mode.
        if mode == SinkMode::Primary {
            let normalizer_config = normalizer::NormalizerConfig {
                url,
                target_schema,
                job_name: job_name.clone(),
                raw_table,
                schema_rx,
            };
            let (shutdown, handle) = normalizer::spawn_normalizer(normalizer_config);
            self.normalize_shutdown = Some(shutdown);
            self.normalizer_handle = Some(handle);

            info!(
                "PostgresSink: setup complete ({} tables, normalizer started)",
                source_schemas.len()
            );
        } else {
            // Snapshot-worker mode: no normalizer. Drop the receiver here (Risk #4).
            // schema_tx stays on self so Section 4's `send_replace` calls from
            // raw_table still have a valid sender. `send_replace` is infallible
            // regardless of whether any receivers exist — it always overwrites
            // the stored value and returns the previous one.
            drop(schema_rx);
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

        // Clone values needed after mutable borrow of self.
        let raw_table = self.raw_table_name.clone();
        let job_name = self.job_name.clone();
        let target_schema = self.schema.clone();

        // Establish (or reuse) the lazy connection. connect() takes &mut self, so
        // it must complete before we borrow self.schema_tx immutably below.
        // The two fields (client vs schema_tx) are disjoint, but Rust's borrow
        // checker cannot prove that across a method boundary, so we inline the
        // lazy-connect logic here to split the borrows by field.
        if self.client.is_none() {
            let (pg_client, connection) = tokio_postgres::connect(&self.url, NoTls)
                .await
                .context("PostgresSink: failed to connect to target PostgreSQL")?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("PostgresSink connection error: {}", e);
                }
            });
            self.client = Some(pg_client);
        }

        // Both borrows below are now on disjoint fields: `client` (from
        // self.client) and `schema_tx` (from self.schema_tx). The compiler
        // can verify this because we access them directly via field paths,
        // not through a shared &mut self method call.
        let schema_tx = self.schema_tx.as_ref().expect(
            "PostgresSink::write_batch called before setup() — schema_tx must be initialized",
        );
        let client = self
            .client
            .as_mut()
            .expect("client was just initialized above");

        let (records_written, bytes_written) = raw_table::write_batch_to_raw(
            client,
            &raw_table,
            &target_schema,
            &job_name,
            lsn,
            &records,
            schema_tx,
        )
        .await?;

        Ok(SinkResult {
            records_written,
            bytes_written,
            last_position,
        })
    }

    async fn close(&mut self) -> Result<()> {
        info!("PostgresSink: closing...");

        // Signal normalizer to shut down. The shutdown flag will be picked up
        // within 2 seconds by the normalizer's polling fallback.
        if let Some(ref shutdown) = self.normalize_shutdown {
            shutdown.store(true, Ordering::Relaxed);
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

        // Drop schema_tx AFTER the normalizer has joined (Risk #1 mitigation).
        // The Receiver inside the normalizer task is already gone at this point,
        // so this drop is a no-op for the channel itself.
        self.schema_tx = None;
        self.normalize_shutdown = None;
        self.client = None;
        info!("PostgresSink: closed");
        Ok(())
    }
}
