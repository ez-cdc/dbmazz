// Copyright 2025
// Licensed under the Elastic License v2.0

mod setup;
pub mod snapshot;
pub mod replication;

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::Config;
use crate::connectors::sinks::create_sink;
use crate::control::state::SharedState;
use crate::control::{self, CdcConfig, Stage};
use crate::core::{SinkMode, Source, SourcePosition};
use crate::pipeline::schema_cache::SchemaCache;
use crate::pipeline::{Pipeline, PipelineEvent};
use crate::source::postgres::{introspect_schemas, PostgresSource};
use crate::state_store::StateStore;
use setup::SetupManager;

/// Factory that creates fresh Sink instances (used by snapshot workers).
type SinkFactory = Arc<dyn Fn() -> anyhow::Result<Box<dyn crate::core::Sink>> + Send + Sync>;

/// Main CDC engine that orchestrates all components
pub struct CdcEngine {
    config: Config,
    shared_state: Arc<SharedState>,
    state_store: StateStore,
    /// SchemaCache for converting pgoutput CdcMessage -> generic CdcRecord.
    /// Owned by the engine, passed mutably to the WAL handler.
    #[allow(dead_code)]
    schema_cache: SchemaCache,
    /// Factory for creating sink instances (snapshot workers need their own).
    sink_factory: SinkFactory,
}

impl CdcEngine {
    pub async fn new(config: Config) -> Result<Self> {
        let slot_name = config.source.postgres().slot_name.clone();
        let cdc_config = CdcConfig {
            flush_size: config.flush_size,
            flush_interval_ms: config.flush_interval_ms,
            tables: config.source.tables.clone(),
            slot_name,
        };
        let shared_state = SharedState::new(cdc_config);

        let state_store = StateStore::new(&config.source.url).await?;

        let sink_config = config.sink.clone();
        let sink_factory: SinkFactory =
            Arc::new(move || create_sink(&sink_config, SinkMode::SnapshotWorker));

        Ok(Self {
            config,
            shared_state,
            state_store,
            schema_cache: SchemaCache::new(),
            sink_factory,
        })
    }

    /// Returns a clone of the SharedState Arc.
    /// Used by the demo mode to read metrics while the engine runs.
    #[allow(dead_code)]
    pub fn shared_state(&self) -> Arc<SharedState> {
        Arc::clone(&self.shared_state)
    }

    /// Execute CDC engine
    pub async fn run(self) -> Result<()> {
        self.shared_state
            .set_stage(Stage::Setup, "Initializing")
            .await;
        self.start_control_server();
        self.spawn_metrics_sampler();

        // Stage: SETUP - Source setup (replication slot, publication)
        self.shared_state
            .set_stage(Stage::Setup, "Running automatic setup")
            .await;
        if let Err(e) = self.run_setup().await {
            self.halt_on_setup_error(&e.to_string()).await;
        }

        // Stage: SETUP - Checkpoint
        self.shared_state
            .set_stage(Stage::Setup, "Loading checkpoint")
            .await;
        let start_lsn = self.load_checkpoint().await?;

        // Stage: SETUP - Source Connection
        self.shared_state
            .set_stage(Stage::Setup, "Connecting to source")
            .await;
        let mut source = self.init_source().await?;

        // Stage: SETUP - Source Setup (connections, schema introspection)
        self.shared_state
            .set_stage(Stage::Setup, "Setting up source")
            .await;
        source.setup(&self.config.source.tables).await?;

        // Stage: SETUP - Replication Stream start position
        let start_position = Some(SourcePosition::Lsn(start_lsn));

        // Stage: SETUP - Sink Connection
        self.shared_state
            .set_stage(Stage::Setup, "Connecting to sink")
            .await;
        let mut sink = create_sink(&self.config.sink, SinkMode::Primary)?;

        if let Err(e) = sink.validate_connection().await {
            self.halt_on_setup_error(&format!("Sink connection failed: {}", e))
                .await;
        }
        info!("  [OK] Sink connection verified");

        // Stage: SETUP - Sink setup (create target tables, raw tables, etc.)
        self.shared_state
            .set_stage(Stage::Setup, "Setting up sink")
            .await;
        let source_schemas =
            introspect_schemas(&self.config.source.url, &self.config.source.tables).await?;
        if let Err(e) = sink.setup(&source_schemas).await {
            self.halt_on_setup_error(&format!("Sink setup failed: {}", e))
                .await;
        }
        info!("  [OK] Sink setup complete");

        // Log sink capabilities
        let caps = sink.capabilities();
        info!("  Sink capabilities:");
        info!(
            "    - upsert: {}, delete: {}, schema_evolution: {}",
            caps.supports_upsert, caps.supports_delete, caps.supports_schema_evolution
        );
        info!(
            "    - optimal_flush_interval: {}ms",
            caps.optimal_flush_interval_ms
        );
        if let Some(max) = caps.max_batch_size {
            info!("    - max_batch_size: {}", max);
        }

        // Stage: SETUP - Pipeline
        self.shared_state
            .set_stage(Stage::Setup, "Initializing pipeline")
            .await;
        let (tx, feedback_rx) = self.init_pipeline(sink, &caps);

        // Stage: CDC - Ready to replicate
        self.shared_state.set_stage(Stage::Cdc, "Replicating").await;
        info!("Connected! Streaming CDC events...");

        // Spawn snapshot worker concurrently if enabled (DO_SNAPSHOT=true)
        if self.config.do_snapshot {
            self.spawn_snapshot_worker(self.config.initial_snapshot_only);
        }

        // Execute main loop via ReplicationLoop trait (source-agnostic)
        let ctx = crate::engine::replication::LoopContext {
            shared_state: self.shared_state.clone(),
            config: Arc::new(self.config.clone()),
            state_store: Arc::new(self.state_store.clone()),
            pipeline_tx: tx,
            feedback_rx,
            source_schemas: Arc::from(source_schemas.as_slice()),
            sink_factory: self.sink_factory.clone(),
        };

        let loop_impl = source.create_loop(start_position).await?;
        loop_impl.run(ctx).await
    }

    async fn halt_on_setup_error(&self, msg: &str) -> ! {
        self.shared_state
            .set_setup_error(Some(msg.to_string()))
            .await;
        self.shared_state
            .set_stage(Stage::Setup, "Setup failed")
            .await;
        error!("{}", msg);
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }

    /// Spawn a snapshot worker task.
    fn spawn_snapshot_worker(&self, shutdown_on_complete: bool) {
        let snap_config = Arc::new(self.config.clone());
        let snap_state = self.shared_state.clone();
        let snap_sink_factory = Arc::clone(&self.sink_factory);

        tokio::spawn(async move {
            match snapshot::run_snapshot(snap_config, snap_state.clone(), snap_sink_factory).await {
                Ok(()) => {
                    snap_state.set_snapshot_active(false);
                    info!("Snapshot completed successfully");
                    if shutdown_on_complete {
                        info!("Initial snapshot only mode: triggering graceful shutdown");
                        let _ = snap_state.shutdown_tx.send(true);
                    }
                }
                Err(e) => {
                    snap_state.set_snapshot_active(false);
                    snap_state.set_snapshot_error(Some(format!("{}", e))).await;
                    error!("Snapshot worker error: {}", e);
                }
            }
        });
        info!("Snapshot worker spawned");
    }

    /// Execute source setup (replication slot, publication).
    async fn run_setup(&self) -> Result<(), setup::SetupError> {
        let setup_manager = SetupManager::new(self.config.clone());
        setup_manager.run().await
    }

    /// Load checkpoint from StateStore
    async fn load_checkpoint(&self) -> Result<u64> {
        let slot_name = &self.config.source.postgres().slot_name;
        let last_lsn = self.state_store.load_checkpoint(slot_name).await?;
        let start_lsn = last_lsn.unwrap_or(0);

        if start_lsn > 0 {
            info!("Checkpoint: Resuming from LSN 0x{:X}", start_lsn);
        } else {
            info!("Checkpoint: Starting from beginning (no previous checkpoint)");
        }

        self.shared_state.update_lsn(start_lsn);
        self.shared_state.confirm_lsn(start_lsn);

        Ok(start_lsn)
    }

    fn start_control_server(&self) {
        let shared = self.shared_state.clone();
        let port = self.config.control_port;

        tokio::spawn(async move {
            if let Err(e) = control::start_control_server(port, shared).await {
                error!("server error: {}", e);
            }
        });
    }

    fn spawn_metrics_sampler(&self) {
        let shared = self.shared_state.clone();
        tokio::spawn(async move {
            control::run_metrics_sampler(shared).await;
        });
    }

    /// Initialize source (returns trait object for source-agnostic dispatch)
    async fn init_source(&self) -> Result<Box<dyn Source>> {
        let pg = self.config.source.postgres();
        let source = PostgresSource::new(
            &self.config.source.url,
            pg.slot_name.clone(),
            pg.publication_name.clone(),
        )
        .await?;
        Ok(Box::new(source))
    }

    /// Initialize pipeline with sink (core::Sink, no adapter)
    fn init_pipeline(
        &self,
        sink: Box<dyn crate::core::Sink>,
        caps: &crate::core::SinkCapabilities,
    ) -> (mpsc::Sender<PipelineEvent>, mpsc::Receiver<u64>) {
        // Job/config values always win. Sink capabilities are only fallback/advisory.
        let batch_size = if self.config.flush_size > 0 {
            self.config.flush_size
        } else {
            caps.max_batch_size.unwrap_or(10_000)
        };
        let flush_interval_ms = if self.config.flush_interval_ms > 0 {
            self.config.flush_interval_ms
        } else if caps.optimal_flush_interval_ms > 0 {
            caps.optimal_flush_interval_ms
        } else {
            5_000
        };

        info!("  Pipeline config (effective):");
        info!(
            "    - batch_size: {} (job/config: {}, sink_max: {:?})",
            batch_size, self.config.flush_size, caps.max_batch_size
        );
        info!(
            "    - flush_interval: {}ms (job/config: {}ms, sink_optimal: {}ms)",
            flush_interval_ms, self.config.flush_interval_ms, caps.optimal_flush_interval_ms
        );

        let (tx, rx) = mpsc::channel(batch_size * 2);
        let (feedback_tx, feedback_rx) = mpsc::channel::<u64>(100);

        let pipeline = Pipeline::new(
            rx,
            sink,
            batch_size,
            Duration::from_millis(flush_interval_ms),
        )
        .with_feedback_channel(feedback_tx)
        .with_shared_state(self.shared_state.clone());

        tokio::spawn(pipeline.run());

        (tx, feedback_rx)
    }

}

/// Flow control for replication loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlow {
    Continue,
    Break,
}
