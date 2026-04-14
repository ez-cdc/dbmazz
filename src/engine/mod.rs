// Copyright 2025
// Licensed under the Elastic License v2.0

mod setup;
pub mod snapshot;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::connectors::sinks::create_sink;
use crate::core::SinkMode;
use crate::grpc::state::SharedState;
use crate::grpc::{self, CdcConfig, CdcState, Stage};
use crate::pipeline::schema_cache::SchemaCache;
use crate::pipeline::{Pipeline, PipelineEvent};
use crate::replication::{
    handle_keepalive, handle_xlog_data, parse_replication_message, WalMessage,
};
use crate::source::postgres::{build_standby_status_update, introspect_schemas, PostgresSource};
use crate::state_store::StateStore;
use setup::SetupManager;

/// Factory that creates fresh Sink instances (used by snapshot workers).
type SinkFactory = Arc<dyn Fn() -> anyhow::Result<Box<dyn crate::core::Sink>> + Send + Sync>;

/// Main CDC engine that orchestrates all components
pub struct CdcEngine {
    config: Config,
    shared_state: Arc<SharedState>,
    state_store: StateStore,
    /// SchemaCache for converting pgoutput CdcMessage → generic CdcRecord.
    /// Owned by the engine, passed mutably to the WAL handler.
    schema_cache: SchemaCache,
    /// Factory for creating sink instances (snapshot workers need their own).
    sink_factory: SinkFactory,
}

impl CdcEngine {
    pub async fn new(config: Config) -> Result<Self> {
        let cdc_config = CdcConfig {
            flush_size: config.flush_size,
            flush_interval_ms: config.flush_interval_ms,
            tables: config.source.tables.clone(),
            slot_name: config.source.postgres().slot_name.clone(),
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
    pub async fn run(mut self) -> Result<()> {
        // Stage: SETUP - gRPC Server (start FIRST so health checks respond immediately)
        self.shared_state
            .set_stage(Stage::Setup, "Initializing")
            .await;
        self.start_grpc_server();

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
            .set_stage(Stage::Setup, "Connecting to PostgreSQL")
            .await;
        let source = self.init_source().await?;

        // Stage: SETUP - Replication Stream
        self.shared_state
            .set_stage(Stage::Setup, "Starting replication stream")
            .await;
        let replication_stream = source.start_replication_from(start_lsn).await?;
        tokio::pin!(replication_stream);

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
        // The WAL consumer continues running in parallel; deduplication is handled
        // via should_emit() in wal_handler using the finished_chunks BTreeMap.
        if self.config.do_snapshot {
            self.spawn_snapshot_worker(self.config.initial_snapshot_only);
        }

        // Execute main loop
        self.run_main_loop(replication_stream, tx, feedback_rx)
            .await
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

    /// Spawn a snapshot worker task. Used for both initial (DO_SNAPSHOT=true)
    /// and on-demand (StartSnapshot gRPC RPC) snapshots.
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

    /// Start gRPC server in background
    fn start_grpc_server(&self) {
        let grpc_state = self.shared_state.clone();
        let grpc_port = self.config.grpc_port;

        tokio::spawn(async move {
            if let Err(e) = grpc::start_grpc_server(grpc_port, grpc_state).await {
                error!("gRPC server error: {}", e);
            }
        });
    }

    /// Initialize PostgreSQL source
    async fn init_source(&self) -> Result<PostgresSource> {
        let pg = self.config.source.postgres();
        let source = PostgresSource::new(
            &self.config.source.url,
            pg.slot_name.clone(),
            pg.publication_name.clone(),
        )
        .await?;

        Ok(source)
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

    /// Main replication loop
    async fn run_main_loop<S>(
        &mut self,
        mut replication_stream: S,
        tx: mpsc::Sender<PipelineEvent>,
        mut feedback_rx: mpsc::Receiver<u64>,
    ) -> Result<()>
    where
        S: StreamExt<Item = Result<bytes::Bytes, tokio_postgres::Error>>
            + SinkExt<bytes::Bytes>
            + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut shutdown_rx = self.shared_state.shutdown_tx.subscribe();
        // Subscribe to on-demand snapshot trigger (fired by StartSnapshot gRPC RPC)
        let mut snapshot_trigger_rx = self.shared_state.subscribe_snapshot_trigger();
        let mut iteration = 0u64;

        loop {
            iteration = iteration.wrapping_add(1);

            // 1. Check state changes every 256 iterations to reduce overhead
            // With ~287 events/s, this checks state ~1x/second instead of 287x/second
            if iteration & 0xFF == 0 {
                if let Some(flow) = self.check_state_control_sync(&tx) {
                    match flow {
                        ControlFlow::Break => break,
                        ControlFlow::Continue => {
                            // Sleep when paused
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }
            }

            // 2. Main select loop
            tokio::select! {
                // Shutdown signal
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received");
                        break;
                    }
                }

                // On-demand snapshot trigger (from StartSnapshot gRPC RPC)
                Ok(()) = snapshot_trigger_rx.changed() => {
                    if *snapshot_trigger_rx.borrow() && !self.shared_state.is_snapshot_active() {
                        info!("On-demand snapshot triggered (CDC_RUNNING → SNAPSHOT)");
                        // Reset trigger so it doesn't fire again
                        let _ = self.shared_state.snapshot_trigger.send(false);
                        self.spawn_snapshot_worker(false);
                    }
                }

                // Replication messages
                data_res = replication_stream.next() => {
                    match data_res {
                        Some(Ok(mut data)) => {
                            if let Some(msg) = parse_replication_message(&mut data) {
                                let _ = self.handle_replication_message(
                                    msg,
                                    &tx,
                                    &mut replication_stream,
                                ).await?;
                            }
                        }
                        Some(Err(e)) => {
                            error!("Replication stream error: {}", e);
                            break;
                        }
                        None => {
                            warn!("Replication stream ended");
                            break;
                        }
                    }
                }

                // Checkpoint feedback
                Some(confirmed_lsn) = feedback_rx.recv() => {
                    self.handle_checkpoint_feedback(
                        confirmed_lsn,
                        &mut replication_stream,
                    ).await?;
                }
            }
        }

        // Cleanup PostgreSQL resources (drop replication slot) - unless skip_slot_cleanup is set
        if self.shared_state.should_skip_slot_cleanup() {
            info!("[SKIP] Skipping slot cleanup (upgrade/restart mode)");
        } else if let Err(e) = setup::cleanup_postgres_resources(
            &self.config.source.url,
            &self.config.source.postgres().slot_name,
        )
        .await
        {
            warn!("Cleanup warning: {}", e);
            // Non-fatal - continue shutdown
        }

        info!("CDC shutdown complete");
        Ok(())
    }

    /// Check CDC state (Pause/Stop/Draining) - Synchronous
    fn check_state_control_sync(&self, tx: &mpsc::Sender<PipelineEvent>) -> Option<ControlFlow> {
        let current_state = self.shared_state.state();

        match current_state {
            CdcState::Stopped => {
                info!("CDC stopped. Exiting immediately.");
                Some(ControlFlow::Break)
            }
            CdcState::Draining => {
                // Check if channel is empty
                if tx.capacity() == self.config.flush_size * 2 {
                    info!("CDC drained. Exiting gracefully.");
                    self.shared_state.set_state(CdcState::Stopped);
                    Some(ControlFlow::Break)
                } else {
                    None // Continue draining
                }
            }
            CdcState::Paused => {
                // Return signal to sleep
                Some(ControlFlow::Continue)
            }
            CdcState::Running => None, // Normal operation
        }
    }

    /// Handle replication messages
    async fn handle_replication_message<S>(
        &mut self,
        msg: WalMessage,
        tx: &mpsc::Sender<PipelineEvent>,
        replication_stream: &mut S,
    ) -> Result<u64>
    where
        S: SinkExt<bytes::Bytes> + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        match msg {
            WalMessage::XLogData { lsn, data } => {
                handle_xlog_data(
                    data,
                    lsn,
                    tx,
                    &self.shared_state,
                    &mut self.schema_cache,
                    self.config.flush_size,
                )
                .await?;
                Ok(lsn)
            }
            WalMessage::KeepAlive {
                lsn,
                reply_requested,
            } => {
                handle_keepalive(lsn, reply_requested, replication_stream).await?;
                Ok(lsn)
            }
            WalMessage::Unknown(tag) => {
                warn!("Unknown replication message tag: {}", tag);
                Ok(0)
            }
        }
    }

    /// Handle checkpoint confirmation
    async fn handle_checkpoint_feedback<S>(
        &self,
        confirmed_lsn: u64,
        replication_stream: &mut S,
    ) -> Result<()>
    where
        S: SinkExt<bytes::Bytes> + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        // CRITICAL: We MUST save checkpoint before confirming to PostgreSQL.
        // If we confirm to PostgreSQL but fail to save locally, we could:
        // 1. PostgreSQL discards WAL (thinking we persisted it)
        // 2. On crash, we restart from old checkpoint
        // 3. WAL data is gone → permanent data loss

        // 1. Save checkpoint to persistent storage
        let slot_name = &self.config.source.postgres().slot_name;
        if let Err(e) = self
            .state_store
            .save_checkpoint(slot_name, confirmed_lsn)
            .await
        {
            error!(
                "Failed to save checkpoint at LSN 0x{:X}: {}",
                confirmed_lsn, e
            );
            error!("NOT confirming to PostgreSQL to prevent data loss");
            // Return error to halt replication loop
            return Err(e);
        }

        // 2. Update SharedState (after successful persistence)
        self.shared_state.confirm_lsn(confirmed_lsn);

        // 3. Confirm to PostgreSQL (only after checkpoint is safely persisted)
        let status = build_standby_status_update(confirmed_lsn);
        if let Err(e) = replication_stream.send(status).await {
            error!("Failed to send status update to PostgreSQL: {}", e);
            error!("Checkpoint saved but not confirmed - may cause duplicate events on restart");
            return Err(anyhow::Error::new(e));
        }

        debug!("Checkpoint confirmed: LSN 0x{:X}", confirmed_lsn);
        Ok(())
    }
}

/// Flow control for the loop
enum ControlFlow {
    Continue,
    Break,
}
