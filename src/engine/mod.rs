// Copyright 2025
// Licensed under the Elastic License v2.0

mod setup;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::grpc::{self, CdcConfig, CdcState, Stage};
use crate::grpc::state::SharedState;
use crate::pipeline::Pipeline;
use crate::replication::{parse_replication_message, handle_xlog_data, handle_keepalive, WalMessage};
use setup::SetupManager;
use crate::sink::NewSinkAdapter;
use crate::source::postgres::{PostgresSource, build_standby_status_update};
use crate::state_store::StateStore;
use crate::connectors::sinks::create_sink;

/// Main CDC engine that orchestrates all components
pub struct CdcEngine {
    config: Config,
    shared_state: Arc<SharedState>,
    state_store: Option<StateStore>,
}

impl CdcEngine {
    /// Create new CdcEngine
    /// NOTE: Does NOT connect to PostgreSQL here. The gRPC server must start first
    /// so the worker-agent health check can succeed. StateStore is initialized lazily
    /// in run() after the gRPC server is listening.
    pub fn new(config: Config) -> Self {
        let cdc_config = CdcConfig {
            flush_size: config.flush_size,
            flush_interval_ms: config.flush_interval_ms,
            tables: config.tables.clone(),
            slot_name: config.slot_name.clone(),
        };
        let shared_state = SharedState::new(cdc_config);

        Self {
            config,
            shared_state,
            state_store: None,
        }
    }

    /// Execute CDC engine
    pub async fn run(mut self) -> Result<()> {
        // Stage: SETUP - gRPC Server (start FIRST so health checks respond immediately)
        self.shared_state.set_stage(Stage::Setup, "Starting gRPC server").await;
        self.start_grpc_server();

        // Stage: SETUP - Initialize StateStore (connects to PostgreSQL for checkpoints)
        self.shared_state.set_stage(Stage::Setup, "Connecting to checkpoint store").await;
        let state_store = StateStore::new(&self.config.database_url).await?;
        self.state_store = Some(state_store);

        // Stage: SETUP - Execute automatic setup
        self.shared_state.set_stage(Stage::Setup, "Running automatic setup").await;
        if let Err(e) = self.run_setup().await {
            // Save error in SharedState for Health Check
            self.shared_state.set_setup_error(Some(e.to_string())).await;
            self.shared_state.set_stage(Stage::Setup, "Setup failed").await;
            error!("Setup failed: {}", e);
            // Keep gRPC server running so control plane can query the error
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }

        // Stage: SETUP - Checkpoint
        self.shared_state.set_stage(Stage::Setup, "Loading checkpoint").await;
        let start_lsn = self.load_checkpoint().await?;

        // Stage: SETUP - Source Connection
        self.shared_state.set_stage(Stage::Setup, "Connecting to PostgreSQL").await;
        let source = self.init_source().await?;
        
        // Stage: SETUP - Replication Stream
        self.shared_state.set_stage(Stage::Setup, "Starting replication stream").await;
        let replication_stream = source.start_replication_from(start_lsn).await?;
        tokio::pin!(replication_stream);

        // Stage: SETUP - Sink Connection
        self.shared_state.set_stage(Stage::Setup, "Connecting to sink").await;
        let sink_adapter = self.init_sink()?;

        // Verify HTTP connectivity BEFORE declaring CDC ready
        if let Err(e) = sink_adapter.verify_http_connection().await {
            let error_msg = format!("Sink HTTP connection failed: {}", e);
            self.shared_state.set_setup_error(Some(error_msg.clone())).await;
            self.shared_state.set_stage(Stage::Setup, "Setup failed").await;
            error!("{}", error_msg);
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
        info!("  [OK] Sink HTTP endpoint accessible");

        // Log sink capabilities
        let caps = sink_adapter.capabilities();
        info!("  Sink capabilities:");
        info!("    - upsert: {}, delete: {}, schema_evolution: {}",
            caps.supports_upsert, caps.supports_delete, caps.supports_schema_evolution);
        info!("    - optimal_flush_interval: {}ms", caps.optimal_flush_interval_ms);
        if let Some(max) = caps.max_batch_size {
            info!("    - max_batch_size: {}", max);
        }

        // Stage: SETUP - Pipeline
        self.shared_state.set_stage(Stage::Setup, "Initializing pipeline").await;
        let (tx, feedback_rx) = self.init_pipeline(sink_adapter, &caps);

        // Stage: CDC - Ready to replicate
        self.shared_state.set_stage(Stage::Cdc, "Replicating").await;
        info!("Connected! Streaming CDC events...");

        // 6. Execute main loop
        self.run_main_loop(
            replication_stream,
            tx,
            feedback_rx,
            start_lsn,
        ).await
    }

    /// Execute automatic setup (PostgreSQL + StarRocks)
    async fn run_setup(&self) -> Result<(), setup::SetupError> {
        let setup_manager = SetupManager::new(self.config.clone());
        setup_manager.run().await
    }

    /// Load checkpoint from StateStore
    async fn load_checkpoint(&self) -> Result<u64> {
        let state_store = self.state_store.as_ref()
            .expect("state_store must be initialized before load_checkpoint");
        let last_lsn = state_store.load_checkpoint(&self.config.slot_name).await?;
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
        let source = PostgresSource::new(
            &self.config.database_url,
            self.config.slot_name.clone(),
            self.config.publication_name.clone(),
        ).await?;

        Ok(source)
    }

    /// Initialize sink using trait-based connectors
    fn init_sink(&self) -> Result<NewSinkAdapter> {
        let core_sink = create_sink(&self.config.sink)?;
        Ok(NewSinkAdapter::new(core_sink))
    }

    /// Initialize pipeline with sink adapter
    fn init_pipeline(
        &self,
        sink: NewSinkAdapter,
        caps: &crate::core::SinkCapabilities,
    ) -> (mpsc::Sender<crate::source::parser::CdcEvent>, mpsc::Receiver<u64>) {
        // Use sink capabilities to configure batching, with config as fallback
        let batch_size = caps.max_batch_size.unwrap_or(self.config.flush_size);
        let flush_interval_ms = if caps.optimal_flush_interval_ms > 0 {
            caps.optimal_flush_interval_ms
        } else {
            self.config.flush_interval_ms
        };

        info!("  Pipeline config (from sink capabilities):");
        info!("    - batch_size: {} (config: {})", batch_size, self.config.flush_size);
        info!("    - flush_interval: {}ms (config: {}ms)", flush_interval_ms, self.config.flush_interval_ms);

        let (tx, rx) = mpsc::channel(batch_size * 2);
        let (feedback_tx, feedback_rx) = mpsc::channel::<u64>(100);

        let pipeline = Pipeline::new(
            rx,
            Box::new(sink),
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
        &self,
        mut replication_stream: S,
        tx: mpsc::Sender<crate::source::parser::CdcEvent>,
        mut feedback_rx: mpsc::Receiver<u64>,
        _start_lsn: u64,
    ) -> Result<()>
    where
        S: StreamExt<Item = Result<bytes::Bytes, tokio_postgres::Error>>
            + SinkExt<bytes::Bytes>
            + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut shutdown_rx = self.shared_state.shutdown_tx.subscribe();
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
            &self.config.database_url,
            &self.config.slot_name,
        ).await {
            warn!("Cleanup warning: {}", e);
            // Non-fatal - continue shutdown
        }

        info!("CDC shutdown complete");
        Ok(())
    }

    /// Check CDC state (Pause/Stop/Draining) - Synchronous
    fn check_state_control_sync(
        &self,
        tx: &mpsc::Sender<crate::source::parser::CdcEvent>,
    ) -> Option<ControlFlow> {
        let current_state = self.shared_state.state();
        
        match current_state {
            CdcState::Stopped => {
                info!("CDC stopped by control plane. Exiting immediately.");
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
        &self,
        msg: WalMessage,
        tx: &mpsc::Sender<crate::source::parser::CdcEvent>,
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
                    self.config.flush_size,
                ).await?;
                Ok(lsn)
            }
            WalMessage::KeepAlive { lsn, reply_requested } => {
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
        let state_store = self.state_store.as_ref()
            .expect("state_store must be initialized before checkpoint feedback");
        if let Err(e) = state_store
            .save_checkpoint(&self.config.slot_name, confirmed_lsn)
            .await
        {
            error!("Failed to save checkpoint at LSN 0x{:X}: {}", confirmed_lsn, e);
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

