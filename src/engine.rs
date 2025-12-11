use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::grpc::{self, CdcConfig, CdcState, Stage};
use crate::grpc::state::SharedState;
use crate::pipeline::Pipeline;
use crate::replication::{parse_replication_message, handle_xlog_data, handle_keepalive, WalMessage};
use crate::sink::starrocks::StarRocksSink;
use crate::source::postgres::{PostgresSource, build_standby_status_update};
use crate::state_store::StateStore;

/// Motor principal de CDC que orquesta todos los componentes
pub struct CdcEngine {
    config: Config,
    shared_state: Arc<SharedState>,
    state_store: StateStore,
}

impl CdcEngine {
    /// Crear nuevo CdcEngine
    pub async fn new(config: Config) -> Result<Self> {
        // 1. Crear SharedState
        let cdc_config = CdcConfig {
            flush_size: config.flush_size,
            flush_interval_ms: config.flush_interval_ms,
            tables: config.tables.clone(),
            slot_name: config.slot_name.clone(),
        };
        let shared_state = SharedState::new(cdc_config);

        // 2. Inicializar StateStore
        let state_store = StateStore::new(&config.database_url).await?;
        
        Ok(Self {
            config,
            shared_state,
            state_store,
        })
    }

    /// Ejecutar el motor CDC
    pub async fn run(self) -> Result<()> {
        // Stage: SETUP - Checkpoint
        self.shared_state.set_stage(Stage::Setup, "Loading checkpoint").await;
        let start_lsn = self.load_checkpoint().await?;

        // Stage: SETUP - gRPC Server
        self.shared_state.set_stage(Stage::Setup, "Starting gRPC server").await;
        self.start_grpc_server();

        // Stage: SETUP - Source Connection
        self.shared_state.set_stage(Stage::Setup, "Connecting to PostgreSQL").await;
        let source = self.init_source().await?;
        
        // Stage: SETUP - Replication Stream
        self.shared_state.set_stage(Stage::Setup, "Starting replication stream").await;
        let replication_stream = source.start_replication_from(start_lsn).await?;
        tokio::pin!(replication_stream);

        // Stage: SETUP - Sink Connection
        self.shared_state.set_stage(Stage::Setup, "Connecting to StarRocks").await;
        let sink = self.init_sink();

        // Stage: SETUP - Pipeline
        self.shared_state.set_stage(Stage::Setup, "Initializing pipeline").await;
        let (tx, feedback_rx) = self.init_pipeline(sink);

        // Stage: CDC - Ready to replicate
        self.shared_state.set_stage(Stage::Cdc, "Replicating").await;
        println!("Connected! Streaming CDC events...");

        // 6. Ejecutar main loop
        self.run_main_loop(
            replication_stream,
            tx,
            feedback_rx,
            start_lsn,
        ).await
    }

    /// Cargar checkpoint desde StateStore
    async fn load_checkpoint(&self) -> Result<u64> {
        let last_lsn = self.state_store.load_checkpoint(&self.config.slot_name).await?;
        let start_lsn = last_lsn.unwrap_or(0);
        
        if start_lsn > 0 {
            println!("Checkpoint: Resuming from LSN 0x{:X}", start_lsn);
        } else {
            println!("Checkpoint: Starting from beginning (no previous checkpoint)");
        }

        self.shared_state.update_lsn(start_lsn);
        self.shared_state.confirm_lsn(start_lsn);

        Ok(start_lsn)
    }

    /// Iniciar servidor gRPC en background
    fn start_grpc_server(&self) {
        let grpc_state = self.shared_state.clone();
        let grpc_port = self.config.grpc_port;
        
        tokio::spawn(async move {
            if let Err(e) = grpc::start_grpc_server(grpc_port, grpc_state).await {
                eprintln!("gRPC server error: {}", e);
            }
        });
    }

    /// Inicializar PostgreSQL source
    async fn init_source(&self) -> Result<PostgresSource> {
        let source = PostgresSource::new(
            &self.config.database_url,
            self.config.slot_name.clone(),
            self.config.publication_name.clone(),
        ).await?;
        
        // Validar REPLICA IDENTITY
        if let Err(e) = source.validate_replica_identity(&self.config.tables).await {
            eprintln!("⚠️  REPLICA IDENTITY validation failed: {}", e);
            eprintln!("    Continuing anyway, but soft deletes may not work correctly.");
        }

        Ok(source)
    }

    /// Inicializar StarRocks sink
    fn init_sink(&self) -> Box<StarRocksSink> {
        Box::new(StarRocksSink::new(
            self.config.starrocks_url.clone(),
            self.config.starrocks_db.clone(),
            self.config.starrocks_user.clone(),
            self.config.starrocks_pass.clone(),
        ))
    }

    /// Inicializar pipeline y retornar canales
    fn init_pipeline(
        &self,
        sink: Box<StarRocksSink>,
    ) -> (mpsc::Sender<crate::source::parser::CdcEvent>, mpsc::Receiver<u64>) {
        let (tx, rx) = mpsc::channel(self.config.flush_size * 2);
        let (feedback_tx, feedback_rx) = mpsc::channel::<u64>(100);
        
        let pipeline = Pipeline::new(
            rx,
            sink,
            self.config.flush_size,
            Duration::from_millis(self.config.flush_interval_ms),
        )
        .with_feedback_channel(feedback_tx)
        .with_shared_state(self.shared_state.clone());
        
        tokio::spawn(pipeline.run());

        (tx, feedback_rx)
    }

    /// Main loop de replicación
    async fn run_main_loop<S>(
        &self,
        mut replication_stream: S,
        tx: mpsc::Sender<crate::source::parser::CdcEvent>,
        mut feedback_rx: mpsc::Receiver<u64>,
        start_lsn: u64,
    ) -> Result<()>
    where
        S: StreamExt<Item = Result<bytes::Bytes, tokio_postgres::Error>>
            + SinkExt<bytes::Bytes>
            + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut current_lsn: u64 = start_lsn;
        let mut shutdown_rx = self.shared_state.shutdown_tx.subscribe();

        loop {
            // 1. Check state changes (Pause/Stop/Draining)
            if let Some(flow) = self.check_state_control(&tx).await {
                if matches!(flow, ControlFlow::Break) {
                    break;
                }
                continue;
            }

            // 2. Main select loop
            tokio::select! {
                // Shutdown signal
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        println!("Shutdown signal received");
                        break;
                    }
                }

                // Replication messages
                data_res = replication_stream.next() => {
                    match data_res {
                        Some(Ok(mut data)) => {
                            if let Some(msg) = parse_replication_message(&mut data) {
                                current_lsn = self.handle_replication_message(
                                    msg,
                                    &tx,
                                    &mut replication_stream,
                                ).await?;
                            }
                        }
                        Some(Err(e)) => {
                            eprintln!("Replication stream error: {}", e);
                            break;
                        }
                        None => {
                            eprintln!("Replication stream ended");
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

        println!("CDC shutdown complete");
        Ok(())
    }

    /// Verificar estado del CDC (Pause/Stop/Draining)
    async fn check_state_control(
        &self,
        tx: &mpsc::Sender<crate::source::parser::CdcEvent>,
    ) -> Option<ControlFlow> {
        let current_state = *self.shared_state.state.read().await;
        
        match current_state {
            CdcState::Stopped => {
                println!("CDC stopped by control plane. Exiting immediately.");
                Some(ControlFlow::Break)
            }
            CdcState::Draining => {
                // Check if channel is empty
                if tx.capacity() == self.config.flush_size * 2 {
                    println!("CDC drained. Exiting gracefully.");
                    *self.shared_state.state.write().await = CdcState::Stopped;
                    Some(ControlFlow::Break)
                } else {
                    None // Continue draining
                }
            }
            CdcState::Paused => {
                // Sleep and wait for resume
                tokio::time::sleep(Duration::from_millis(100)).await;
                Some(ControlFlow::Continue)
            }
            CdcState::Running => None, // Normal operation
        }
    }

    /// Manejar mensajes de replicación
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
                eprintln!("Unknown replication message tag: {}", tag);
                Ok(0)
            }
        }
    }

    /// Manejar confirmación de checkpoint
    async fn handle_checkpoint_feedback<S>(
        &self,
        confirmed_lsn: u64,
        replication_stream: &mut S,
    ) -> Result<()>
    where
        S: SinkExt<bytes::Bytes> + Unpin,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        // 1. Actualizar SharedState
        self.shared_state.confirm_lsn(confirmed_lsn);

        // 2. Guardar checkpoint
        if let Err(e) = self.state_store
            .save_checkpoint(&self.config.slot_name, confirmed_lsn)
            .await
        {
            eprintln!("Failed to save checkpoint: {}", e);
            return Ok(()); // No fatal
        }

        // 3. Confirmar a PostgreSQL
        let status = build_standby_status_update(confirmed_lsn);
        if let Err(e) = replication_stream.send(status).await {
            eprintln!("Failed to send status update to PostgreSQL: {}", e);
            return Ok(()); // No fatal
        }

        println!("✓ Checkpoint confirmed: LSN 0x{:X}", confirmed_lsn);
        Ok(())
    }
}

/// Control de flujo para el loop
enum ControlFlow {
    Continue,
    Break,
}

