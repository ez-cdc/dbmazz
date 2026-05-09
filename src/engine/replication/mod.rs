//! Replication loop abstraction — each source type implements its own event loop.

pub mod postgres;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::Config;
use crate::control::state::{CdcState, SharedState};
use crate::control::Stage;
use crate::core::traits::SourceTableSchema;
use crate::core::Sink;
use crate::engine::ControlFlow;
use crate::pipeline::PipelineEvent;
use crate::state_store::StateStore;

#[cfg(feature = "mysql-source")]
mod mysql;
#[cfg(feature = "mysql-source")]
pub use mysql::MysqlReplicationLoop;

/// Factory that creates fresh Sink instances (used by snapshot workers).
type SinkFactory = Arc<dyn Fn() -> anyhow::Result<Box<dyn Sink>> + Send + Sync>;

/// Shared context passed to every ReplicationLoop implementation.
pub struct LoopContext {
    pub shared_state: Arc<SharedState>,
    pub config: Arc<Config>,
    pub state_store: Arc<StateStore>,
    pub pipeline_tx: mpsc::Sender<PipelineEvent>,
    pub feedback_rx: mpsc::Receiver<u64>,
    pub source_schemas: Arc<[SourceTableSchema]>,
    pub sink_factory: SinkFactory,
    /// MySQL-only: registry of in-flight snapshot chunks for the read-only
    /// DBLog reconciliation. The consumer marks evicted PKs and signals
    /// chunk drain when its `current_gtid_set` covers the chunk's HIGH
    /// watermark. Always present (default-constructed for non-MySQL
    /// pipelines, where it stays empty).
    #[cfg(feature = "mysql-source")]
    pub active_chunks: crate::engine::snapshot::active_chunks::ActiveChunks,
    /// MySQL-only: live, shared view of the consumer's accumulated
    /// `gtid_executed`. The MySQL replication loop is the single writer
    /// (via `ParserState::current_gtid_set`); snapshot workers read it
    /// when registering chunks (LOW watermark) and again after the
    /// chunk SELECT (HIGH watermark) — replacing per-chunk
    /// `SELECT @@global.gtid_executed` queries against the source.
    /// Default-constructed for non-MySQL pipelines.
    #[cfg(feature = "mysql-source")]
    pub consumer_gtid: std::sync::Arc<std::sync::RwLock<crate::source::mysql::gtid::GtidSet>>,
}

/// Each source type implements its own replication event loop.
/// The engine picks the right implementation at startup.
#[async_trait]
pub trait ReplicationLoop: Send {
    async fn run(self: Box<Self>, ctx: LoopContext) -> Result<()>;
}

/// Shared loop utilities (state check, shutdown, snapshot trigger).
pub struct LoopHelper;

impl LoopHelper {
    /// Check CDC state (Pause/Stop/Draining). Returns control flow signal.
    pub fn check_state_control(
        state: &CdcState,
        pipeline_capacity: usize,
        flush_size: usize,
    ) -> Option<ControlFlow> {
        match state {
            CdcState::Stopped => {
                info!("CDC stopped. Exiting immediately.");
                Some(ControlFlow::Break)
            }
            CdcState::Draining => {
                if pipeline_capacity == flush_size * 2 {
                    info!("CDC drained. Exiting gracefully.");
                    Some(ControlFlow::Break)
                } else {
                    None
                }
            }
            CdcState::Paused => Some(ControlFlow::Continue),
            CdcState::Running => None,
        }
    }

    /// Set up the CDC stage in shared state.
    pub async fn set_cdc_stage(shared_state: &SharedState) {
        shared_state.set_stage(Stage::Cdc, "Replicating").await;
        info!("Connected! Streaming CDC events...");
    }
}

pub use postgres::PgReplicationLoop;
