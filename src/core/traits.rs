use crate::config::SourceType;
use crate::core::position::SourcePosition;
use crate::core::record::{CdcRecord, DataType};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Mode in which a sink instance operates.
/// Primary sinks run background tasks (normalizer, etc.).
/// SnapshotWorker sinks skip background tasks — the primary handles them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
    Primary,
    SnapshotWorker,
}

/// Sink capabilities for feature detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkCapabilities {
    pub supports_upsert: bool,
    pub supports_delete: bool,
    pub supports_schema_evolution: bool,
    pub supports_transactions: bool,
    pub loading_model: LoadingModel,
    pub min_batch_size: Option<usize>,
    pub max_batch_size: Option<usize>,
    pub optimal_flush_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoadingModel {
    Streaming,
    StagedBatch { stage_format: StageFormat },
    MessageQueue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StageFormat {
    Parquet,
    Csv,
    Json,
    Avro,
}

/// Result returned from sink write operations
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct SinkResult {
    pub records_written: usize,
    pub bytes_written: u64,
    pub last_position: Option<SourcePosition>,
    /// Number of `CdcRecord::SchemaChange` events that the sink could not
    /// auto-apply during this batch (e.g. StarRocks degraded mode where
    /// the target table lacks `fast_schema_evolution=true`). The pipeline
    /// adds this to the global `schema_evolution_skipped_total` counter
    /// surfaced in `/api/v1/cdc/metrics`.
    pub schema_evolution_skipped: u64,
}

/// Schema of a source table — provided to sinks during setup so they can
/// create destination tables, raw tables, stages, etc.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SourceTableSchema {
    pub schema: String,
    pub name: String,
    pub columns: Vec<SourceColumn>,
    pub primary_keys: Vec<String>,
}

/// A single column in a source table schema.
///
/// `data_type` is the authoritative, source-agnostic type info that sinks
/// dispatch on for DDL generation, MERGE projection casts, and runtime
/// value conversion. `pg_type_id` is an OPTIONAL refinement carried only
/// by Postgres sources — sinks consult it as a fallback when `DataType`
/// is too coarse to disambiguate (e.g., `varchar(N)` vs `text` both
/// collapse to `DataType::String`). Non-PG sources (MySQL, and any
/// future source) populate `None`.
///
/// **Sinks MUST NOT use the presence/absence of `pg_type_id` as a
/// source-type discriminator.** The intent is purely refinement, not
/// branching.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SourceColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub pg_type_id: Option<u32>,
}

/// Generic replication stream — replaces PG-specific CopyBothDuplex<Bytes>
#[async_trait]
#[allow(dead_code)]
pub trait ReplicationStream: Send + Unpin + 'static {
    /// Read the next raw event from the replication stream.
    /// Returns None on clean end-of-stream.
    async fn next_event(&mut self) -> Option<Result<Vec<u8>>>;

    /// Send a feedback/keepalive message back to the source.
    /// Default implementation is a no-op (MySQL doesn't need this).
    async fn send_feedback(&mut self, _data: &[u8]) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
#[allow(dead_code)]
pub trait Source: Send + Sync {
    /// Returns the name of the source implementation
    fn name(&self) -> &'static str;

    /// Returns the source type for dispatch
    fn source_type(&self) -> SourceType;

    /// Validate prerequisites (connectivity, GTID, binlog format)
    async fn validate(&self) -> Result<()>;

    /// Setup source-side resources (slots for PG, GTID verification for MySQL)
    async fn setup(&mut self, tables: &[String]) -> Result<()>;

    /// Start replication stream from given position
    async fn start_replication(
        &mut self,
        position: Option<SourcePosition>,
    ) -> Result<Box<dyn ReplicationStream>>;

    /// Get current replication position for checkpointing
    fn checkpoint_position(&self) -> Option<SourcePosition>;

    /// Cleanup source-side resources
    async fn cleanup(&mut self) -> Result<()>;

    /// Create the appropriate replication loop for this source type.
    /// This replaces the need for downcasting. Each implementation
    /// creates the loop variant it needs (PgReplicationLoop or MysqlReplicationLoop).
    async fn create_loop(
        &mut self,
        position: Option<SourcePosition>,
    ) -> Result<Box<dyn crate::engine::replication::ReplicationLoop>>;
}

#[async_trait]
#[allow(dead_code)]
pub trait Sink: Send + Sync {
    /// Returns the name of the sink implementation
    fn name(&self) -> &'static str;

    /// Returns the capabilities of this sink
    fn capabilities(&self) -> SinkCapabilities;

    /// Validates the sink connection and configuration
    async fn validate_connection(&self) -> Result<()>;

    /// Prepare the destination: create target tables, raw tables, stages, etc.
    /// Called once at startup before CDC and snapshot begin.
    /// Receives source table schemas so the sink can generate appropriate DDL.
    async fn setup(&mut self, _source_schemas: &[SourceTableSchema]) -> Result<()> {
        Ok(())
    }

    /// Writes a batch of CDC records to the sink.
    /// Each sink decides how to apply them (Stream Load, raw table + MERGE, stage, etc).
    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult>;

    /// Close the sink: flush pending data and clean up resources.
    async fn close(&mut self) -> Result<()>;
}
