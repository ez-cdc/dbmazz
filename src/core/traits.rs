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
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SinkResult {
    pub records_written: usize,
    pub bytes_written: u64,
    pub last_position: Option<SourcePosition>,
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

/// A single column in a source table schema
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SourceColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Original PostgreSQL type OID (useful for PG-to-PG sinks)
    pub pg_type_id: u32,
}

#[async_trait]
#[allow(dead_code)]
pub trait Source: Send + Sync {
    /// Returns the name of the source implementation
    fn name(&self) -> &'static str;

    /// Validates the source configuration and connection
    async fn validate(&self) -> Result<()>;

    /// Starts the source and begins processing CDC events
    async fn start(&mut self) -> Result<()>;

    /// Stops the source gracefully
    async fn stop(&mut self) -> Result<()>;

    /// Returns the current source position for checkpointing
    fn current_position(&self) -> Option<SourcePosition>;
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
