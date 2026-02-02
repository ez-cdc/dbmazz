use crate::core::record::CdcRecord;
use crate::core::position::SourcePosition;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
pub struct SinkResult {
    pub records_written: usize,
    pub bytes_written: u64,
    pub last_position: Option<SourcePosition>,
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
pub trait Sink: Send + Sync {
    /// Returns the name of the sink implementation
    fn name(&self) -> &'static str;

    /// Returns the capabilities of this sink
    fn capabilities(&self) -> SinkCapabilities;

    /// Validates the sink connection and configuration
    async fn validate_connection(&self) -> Result<()>;

    /// Writes a batch of CDC records to the sink
    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult>;

    /// Closes the sink and flushes any remaining data
    async fn close(&mut self) -> Result<()>;
}
