pub mod adapter;

use crate::pipeline::schema_cache::{SchemaCache, SchemaDelta};
use crate::source::parser::CdcMessage;
use anyhow::Result;
use async_trait::async_trait;

/// Legacy Sink trait for backward compatibility
///
/// This trait uses PostgreSQL-specific `CdcMessage` types. New connectors
/// should implement `core::Sink` instead and use `adapter::NewSinkAdapter`
/// to bridge with the legacy pipeline.
#[async_trait]
pub trait Sink: Send + Sync {
    async fn push_batch(
        &mut self,
        batch: &[CdcMessage],
        schema_cache: &SchemaCache,
        lsn: u64,
    ) -> Result<()>;

    async fn apply_schema_delta(&self, delta: &SchemaDelta) -> Result<()>;
}

pub use adapter::NewSinkAdapter;
