pub mod adapter;
pub mod curl_loader;
pub mod starrocks;

use async_trait::async_trait;
use anyhow::Result;
use crate::source::parser::CdcMessage;
use crate::pipeline::schema_cache::{SchemaCache, SchemaDelta};

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
        lsn: u64
    ) -> Result<()>;

    async fn apply_schema_delta(&self, delta: &SchemaDelta) -> Result<()>;
}

pub use adapter::NewSinkAdapter;

