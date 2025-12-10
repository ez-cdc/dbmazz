pub mod starrocks;

use async_trait::async_trait;
use anyhow::Result;
use crate::source::parser::CdcMessage;
use crate::pipeline::schema_cache::SchemaCache;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn push_batch(
        &mut self, 
        batch: &[CdcMessage],
        schema_cache: &SchemaCache
    ) -> Result<()>;
}

