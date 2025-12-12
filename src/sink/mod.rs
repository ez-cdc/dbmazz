pub mod curl_loader;
pub mod starrocks;

use async_trait::async_trait;
use anyhow::Result;
use crate::source::parser::CdcMessage;
use crate::pipeline::schema_cache::{SchemaCache, SchemaDelta};

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

