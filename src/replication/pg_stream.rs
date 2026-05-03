// Copyright 2025
// Licensed under the Elastic License v2.0

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::pin::Pin;
use tokio_postgres::CopyBothDuplex;

use crate::core::ReplicationStream;

/// Wraps a PostgreSQL `CopyBothDuplex<Bytes>` stream as a generic `ReplicationStream`.
///
/// This is the bridge between the tokio-postgres replication protocol and the
/// source-agnostic `ReplicationStream` trait used by the engine.
///
/// `CopyBothDuplex` uses `pin_project!` internally and is `!Unpin`, so we store
/// it in a `Pin<Box<...>>` to satisfy the `Unpin` requirement of `ReplicationStream`.
pub struct PgReplicationStream {
    inner: Pin<Box<CopyBothDuplex<Bytes>>>,
}

impl PgReplicationStream {
    pub fn new(inner: CopyBothDuplex<Bytes>) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }
}

#[async_trait]
impl ReplicationStream for PgReplicationStream {
    async fn next_event(&mut self) -> Option<Result<Vec<u8>>> {
        match self.inner.as_mut().next().await {
            Some(Ok(bytes)) => Some(Ok(bytes.to_vec())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    async fn send_feedback(&mut self, data: &[u8]) -> Result<()> {
        self.inner
            .as_mut()
            .send(Bytes::copy_from_slice(data))
            .await?;
        Ok(())
    }
}
