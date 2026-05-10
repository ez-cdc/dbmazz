use std::str::FromStr;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use mysql_common::packets::Sid;
use mysql_common::proto::MySerialize;
use tracing::{info, warn};

use crate::core::ReplicationStream;

pub struct MysqlBinlogStream {
    inner: mysql_async::BinlogStream,
}

impl MysqlBinlogStream {
    /// Open a binlog stream against `conn` using `server_id`.
    ///
    /// `initial_gtid_set`, when `Some` and non-empty, requests the server
    /// to start streaming at the position immediately after the supplied
    /// `Gtid_set` — restoring restart correctness from the checkpoint.
    /// When `None` or empty, the stream starts from the earliest available
    /// binlog (the original behaviour).
    pub async fn new(
        conn: mysql_async::Conn,
        server_id: u32,
        initial_gtid_set: Option<&str>,
    ) -> Result<Self> {
        let mut request = mysql_async::BinlogStreamRequest::new(server_id).with_gtid();

        let owned_sids: Vec<Sid<'static>> = match initial_gtid_set {
            Some(set) if !set.trim().is_empty() => {
                let mut parsed = Vec::new();
                for part in set.split(',') {
                    let part = part.trim();
                    if part.is_empty() {
                        continue;
                    }
                    match Sid::from_str(part) {
                        Ok(sid) => parsed.push(sid),
                        Err(e) => {
                            warn!(
                                "Failed to parse Gtid_set fragment '{}': {} — \
                                 ignoring this fragment for resume",
                                part, e
                            );
                        }
                    }
                }
                parsed
            }
            _ => Vec::new(),
        };
        if !owned_sids.is_empty() {
            request = request.with_gtid_set(owned_sids.clone());
            info!(
                "MySQL binlog stream resuming with {} GTID source(s)",
                owned_sids.len()
            );
        }

        let stream: mysql_async::BinlogStream = conn
            .get_binlog_stream(request)
            .await
            .context("Failed to start MySQL binlog stream")?;
        info!("MySQL binlog stream started (server_id: {})", server_id);
        Ok(Self { inner: stream })
    }

    /// Returns the next typed binlog event as parsed by mysql_async's EventStreamReader.
    pub async fn next_event_typed(
        &mut self,
    ) -> Option<Result<mysql_common::binlog::events::Event>> {
        let result = self.inner.next().await?;
        Some(match result {
            Ok(event) => Ok(event),
            Err(e) => Err(anyhow::anyhow!("Binlog stream error: {}", e)),
        })
    }

    /// Resolve schema + table name for a table_id from the internal TME cache.
    pub fn get_table_name(&self, table_id: u64) -> Option<(String, String)> {
        self.inner.get_tme(table_id).map(|tme| {
            let schema = tme.database_name().to_string();
            let table = tme.table_name().to_string();
            (schema, table)
        })
    }

    /// Consume self and return the inner BinlogStream (for direct typed access).
    pub fn into_inner(self) -> mysql_async::BinlogStream {
        self.inner
    }

    /// Get raw column type codes for a table_id from the internal TME cache.
    pub fn get_column_types(&self, table_id: u64) -> Option<Vec<u8>> {
        self.inner.get_tme(table_id).map(|tme| {
            (0..tme.columns_count() as usize)
                .filter_map(|i| tme.get_raw_column_type(i).ok().flatten())
                .map(|ct| ct as u8)
                .collect()
        })
    }
}

#[async_trait]
impl ReplicationStream for MysqlBinlogStream {
    async fn next_event(&mut self) -> Option<Result<Vec<u8>>> {
        let result = self.inner.next().await?;
        Some(match result {
            Ok(event) => {
                let header = event.header();
                let mut buf = Vec::new();
                header.serialize(&mut buf);
                buf.extend_from_slice(event.data());
                Ok(buf)
            }
            Err(e) => Err(anyhow::anyhow!("Binlog stream error: {}", e)),
        })
    }

    async fn send_feedback(&mut self, _data: &[u8]) -> Result<()> {
        Ok(())
    }
}
