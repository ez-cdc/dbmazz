// Copyright 2025
// Licensed under the Elastic License v2.0

#![allow(dead_code)]
//! PostgreSQL CDC Source using logical replication (pgoutput)
//!
//! This module provides a complete PostgreSQL CDC implementation that:
//! - Connects using logical replication protocol
//! - Parses pgoutput WAL messages
//! - Emits normalized CdcRecord events
//! - Tracks LSN positions for checkpointing
//!
//! # Architecture
//!
//! ```text
//! PostgreSQL WAL
//!       |
//!       v
//! [Replication Stream] --> [Parser] --> [CdcRecord] --> [Channel]
//!       ^                                                    |
//!       |                                                    v
//! [Standby Status]  <--  [Checkpoint Feedback]  <--    [Pipeline]
//! ```
//!
//! # Usage
//!
//! ```ignore
//! let (tx, rx) = mpsc::channel(10000);
//! let mut source = PostgresSource::new(
//!     "postgres://localhost/mydb",
//!     "my_slot".to_string(),
//!     "my_pub".to_string(),
//!     vec!["orders".to_string()],
//!     tx,
//! ).await?;
//!
//! source.validate().await?;
//! source.start().await?;
//! ```

mod config;
mod parser;
mod replication;
mod setup;
mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_postgres::{Client, CopyBothDuplex, NoTls};
use tracing::{error, info, warn};

use crate::core::{CdcRecord, Source, SourcePosition};

pub use parser::{CdcMessage, Column, PgOutputParser};
pub use replication::WalMessage;

/// PostgreSQL CDC Source implementing the Source trait
///
/// Connects to PostgreSQL using logical replication and streams
/// CDC events as normalized CdcRecord messages.
pub struct PostgresSource {
    /// Database connection URL (cleaned of replication params)
    database_url: String,

    /// Replication slot name
    slot_name: String,

    /// Publication name containing tables to replicate
    publication_name: String,

    /// List of tables to replicate
    tables: Vec<String>,

    /// Channel for emitting CdcRecord events
    record_tx: mpsc::Sender<CdcRecord>,

    /// Current LSN position (atomic for lock-free reads)
    current_lsn: Arc<AtomicU64>,

    /// Confirmed/checkpointed LSN
    confirmed_lsn: Arc<AtomicU64>,

    /// Schema cache: relation_id -> (schema, table, columns)
    schema_cache: Arc<RwLock<HashMap<u32, RelationInfo>>>,

    /// Running state flag
    is_running: Arc<AtomicBool>,

    /// Replication client (initialized on start)
    replication_client: Option<Client>,
}

/// Cached relation (table) information from Relation messages
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
}

impl PostgresSource {
    /// Create a new PostgresSource
    ///
    /// This establishes the initial connection but does not start replication.
    /// Call `validate()` to verify configuration and `start()` to begin streaming.
    pub async fn new(
        database_url: &str,
        slot_name: String,
        publication_name: String,
        tables: Vec<String>,
        record_tx: mpsc::Sender<CdcRecord>,
    ) -> Result<Self> {
        // Clean URL of replication parameters
        let clean_url = Self::clean_url(database_url);

        Ok(Self {
            database_url: clean_url,
            slot_name,
            publication_name,
            tables,
            record_tx,
            current_lsn: Arc::new(AtomicU64::new(0)),
            confirmed_lsn: Arc::new(AtomicU64::new(0)),
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),
            replication_client: None,
        })
    }

    /// Clean database URL of replication parameters
    fn clean_url(url: &str) -> String {
        url.replace("?replication=database", "")
            .replace("&replication=database", "")
            .replace("replication=database&", "")
    }

    /// Get the current LSN position
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::Relaxed)
    }

    /// Get the confirmed LSN position
    pub fn confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn.load(Ordering::Relaxed)
    }

    /// Update the current LSN
    pub fn update_lsn(&self, lsn: u64) {
        self.current_lsn.store(lsn, Ordering::Relaxed);
    }

    /// Confirm an LSN (called after checkpoint)
    pub fn confirm_lsn(&self, lsn: u64) {
        self.confirmed_lsn.store(lsn, Ordering::Relaxed);
    }

    /// Check if the source is running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    /// Start replication from a specific LSN
    ///
    /// Returns the replication stream for the caller to process
    pub async fn start_replication_from(&self, start_lsn: u64) -> Result<CopyBothDuplex<Bytes>> {
        // Create replication connection
        let mut config: tokio_postgres::Config = self.database_url.parse()?;
        config.replication_mode(tokio_postgres::config::ReplicationMode::Logical);

        let (client, connection) = config
            .connect(NoTls)
            .await
            .context("Failed to create replication connection")?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Replication connection error: {}", e);
            }
        });

        // Convert LSN to PostgreSQL format (X/Y)
        let lsn_str = if start_lsn == 0 {
            "0/0".to_string()
        } else {
            format!("{:X}/{:X}", start_lsn >> 32, start_lsn & 0xFFFFFFFF)
        };

        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            self.slot_name, lsn_str, self.publication_name
        );

        info!("Starting replication from LSN: {}", lsn_str);

        let stream = client
            .copy_both_simple(&query)
            .await
            .context("Failed to start replication")?;

        Ok(stream)
    }

    /// Process a WAL message and emit CdcRecords
    ///
    /// This is the main message processing logic that converts pgoutput
    /// messages into normalized CdcRecord events.
    pub async fn process_wal_message(&self, msg: WalMessage) -> Result<Option<u64>> {
        match msg {
            WalMessage::XLogData { lsn, data } => {
                self.update_lsn(lsn);

                if data.is_empty() {
                    return Ok(Some(lsn));
                }

                let pgoutput_tag = data[0];
                let pgoutput_body = data.slice(1..);

                if let Some(cdc_msg) = PgOutputParser::parse(pgoutput_tag, pgoutput_body)? {
                    self.handle_cdc_message(cdc_msg, lsn).await?;
                }

                Ok(Some(lsn))
            }
            WalMessage::KeepAlive {
                lsn,
                reply_requested: _,
            } => {
                // KeepAlive handling is done by the caller
                Ok(Some(lsn))
            }
            WalMessage::Unknown(tag) => {
                warn!("Unknown WAL message tag: {}", tag);
                Ok(None)
            }
        }
    }

    /// Handle a parsed CDC message and emit CdcRecord
    async fn handle_cdc_message(&self, msg: CdcMessage, lsn: u64) -> Result<()> {
        let position = SourcePosition::Lsn(lsn);

        match msg {
            CdcMessage::Relation {
                id,
                namespace,
                name,
                columns,
                ..
            } => {
                // Cache the relation info for later use
                let info = RelationInfo {
                    schema: namespace.clone(),
                    name: name.clone(),
                    columns: columns.clone(),
                };
                self.schema_cache.write().insert(id, info.clone());

                // Emit schema change record
                let column_defs = types::columns_to_defs(&columns);
                let record = CdcRecord::SchemaChange {
                    table: crate::core::TableRef::new(Some(namespace), name),
                    columns: column_defs,
                    position,
                };
                self.emit_record(record).await?;
            }

            CdcMessage::Begin {
                xid, ..
            } => {
                let record = CdcRecord::Begin { xid: xid as u64 };
                self.emit_record(record).await?;
            }

            CdcMessage::Commit {
                end_lsn, ..
            } => {
                let record = CdcRecord::Commit {
                    xid: 0, // XID not available in Commit message
                    position: SourcePosition::Lsn(end_lsn),
                };
                self.emit_record(record).await?;
            }

            CdcMessage::Insert { relation_id, tuple } => {
                if let Some(info) = self.schema_cache.read().get(&relation_id).cloned() {
                    let columns = types::tuple_to_column_values(&tuple, &info.columns);
                    let record = CdcRecord::Insert {
                        table: crate::core::TableRef::new(Some(info.schema), info.name),
                        columns,
                        position,
                    };
                    self.emit_record(record).await?;
                }
            }

            CdcMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
            } => {
                if let Some(info) = self.schema_cache.read().get(&relation_id).cloned() {
                    let old_columns = old_tuple.map(|t| types::tuple_to_column_values(&t, &info.columns));
                    let new_columns = types::tuple_to_column_values(&new_tuple, &info.columns);

                    let record = CdcRecord::Update {
                        table: crate::core::TableRef::new(Some(info.schema), info.name),
                        old_columns,
                        new_columns,
                        position,
                    };
                    self.emit_record(record).await?;
                }
            }

            CdcMessage::Delete {
                relation_id,
                old_tuple,
            } => {
                if let Some(info) = self.schema_cache.read().get(&relation_id).cloned() {
                    if let Some(tuple) = old_tuple {
                        let columns = types::tuple_to_column_values(&tuple, &info.columns);
                        let record = CdcRecord::Delete {
                            table: crate::core::TableRef::new(Some(info.schema), info.name),
                            columns,
                            position,
                        };
                        self.emit_record(record).await?;
                    }
                }
            }

            CdcMessage::KeepAlive { wal_end, .. } => {
                let record = CdcRecord::Heartbeat {
                    position: SourcePosition::Lsn(wal_end),
                };
                self.emit_record(record).await?;
            }

            CdcMessage::Unknown => {
                // Ignore unknown messages
            }
        }

        Ok(())
    }

    /// Emit a CdcRecord to the channel
    async fn emit_record(&self, record: CdcRecord) -> Result<()> {
        self.record_tx
            .send(record)
            .await
            .context("Failed to send CdcRecord to channel")?;
        Ok(())
    }

    /// Validates that tables have REPLICA IDENTITY FULL
    pub async fn validate_replica_identity(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Validation connection error: {}", e);
            }
        });

        for table in &self.tables {
            let parts: Vec<&str> = table.split('.').collect();
            let table_name = if parts.len() > 1 { parts[1] } else { parts[0] };
            let schema_name = if parts.len() > 1 {
                parts[0]
            } else {
                "public"
            };

            let row = client
                .query_one(
                    "SELECT c.relreplident, c.relname
                     FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
                     WHERE c.relname = $1 AND n.nspname = $2",
                    &[&table_name, &schema_name],
                )
                .await
                .with_context(|| {
                    format!("Failed to query replica identity for table '{}'", table)
                })?;

            let replica_identity: i8 = row.get(0);
            let relname: String = row.get(1);
            let replica_char = replica_identity as u8 as char;

            match replica_char {
                'f' => {
                    info!("Table '{}' has REPLICA IDENTITY FULL", relname);
                }
                'd' => {
                    warn!("Table '{}' has REPLICA IDENTITY DEFAULT", relname);
                    warn!("    This may cause issues with soft deletes.");
                    warn!("    Run: ALTER TABLE {} REPLICA IDENTITY FULL;", table);
                }
                'n' => {
                    anyhow::bail!(
                        "Table '{}' has REPLICA IDENTITY NOTHING. \
                        This is not supported for CDC. \
                        Run: ALTER TABLE {} REPLICA IDENTITY FULL;",
                        relname,
                        table
                    );
                }
                'i' => {
                    info!("Table '{}' has REPLICA IDENTITY INDEX", relname);
                    error!(
                        "    Note: For full soft delete support, consider REPLICA IDENTITY FULL"
                    );
                }
                _ => {
                    error!(
                        "Unknown REPLICA IDENTITY '{}' for table '{}'",
                        replica_char, relname
                    );
                }
            }
        }

        Ok(())
    }

    /// Get the slot name
    pub fn slot_name(&self) -> &str {
        &self.slot_name
    }

    /// Get the publication name
    pub fn publication_name(&self) -> &str {
        &self.publication_name
    }

    /// Get the database URL
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Get the tables being replicated
    pub fn tables(&self) -> &[String] {
        &self.tables
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn validate(&self) -> Result<()> {
        // Validate connection
        let (client, connection) = tokio_postgres::connect(&self.database_url, NoTls)
            .await
            .context("Failed to connect to PostgreSQL")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Validation connection error: {}", e);
            }
        });

        // Check version
        let row = client.query_one("SELECT version()", &[]).await?;
        let version: String = row.get(0);
        info!("PostgreSQL version: {}", version);

        // Validate replica identity
        self.validate_replica_identity().await?;

        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        self.is_running.store(true, Ordering::Relaxed);
        info!("PostgreSQL source started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        self.is_running.store(false, Ordering::Relaxed);
        info!("PostgreSQL source stopped");
        Ok(())
    }

    fn current_position(&self) -> Option<SourcePosition> {
        let lsn = self.current_lsn.load(Ordering::Relaxed);
        if lsn > 0 {
            Some(SourcePosition::Lsn(lsn))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_url() {
        let url1 = "postgres://localhost/db?replication=database";
        assert_eq!(
            PostgresSource::clean_url(url1),
            "postgres://localhost/db"
        );

        let url2 = "postgres://localhost/db?param=1&replication=database";
        assert_eq!(
            PostgresSource::clean_url(url2),
            "postgres://localhost/db?param=1"
        );

        let url3 = "postgres://localhost/db";
        assert_eq!(PostgresSource::clean_url(url3), "postgres://localhost/db");
    }

    #[tokio::test]
    async fn test_lsn_tracking() {
        let (tx, _rx) = mpsc::channel(100);
        let source = PostgresSource::new(
            "postgres://localhost/test",
            "test_slot".to_string(),
            "test_pub".to_string(),
            vec!["test".to_string()],
            tx,
        )
        .await
        .unwrap();

        assert_eq!(source.current_lsn(), 0);
        source.update_lsn(12345);
        assert_eq!(source.current_lsn(), 12345);

        assert_eq!(source.confirmed_lsn(), 0);
        source.confirm_lsn(10000);
        assert_eq!(source.confirmed_lsn(), 10000);
    }
}
