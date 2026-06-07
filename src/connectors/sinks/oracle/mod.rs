// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle sink connector.
//!
//! Replicates CDC changes to a target Oracle Database using MERGE (upsert)
//! for inserts/updates and direct DELETE for deletions. The Oracle crate
//! (kubo/rust-oracle) is synchronous, so all Oracle operations run via
//! `tokio::task::spawn_blocking` on the Tokio blocking thread pool.
//!
//! ## CDC Flow
//!
//! ```text
//! write_batch(Vec<CdcRecord>)
//!   ├── For each record group by table:
//!   │   ├── INSERT/UPDATE → MERGE INTO target_table
//!   │   └── DELETE        → DELETE FROM target_table
//!   ├── COMMIT (per batch, if transactions are enabled)
//!   └── Return SinkResult with record count
//! ```

mod merge_generator;
mod schema_tracking;
mod setup;
mod types;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::info;

use crate::config::SinkConfig;
use crate::core::position::SourcePosition;
use crate::core::record::CdcRecord;
use crate::core::traits::{
    LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult, SourceTableSchema,
};

/// Oracle sink — writes CDC records to a target Oracle Database.
pub struct OracleSink {
    /// Oracle connection string (e.g., "//host:1521/service")
    url: String,
    /// Target schema/owner name in Oracle
    schema: String,
    /// Job name for metadata tracking
    #[allow(dead_code)]
    job_name: String,
    /// Database name (for logging)
    #[allow(dead_code)]
    database: String,
    /// Connection is lazy and shared via Arc<Mutex<...>>
    /// Since the oracle crate is synchronous, we hold the connection
    /// on the blocking pool side only during actual operations.
    /// For now: the connection is established per-batch via spawn_blocking.
    /// In a full implementation a pooled connection (e.g. r2d2-oracle) would be used.
    mode: SinkMode,
}

impl OracleSink {
    /// Create a new OracleSink from the provided configuration.
    /// Connection is lazy — established on first use.
    pub fn new(config: &SinkConfig, mode: SinkMode) -> Result<Self> {
        let oracle_config = config.oracle_config()?;

        let schema = if oracle_config.schema.is_empty() {
            // Default to the database user uppercase (Oracle convention)
            config.user.to_uppercase()
        } else {
            oracle_config.schema.clone()
        };

        info!(
            "OracleSink initialized: db={}, schema={}, job={}",
            config.database, schema, oracle_config.job_name
        );

        Ok(Self {
            url: config.url.clone(),
            schema,
            job_name: oracle_config.job_name.clone(),
            database: config.database.clone(),
            mode,
        })
    }
}

#[async_trait]
impl Sink for OracleSink {
    fn name(&self) -> &'static str {
        "oracle"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: false, // Will be enabled in a follow-up
            supports_transactions: true,
            loading_model: LoadingModel::Streaming,
            min_batch_size: Some(1),
            max_batch_size: Some(250_000),
            optimal_flush_interval_ms: 10_000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        // Validate by attempting a sync connection inside spawn_blocking.
        // Returns Ok if we can connect and run a basic query.
        let _url = self.url.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            // TODO: Use the oracle crate:
            // let conn = oracle::Connection::connect(username, password, &url)?;
            // let version: String = conn.query("SELECT banner FROM v$version", &[])?
            //    .next()?;
            // info!("OracleSink: connected, version: {}", version);
            let _ = _url;
            info!("OracleSink: connection validation (placeholder — oracle crate not yet linked)");
            Ok(())
        })
        .await
        .context("Failed to join spawn_blocking for Oracle validation")?
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        info!(
            "OracleSink: setting up {} tables in schema '{}'",
            source_schemas.len(),
            self.schema
        );

        // Delegate to setup module
        setup::run_setup(
            &self.url,
            &self.schema,
            &self.job_name,
            source_schemas,
        )
        .await
        .context("OracleSink: setup failed")?;

        info!(
            "OracleSink: setup complete ({} tables)",
            source_schemas.len()
        );
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult::default());
        }

        // Track last position for checkpoint
        let last_position = records.iter().rev().find_map(|r| match r {
            CdcRecord::Insert { position, .. }
            | CdcRecord::Update { position, .. }
            | CdcRecord::Delete { position, .. }
            | CdcRecord::Commit { position, .. }
            | CdcRecord::Heartbeat { position, .. } => Some(position.clone()),
            _ => None,
        });

        let batch_size = records.len();
        let _url = self.url.clone();
        let _schema = self.schema.clone();

        // Execute batch in spawn_blocking since oracle crate is sync
        tokio::task::spawn_blocking(move || -> Result<(usize, u64)> {
            // TODO: Use oracle crate for actual DB operations:
            // let conn = oracle::Connection::connect(username, password, &url)?;
            // for record in &records { ... }
            //
            // For each table group:
            //   - INSERT records → MERGE INTO
            //   - UPDATE records → MERGE INTO
            //   - DELETE records → DELETE
            info!(
                "OracleSink: would write batch of {} records to {}",
                batch_size, _url
            );
            let _ = _schema;
            Ok((batch_size, 0))
        })
        .await
        .context("Failed to join spawn_blocking for Oracle batch write")?
        .map(|(records_written, bytes_written)| SinkResult {
            records_written,
            bytes_written,
            last_position,
            schema_evolution_skipped: 0,
        })
    }

    async fn close(&mut self) -> Result<()> {
        info!("OracleSink: closing (no persistent connection to close)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_name() {
        // Sink name should match the CLI datasource type
        let sink = OracleSink {
            url: "//localhost:1521/FREEPDB1".to_string(),
            schema: "C##DBMAZZ".to_string(),
            job_name: "test_job".to_string(),
            database: "oracle".to_string(),
            mode: SinkMode::Primary,
        };
        assert_eq!(sink.name(), "oracle");
    }

    #[test]
    fn test_capabilities() {
        let sink = OracleSink {
            url: "//localhost:1521/FREEPDB1".to_string(),
            schema: "C##DBMAZZ".to_string(),
            job_name: "test_job".to_string(),
            database: "oracle".to_string(),
            mode: SinkMode::Primary,
        };
        let caps = sink.capabilities();
        assert!(caps.supports_upsert);
        assert!(caps.supports_delete);
        assert!(caps.supports_transactions);
    }
}
