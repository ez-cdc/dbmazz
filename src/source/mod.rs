// Copyright 2025
// Licensed under the Elastic License v2.0

//! # Source Connectors
//!
//! Source connector implementations that read change events from upstream
//! databases. Each source implements the `Source` trait from
//! [`crate::core::traits`].
//!
//! ## Available Sources
//!
//! - **PostgreSQL**: Logical replication via the `pgoutput` plugin
//!   (replication slot + publication).
//! - **MySQL** (BETA, behind the `mysql-source` cargo feature): Binlog
//!   streaming with GTID-aware checkpoints + the read-only DBLog
//!   incremental snapshot algorithm. See `docs/mysql-source.md` for
//!   prerequisites, configuration, and the BETA scope.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::source::create_source;
//!
//! let mut source = create_source(&config.source).await?;
//! source.setup(&tables).await?;
//! ```
//!
//! Mirrors the [`crate::connectors::sinks::create_sink`] factory shape so
//! the engine constructs sources via a single dispatch point regardless
//! of the configured `SourceType`.

pub mod converter;
pub mod parser;
pub mod postgres;

#[cfg(feature = "mysql-source")]
pub mod mysql;

use anyhow::Result;

use crate::config::{SourceConfig, SourceType};
use crate::core::Source;

#[cfg(feature = "mysql-source")]
use self::mysql::MysqlSource;
use self::postgres::PostgresSource;

/// Creates a source connector based on the provided configuration.
///
/// Factory function that instantiates the appropriate `Source`
/// implementation based on the `source_type` field. Mirrors the
/// `create_sink` factory in `src/connectors/sinks/mod.rs` so the engine
/// has a single dispatch point per side of the pipeline.
///
/// # Arguments
///
/// * `config` — the source configuration (URL, tables, source-type-
///   specific block such as `PostgresSourceConfig` or
///   `MysqlSourceConfig`).
///
/// # Errors
///
/// - Returns an error if the configured `SourceType` is not enabled in
///   this build (e.g., `Mysql` without the `mysql-source` cargo feature).
/// - Propagates any connection or validation error from the underlying
///   source constructor.
pub async fn create_source(config: &SourceConfig) -> Result<Box<dyn Source>> {
    match config.source_type {
        SourceType::Postgres => {
            let pg = config.postgres();
            let source = PostgresSource::new(
                &config.url,
                pg.slot_name.clone(),
                pg.publication_name.clone(),
            )
            .await?;
            Ok(Box::new(source))
        }
        #[cfg(feature = "mysql-source")]
        SourceType::Mysql => {
            let mysql_cfg = config.mysql();
            let source = MysqlSource::new(&config.url, mysql_cfg).await?;
            Ok(Box::new(source))
        }
        #[cfg(not(feature = "mysql-source"))]
        SourceType::Mysql => {
            anyhow::bail!(
                "MySQL source support is not enabled in this build. \
                 Rebuild with --features mysql-source."
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PostgresSourceConfig, SourceConfig};

    /// Verifies the factory dispatches to the Postgres branch and surfaces
    /// connection errors (we don't have a live PG in unit tests, so the
    /// constructor will fail on connect — what matters is the dispatch
    /// reaches the Postgres branch, not that it succeeds end-to-end).
    #[tokio::test]
    async fn create_source_dispatches_postgres() {
        let config = SourceConfig {
            source_type: SourceType::Postgres,
            url: "postgres://invalid-host-for-unit-test:5432/none".to_string(),
            tables: vec!["t".to_string()],
            postgres: Some(PostgresSourceConfig {
                slot_name: "test_slot".to_string(),
                publication_name: "test_pub".to_string(),
            }),
            mysql: None,
        };
        let result = create_source(&config).await;
        // Dispatch reached Postgres; the underlying connect fails (no
        // server), which is what we want: the error proves we hit the
        // PG branch and didn't, e.g., panic on a missing PG config.
        let err = match result {
            Err(e) => e.to_string().to_lowercase(),
            Ok(_) => panic!("expected create_source to fail without a real PG server"),
        };
        assert!(
            err.contains("connect")
                || err.contains("resolve")
                || err.contains("dns")
                || err.contains("no such")
                || err.contains("address"),
            "expected a connection-shaped error, got: {err}"
        );
    }

    #[cfg(feature = "mysql-source")]
    #[tokio::test]
    async fn create_source_dispatches_mysql() {
        use crate::config::MysqlSourceConfig;
        let config = SourceConfig {
            source_type: SourceType::Mysql,
            url: "mysql://invalid-host-for-unit-test:3306/none".to_string(),
            tables: vec!["t".to_string()],
            postgres: None,
            mysql: Some(MysqlSourceConfig {
                server_id: 5400,
                gtid_enabled: true,
                tls_skip_verify: false,
            }),
        };
        // MysqlSource::new doesn't open a connection at construction
        // time — it only parses the URL and stashes config. So this
        // call should SUCCEED (the connection is opened on `setup()`).
        // The assertion verifies we hit the MySQL branch (returns Ok).
        let result = create_source(&config).await;
        if let Err(e) = result {
            panic!("expected MySQL branch to construct without connecting; got: {e}");
        }
    }
}
