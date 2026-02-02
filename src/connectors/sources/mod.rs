// Copyright 2025
// Licensed under the Elastic License v2.0

#![allow(dead_code)]
//! Source connectors for CDC data ingestion
//!
//! Each source implements the `Source` trait from `crate::core::traits`.

pub mod postgres;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;

use crate::config::{SourceConfig, SourceType};
use crate::core::{CdcRecord, Source};

pub use postgres::PostgresSource;

/// Factory function to create a source based on configuration
///
/// # Arguments
/// * `config` - The source configuration containing type and connection details
/// * `record_tx` - Channel sender for emitting CdcRecord events
///
/// # Returns
/// A boxed Source trait object ready for streaming
///
/// # Example
/// ```ignore
/// let (tx, rx) = mpsc::channel(10000);
/// let source = create_source(&config.source, tx).await?;
/// source.start().await?;
/// ```
pub async fn create_source(
    config: &SourceConfig,
    record_tx: mpsc::Sender<CdcRecord>,
) -> Result<Box<dyn Source>> {
    match config.source_type {
        SourceType::Postgres => {
            let pg_config = config
                .postgres
                .as_ref()
                .ok_or_else(|| anyhow!("PostgreSQL config required for postgres source type"))?;

            let source = PostgresSource::new(
                &config.url,
                pg_config.slot_name.clone(),
                pg_config.publication_name.clone(),
                config.tables.clone(),
                record_tx,
            )
            .await?;

            Ok(Box::new(source))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_source_missing_postgres_config() {
        let (tx, _rx) = mpsc::channel(100);
        let config = SourceConfig {
            source_type: SourceType::Postgres,
            url: "postgres://localhost/test".to_string(),
            tables: vec!["test".to_string()],
            postgres: None, // Missing!
        };

        let result = create_source(&config, tx).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("PostgreSQL config required"),
            "Expected error about missing PostgreSQL config, got: {}",
            err
        );
    }
}
