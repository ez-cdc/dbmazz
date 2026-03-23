// Copyright 2025
// Licensed under the Elastic License v2.0

//! # Sink Connectors
//!
//! This module contains sink connector implementations that write CDC records
//! to various target systems. Each sink implements the `Sink` trait from
//! `crate::core::traits`.
//!
//! ## Available Sinks
//!
//! - **StarRocks**: OLAP database with Stream Load API support
//! - **PostgreSQL**: Relational database via raw table + MERGE (PG >= 15)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use crate::connectors::sinks::create_sink;
//! use crate::config::SinkConfig;
//!
//! let sink = create_sink(&config)?;
//! sink.validate_connection().await?;
//! sink.write_batch(records).await?;
//! ```

pub mod postgres;
pub mod starrocks;

use anyhow::Result;

use self::postgres::PostgresSink;
use self::starrocks::StarRocksSink;
use crate::config::{SinkConfig, SinkType};
use crate::core::Sink;

/// Creates a sink connector based on the provided configuration.
///
/// This factory function instantiates the appropriate sink implementation
/// based on the `sink_type` field in the configuration.
///
/// # Arguments
///
/// * `config` - The sink configuration containing connection details and type
///
/// # Returns
///
/// A boxed `Sink` trait object that can be used to write CDC records
///
/// # Errors
///
/// Returns an error if:
/// - The sink type is not supported
/// - The sink configuration is invalid
/// - The sink fails to initialize
///
/// # Example
///
/// ```rust,ignore
/// let config = SinkConfig {
///     sink_type: SinkType::StarRocks,
///     url: "http://starrocks:8040".to_string(),
///     port: 9030,
///     database: "cdc_db".to_string(),
///     user: "root".to_string(),
///     password: "".to_string(),
///     starrocks: Some(StarRocksSinkConfig {}),
/// };
///
/// let sink = create_sink(&config)?;
/// ```
pub fn create_sink(config: &SinkConfig) -> Result<Box<dyn Sink>> {
    match config.sink_type {
        SinkType::StarRocks => {
            let sink = StarRocksSink::new(config)?;
            Ok(Box::new(sink))
        }
        SinkType::Postgres => {
            let sink = PostgresSink::new(config)?;
            Ok(Box::new(sink))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PostgresSinkConfig, SinkConfig, SinkType, StarRocksSinkConfig};

    #[test]
    fn test_create_starrocks_sink() {
        let config = SinkConfig {
            sink_type: SinkType::StarRocks,
            url: "http://starrocks:8040".to_string(),
            port: 9030,
            database: "test_db".to_string(),
            user: "root".to_string(),
            password: "".to_string(),
            starrocks: Some(StarRocksSinkConfig {}),
            postgres: None,
        };

        let result = create_sink(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_postgres_sink() {
        let config = SinkConfig {
            sink_type: SinkType::Postgres,
            url: "postgres://localhost/test".to_string(),
            port: 5432,
            database: "test_db".to_string(),
            user: "postgres".to_string(),
            password: "".to_string(),
            starrocks: None,
            postgres: Some(PostgresSinkConfig {
                schema: "public".to_string(),
                job_name: "test_slot".to_string(),
            }),
        };

        let result = create_sink(&config);
        assert!(result.is_ok());
    }
}
