// Copyright 2025
// Licensed under the Elastic License v2.0

//! Sink connectors.

#[cfg(feature = "sink-postgres")]
pub mod postgres;
#[cfg(feature = "sink-snowflake")]
pub mod snowflake;
#[cfg(feature = "sink-starrocks")]
pub mod starrocks;

use anyhow::Result;

#[cfg(feature = "sink-postgres")]
use self::postgres::PostgresSink;
#[cfg(feature = "sink-snowflake")]
use self::snowflake::SnowflakeSink;
#[cfg(feature = "sink-starrocks")]
use self::starrocks::StarRocksSink;
use crate::config::{SinkConfig, SinkType};
use crate::core::{Sink, SinkMode};

/// Creates a sink connector based on the provided configuration and mode.
///
/// This factory function instantiates the appropriate sink implementation
/// based on the `sink_type` field in the configuration.
///
/// # Arguments
///
/// * `config` - The sink configuration containing connection details and type
/// * `mode` - `SinkMode::Primary` for the main CDC sink (runs background tasks),
///   `SinkMode::SnapshotWorker` for snapshot worker instances (skips background tasks)
pub fn create_sink(config: &SinkConfig, mode: SinkMode) -> Result<Box<dyn Sink>> {
    match &config.sink_type {
        #[cfg(feature = "sink-starrocks")]
        SinkType::StarRocks => {
            let sink = StarRocksSink::new(config, mode)?;
            Ok(Box::new(sink))
        }
        #[cfg(feature = "sink-postgres")]
        SinkType::Postgres => {
            let sink = PostgresSink::new(config, mode)?;
            Ok(Box::new(sink))
        }
        #[cfg(feature = "sink-snowflake")]
        SinkType::Snowflake => {
            let sink = SnowflakeSink::new(config, mode)?;
            Ok(Box::new(sink))
        }
        #[allow(unreachable_patterns)]
        other => anyhow::bail!(
            "Sink type '{}' is not compiled in this build. Rebuild with the appropriate feature flag (e.g. --features sink-starrocks)",
            other
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};

    #[cfg(feature = "sink-starrocks")]
    #[test]
    fn test_create_starrocks_sink() {
        let config = SinkConfig {
            sink_type: SinkType::StarRocks,
            url: "http://starrocks:8040".to_string(),
            port: 9030,
            database: "test_db".to_string(),
            user: "root".to_string(),
            password: "".to_string(),
            specific: SinkSpecificConfig::StarRocks,
        };

        let result = create_sink(&config, SinkMode::Primary);
        assert!(result.is_ok());
    }

    #[cfg(feature = "sink-postgres")]
    #[test]
    fn test_create_postgres_sink() {
        use crate::config::PostgresSinkConfig;

        let config = SinkConfig {
            sink_type: SinkType::Postgres,
            url: "postgres://localhost/test".to_string(),
            port: 5432,
            database: "test_db".to_string(),
            user: "postgres".to_string(),
            password: "".to_string(),
            specific: SinkSpecificConfig::Postgres(PostgresSinkConfig {
                schema: "public".to_string(),
                job_name: "test_slot".to_string(),
            }),
        };

        let result = create_sink(&config, SinkMode::Primary);
        assert!(result.is_ok());
    }

    #[cfg(feature = "sink-snowflake")]
    #[test]
    fn test_create_snowflake_sink() {
        use serial_test::serial;

        std::env::set_var("SINK_SNOWFLAKE_ACCOUNT", "test_account");
        std::env::set_var("SINK_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH");

        let config = SinkConfig {
            sink_type: SinkType::Snowflake,
            url: "test_account.snowflakecomputing.com".to_string(),
            port: 443,
            database: "test_db".to_string(),
            user: "test_user".to_string(),
            password: "test_pass".to_string(),
            specific: SinkSpecificConfig::Snowflake,
        };

        let result = create_sink(&config, SinkMode::Primary);
        assert!(result.is_ok());

        std::env::remove_var("SINK_SNOWFLAKE_ACCOUNT");
        std::env::remove_var("SINK_SNOWFLAKE_WAREHOUSE");
    }
}
