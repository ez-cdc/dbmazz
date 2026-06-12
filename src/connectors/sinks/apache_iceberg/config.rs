// Copyright 2025
// Licensed under the Elastic License v2.0

//! Apache Iceberg Sink Configuration
//!
//! Handles configuration parsing for the Apache Iceberg sink connector.
//! Communicates with a REST catalog for table management and commit operations.

use anyhow::{anyhow, Result};
use tracing::info;

use crate::config::SinkConfig;

/// Apache Iceberg sink configuration for REST catalog access.
#[derive(Clone)]
pub struct ApacheIcebergSinkConfig {
    /// REST catalog URL (e.g., "http://localhost:8181")
    pub url: String,

    /// Iceberg warehouse path (e.g., "s3://my-bucket/warehouse")
    pub warehouse: String,

    /// Target namespace/database within the catalog
    pub namespace: String,

    /// REST API prefix (default: "v1")
    pub prefix: String,
}

impl std::fmt::Debug for ApacheIcebergSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApacheIcebergSinkConfig")
            .field("url", &self.url)
            .field("warehouse", &self.warehouse)
            .field("namespace", &self.namespace)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl ApacheIcebergSinkConfig {
    /// Creates an Apache Iceberg configuration from the generic SinkConfig + env vars.
    pub fn from_sink_config(config: &SinkConfig) -> Result<Self> {
        let url = config.url.clone();

        let warehouse =
            std::env::var("SINK_WAREHOUSE").map_err(|_| anyhow!("SINK_WAREHOUSE must be set"))?;

        let namespace = config.database.clone();

        let prefix = std::env::var("SINK_ICEBERG_PREFIX").unwrap_or_else(|_| "v1".to_string());

        info!(
            "ApacheIcebergSinkConfig: url={}, warehouse={}, namespace={}, prefix={}",
            url, warehouse, namespace, prefix
        );

        Ok(Self {
            url,
            warehouse,
            namespace,
            prefix,
        })
    }

    /// Returns the full REST catalog base URL including the API prefix.
    pub fn catalog_url(&self) -> String {
        format!("{}/{}", self.url.trim_end_matches('/'), self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};

    fn test_sink_config() -> SinkConfig {
        SinkConfig {
            sink_type: SinkType::ApacheIceberg,
            url: "http://localhost:8181".to_string(),
            port: 8181,
            database: "test_db".to_string(),
            user: String::new(),
            password: String::new(),
            specific: SinkSpecificConfig::ApacheIceberg,
        }
    }

    #[test]
    fn test_from_sink_config() {
        temp_env::with_vars(
            [
                ("SINK_WAREHOUSE", Some("s3://test-bucket/warehouse")),
                ("SINK_ICEBERG_PREFIX", Some("v2")),
            ],
            || {
                let sink_config = test_sink_config();
                let config = ApacheIcebergSinkConfig::from_sink_config(&sink_config).unwrap();

                assert_eq!(config.url, "http://localhost:8181");
                assert_eq!(config.warehouse, "s3://test-bucket/warehouse");
                assert_eq!(config.namespace, "test_db");
                assert_eq!(config.prefix, "v2");
            },
        );
    }

    #[test]
    fn test_from_sink_config_default_prefix() {
        temp_env::with_vars(
            [
                ("SINK_WAREHOUSE", Some("s3://test-bucket/warehouse")),
                ("SINK_ICEBERG_PREFIX", None::<&str>),
            ],
            || {
                let sink_config = test_sink_config();
                let config = ApacheIcebergSinkConfig::from_sink_config(&sink_config).unwrap();

                assert_eq!(config.prefix, "v1");
            },
        );
    }

    #[test]
    fn test_from_sink_config_missing_warehouse() {
        temp_env::with_vars([("SINK_WAREHOUSE", None::<&str>)], || {
            let sink_config = test_sink_config();
            let result = ApacheIcebergSinkConfig::from_sink_config(&sink_config);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("SINK_WAREHOUSE"));
        });
    }

    #[test]
    fn test_catalog_url() {
        temp_env::with_vars(
            [
                ("SINK_WAREHOUSE", Some("s3://test-bucket/warehouse")),
                ("SINK_ICEBERG_PREFIX", Some("v2")),
            ],
            || {
                let sink_config = test_sink_config();
                let config = ApacheIcebergSinkConfig::from_sink_config(&sink_config).unwrap();

                assert_eq!(config.catalog_url(), "http://localhost:8181/v2");
            },
        );
    }

    #[test]
    fn test_catalog_url_trailing_slash() {
        // Ensure trailing slash on url is handled
        let mut cfg = test_sink_config();
        cfg.url = "http://localhost:8181/".to_string();

        temp_env::with_vars(
            [
                ("SINK_WAREHOUSE", Some("s3://test-bucket/warehouse")),
                ("SINK_ICEBERG_PREFIX", Some("v1")),
            ],
            || {
                let config = ApacheIcebergSinkConfig::from_sink_config(&cfg).unwrap();
                assert_eq!(config.catalog_url(), "http://localhost:8181/v1");
            },
        );
    }

    #[test]
    fn test_debug_no_redaction() {
        temp_env::with_vars(
            [
                ("SINK_WAREHOUSE", Some("s3://test-bucket/warehouse")),
                ("SINK_ICEBERG_PREFIX", Some("v1")),
            ],
            || {
                let sink_config = test_sink_config();
                let config = ApacheIcebergSinkConfig::from_sink_config(&sink_config).unwrap();

                let debug_str = format!("{:?}", config);
                assert!(debug_str.contains("http://localhost:8181"));
                assert!(debug_str.contains("s3://test-bucket/warehouse"));
                assert!(debug_str.contains("test_db"));
                assert!(!debug_str.contains("REDACTED"));
            },
        );
    }
}
