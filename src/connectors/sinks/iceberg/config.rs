// Copyright 2025
// Licensed under the Elastic License v2.0

//! Iceberg Sink Configuration
//!
//! Parses S3-compatible object store and Iceberg catalog configuration
//! from environment variables.

use crate::config::SinkConfig;
use anyhow::{anyhow, Result};

const DEFAULT_PREFIX: &str = "ez-cdc";
const DEFAULT_REGION: &str = "us-east-1";
const DEFAULT_FLUSH_FILES: usize = 20;
const DEFAULT_FLUSH_BYTES: u64 = 100 * 1024 * 1024; // 100 MB

/// Iceberg sink configuration.
#[derive(Clone)]
pub struct IcebergSinkConfig {
    /// S3 bucket name
    pub bucket: String,
    /// Key prefix for all objects
    pub prefix: String,
    /// AWS region
    pub region: String,
    /// Custom S3 endpoint (MinIO, GCS, LocalStack)
    pub endpoint: String,
    /// Static access key
    pub access_key_id: String,
    /// Static secret key
    pub secret_access_key: String,
    /// IAM role ARN to assume
    pub role_arn: String,
    /// Use path-style addressing (required for MinIO)
    pub force_path_style: bool,
    /// Iceberg REST catalog URI (optional)
    pub catalog_uri: String,
    /// Iceberg warehouse path
    pub warehouse: String,
    /// Max files before auto-commit
    pub flush_files: usize,
    /// Max bytes before auto-commit
    pub flush_bytes: u64,
}

impl std::fmt::Debug for IcebergSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSinkConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field(
                "access_key_id",
                &if self.access_key_id.is_empty() {
                    "(not set)"
                } else {
                    "[REDACTED]"
                },
            )
            .field("secret_access_key", &"[REDACTED]")
            .field("role_arn", &self.role_arn)
            .field("force_path_style", &self.force_path_style)
            .field("catalog_uri", &self.catalog_uri)
            .field("warehouse", &self.warehouse)
            .field("flush_files", &self.flush_files)
            .field("flush_bytes", &self.flush_bytes)
            .finish()
    }
}

impl Default for IcebergSinkConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: DEFAULT_PREFIX.to_string(),
            region: DEFAULT_REGION.to_string(),
            endpoint: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            role_arn: String::new(),
            force_path_style: false,
            catalog_uri: String::new(),
            warehouse: String::new(),
            flush_files: DEFAULT_FLUSH_FILES,
            flush_bytes: DEFAULT_FLUSH_BYTES,
        }
    }
}

impl IcebergSinkConfig {
    /// Creates IcebergSinkConfig from a generic SinkConfig by extracting
    /// the pre-built config from `SinkSpecificConfig::Iceberg(...)`.
    pub fn from_sink_config(config: &SinkConfig) -> Result<Self> {
        match &config.specific {
            crate::config::SinkSpecificConfig::Iceberg(cfg) => Ok(cfg.clone()),
            _ => Err(anyhow!(
                "Expected Iceberg sink config, got {:?}",
                config.sink_type
            )),
        }
    }

    /// Returns the S3 staging prefix for in-flight files.
    pub fn staging_prefix(&self, table_name: &str) -> String {
        format!("{}/_staging/{}/", self.prefix, table_name)
    }

    /// Returns the S3 data prefix for committed Iceberg files.
    pub fn data_prefix(&self, table_name: &str) -> String {
        format!("{}/{}/", self.prefix, table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};

    #[test]
    fn test_config_from_sink_config() {
        let expected = IcebergSinkConfig {
            bucket: "test-bucket".to_string(),
            prefix: "test-prefix".to_string(),
            region: "us-west-2".to_string(),
            endpoint: "http://minio:9000".to_string(),
            access_key_id: "minioadmin".to_string(),
            secret_access_key: "minioadmin".to_string(),
            role_arn: String::new(),
            force_path_style: true,
            catalog_uri: "http://catalog:8181".to_string(),
            warehouse: "s3://test-bucket/test-prefix/warehouse".to_string(),
            flush_files: 50,
            flush_bytes: DEFAULT_FLUSH_BYTES,
        };

        let sink_config = SinkConfig {
            sink_type: SinkType::Iceberg,
            url: "".to_string(),
            port: 0,
            database: "test_db".to_string(),
            user: "".to_string(),
            password: "".to_string(),
            specific: SinkSpecificConfig::Iceberg(expected.clone()),
        };

        let config = IcebergSinkConfig::from_sink_config(&sink_config).unwrap();
        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.prefix, "test-prefix");
        assert_eq!(config.region, "us-west-2");
        assert_eq!(config.endpoint, "http://minio:9000");
        assert!(config.force_path_style);
        assert_eq!(config.catalog_uri, "http://catalog:8181");
        assert_eq!(config.flush_files, 50);
        assert_eq!(config.flush_bytes, DEFAULT_FLUSH_BYTES);
    }

    #[test]
    fn test_staging_and_data_prefix() {
        let config = IcebergSinkConfig {
            bucket: "b".to_string(),
            prefix: "ez-cdc".to_string(),
            region: "us-east-1".to_string(),
            endpoint: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            role_arn: String::new(),
            force_path_style: false,
            catalog_uri: String::new(),
            warehouse: "s3://b/ez-cdc/warehouse".to_string(),
            flush_files: 20,
            flush_bytes: 100_000_000,
        };

        assert_eq!(
            config.staging_prefix("public.users"),
            "ez-cdc/_staging/public.users/"
        );
        assert_eq!(config.data_prefix("public.users"), "ez-cdc/public.users/");
    }
}
