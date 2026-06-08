// Copyright 2025
// Licensed under the Elastic License v2.0

//! Iceberg Sink Configuration
//!
//! Parses S3-compatible object store and Iceberg catalog configuration
//! from environment variables.

use anyhow::{anyhow, Result};
use crate::config::SinkConfig;

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
            .field("access_key_id", &if self.access_key_id.is_empty() { "(not set)" } else { "[REDACTED]" })
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

impl IcebergSinkConfig {
    /// Creates IcebergSinkConfig from generic SinkConfig + env vars.
    pub fn from_sink_config(config: &SinkConfig) -> Result<Self> {
        let bucket = std::env::var("S3_BUCKET")
            .map_err(|_| anyhow!("S3_BUCKET must be set"))?;

        let prefix = std::env::var("S3_PREFIX")
            .unwrap_or_else(|_| DEFAULT_PREFIX.to_string());

        let region = std::env::var("S3_REGION")
            .unwrap_or_else(|_| DEFAULT_REGION.to_string());

        let endpoint = std::env::var("S3_ENDPOINT").unwrap_or_default();
        let access_key_id = std::env::var("S3_ACCESS_KEY_ID").unwrap_or_default();
        let secret_access_key = std::env::var("S3_SECRET_ACCESS_KEY").unwrap_or_default();
        let role_arn = std::env::var("S3_ROLE_ARN").unwrap_or_default();

        let force_path_style = std::env::var("S3_FORCE_PATH_STYLE")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase() == "true";

        let catalog_uri = std::env::var("ICEBERG_CATALOG_URI").unwrap_or_default();

        // Build warehouse path: s3://bucket/prefix/warehouse
        let warehouse = std::env::var("ICEBERG_WAREHOUSE")
            .unwrap_or_else(|_| format!("s3://{}/{}/warehouse", bucket, prefix));

        let flush_files: usize = std::env::var("ICEBERG_FLUSH_FILES")
            .unwrap_or_else(|_| DEFAULT_FLUSH_FILES.to_string())
            .parse()
            .unwrap_or(DEFAULT_FLUSH_FILES);

        let flush_bytes: u64 = std::env::var("ICEBERG_FLUSH_BYTES")
            .unwrap_or_else(|_| DEFAULT_FLUSH_BYTES.to_string())
            .parse()
            .unwrap_or(DEFAULT_FLUSH_BYTES);

        Ok(Self {
            bucket,
            prefix,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            role_arn,
            force_path_style,
            catalog_uri,
            warehouse,
            flush_files,
            flush_bytes,
        })
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
        std::env::set_var("S3_BUCKET", "test-bucket");
        std::env::set_var("S3_PREFIX", "test-prefix");
        std::env::set_var("S3_REGION", "us-west-2");
        std::env::set_var("S3_ENDPOINT", "http://minio:9000");
        std::env::set_var("S3_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("S3_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("S3_FORCE_PATH_STYLE", "true");
        std::env::set_var("ICEBERG_CATALOG_URI", "http://catalog:8181");
        std::env::set_var("ICEBERG_FLUSH_FILES", "50");

        let sink_config = SinkConfig {
            sink_type: SinkType::Iceberg,
            url: "".to_string(),
            port: 0,
            database: "test_db".to_string(),
            user: "".to_string(),
            password: "".to_string(),
            specific: SinkSpecificConfig::Iceberg(IcebergSinkConfig::default()),
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

        std::env::remove_var("S3_BUCKET");
        std::env::remove_var("S3_PREFIX");
        std::env::remove_var("S3_REGION");
        std::env::remove_var("S3_ENDPOINT");
        std::env::remove_var("S3_ACCESS_KEY_ID");
        std::env::remove_var("S3_SECRET_ACCESS_KEY");
        std::env::remove_var("S3_FORCE_PATH_STYLE");
        std::env::remove_var("ICEBERG_CATALOG_URI");
        std::env::remove_var("ICEBERG_FLUSH_FILES");
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

        assert_eq!(config.staging_prefix("public.users"), "ez-cdc/_staging/public.users/");
        assert_eq!(config.data_prefix("public.users"), "ez-cdc/public.users/");
    }
}
