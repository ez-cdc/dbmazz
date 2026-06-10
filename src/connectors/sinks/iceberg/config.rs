// Copyright 2026
// Licensed under the Elastic License v2.0

//! Iceberg sink configuration.
//!
//! Loaded from environment variables by `Config::from_env` (config.rs)
//! via [`IcebergSinkConfig::from_env`]. This module is the single source
//! of truth for the sink's env vars — nothing else reads them.

use std::collections::HashMap;

use anyhow::{Context, Result};

/// Configuration for the Iceberg sink.
///
/// The target namespace comes from `SINK_DATABASE` (consistent with the
/// other sinks); everything else is Iceberg/S3-specific.
#[derive(Clone)]
pub struct IcebergSinkConfig {
    /// Iceberg REST catalog URI (required). e.g. `http://lakekeeper:8181/catalog`
    pub catalog_uri: String,
    /// Warehouse location or identifier, passed to the REST catalog (required).
    pub warehouse: String,
    /// Target Iceberg namespace (from `SINK_DATABASE`).
    pub namespace: String,
    /// S3 region (optional; some S3-compatible stores ignore it).
    pub s3_region: String,
    /// Custom S3 endpoint for MinIO/LocalStack (optional).
    pub s3_endpoint: String,
    /// Static S3 access key id (optional; falls back to ambient credentials).
    pub s3_access_key_id: String,
    /// Static S3 secret access key (optional).
    pub s3_secret_access_key: String,
    /// Path-style addressing, required by MinIO.
    pub s3_path_style: bool,
    /// Retries for Iceberg catalog commits on transient failures.
    pub commit_retries: u32,
}

impl std::fmt::Debug for IcebergSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSinkConfig")
            .field("catalog_uri", &self.catalog_uri)
            .field("warehouse", &self.warehouse)
            .field("namespace", &self.namespace)
            .field("s3_region", &self.s3_region)
            .field("s3_endpoint", &self.s3_endpoint)
            .field("s3_access_key_id", &"[REDACTED]")
            .field("s3_secret_access_key", &"[REDACTED]")
            .field("s3_path_style", &self.s3_path_style)
            .field("commit_retries", &self.commit_retries)
            .finish()
    }
}

impl IcebergSinkConfig {
    /// Load the Iceberg sink configuration from environment variables.
    ///
    /// `namespace` is the already-parsed `SINK_DATABASE` value.
    pub fn from_env(namespace: &str) -> Result<Self> {
        let catalog_uri = std::env::var("ICEBERG_CATALOG_URI")
            .context("ICEBERG_CATALOG_URI must be set for SINK_TYPE=iceberg")?;
        let warehouse = std::env::var("ICEBERG_WAREHOUSE")
            .context("ICEBERG_WAREHOUSE must be set for SINK_TYPE=iceberg")?;

        let commit_retries: u32 = std::env::var("ICEBERG_COMMIT_RETRIES")
            .unwrap_or_else(|_| "3".to_string())
            .parse()
            .context("ICEBERG_COMMIT_RETRIES must be a non-negative integer")?;

        Ok(Self {
            catalog_uri,
            warehouse,
            namespace: namespace.to_string(),
            s3_region: std::env::var("S3_REGION").unwrap_or_default(),
            s3_endpoint: std::env::var("S3_ENDPOINT").unwrap_or_default(),
            s3_access_key_id: std::env::var("S3_ACCESS_KEY_ID").unwrap_or_default(),
            s3_secret_access_key: std::env::var("S3_SECRET_ACCESS_KEY").unwrap_or_default(),
            s3_path_style: std::env::var("S3_PATH_STYLE")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            commit_retries,
        })
    }

    /// Properties map for the REST catalog (`uri`, `warehouse`) plus the
    /// FileIO S3 properties. Empty optional values are omitted so the
    /// SDK/ambient defaults apply.
    pub fn catalog_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), self.catalog_uri.clone());
        props.insert("warehouse".to_string(), self.warehouse.clone());

        if !self.s3_region.is_empty() {
            props.insert(iceberg::io::S3_REGION.to_string(), self.s3_region.clone());
        }
        if !self.s3_endpoint.is_empty() {
            props.insert(
                iceberg::io::S3_ENDPOINT.to_string(),
                self.s3_endpoint.clone(),
            );
        }
        if !self.s3_access_key_id.is_empty() {
            props.insert(
                iceberg::io::S3_ACCESS_KEY_ID.to_string(),
                self.s3_access_key_id.clone(),
            );
        }
        if !self.s3_secret_access_key.is_empty() {
            props.insert(
                iceberg::io::S3_SECRET_ACCESS_KEY.to_string(),
                self.s3_secret_access_key.clone(),
            );
        }
        if self.s3_path_style {
            props.insert(
                iceberg::io::S3_PATH_STYLE_ACCESS.to_string(),
                "true".to_string(),
            );
        }
        props
    }

    /// Extract the Iceberg-specific config from a generic `SinkConfig`.
    pub fn from_sink_config(config: &crate::config::SinkConfig) -> Result<Self> {
        match &config.specific {
            crate::config::SinkSpecificConfig::Iceberg(cfg) => Ok(cfg.clone()),
            _ => anyhow::bail!(
                "Expected Iceberg sink config, got sink type '{}'",
                config.sink_type
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> IcebergSinkConfig {
        IcebergSinkConfig {
            catalog_uri: "http://localhost:8181/catalog".to_string(),
            warehouse: "warehouse".to_string(),
            namespace: "analytics".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_access_key_id: "minioadmin".to_string(),
            s3_secret_access_key: "supersecret".to_string(),
            s3_path_style: true,
            commit_retries: 3,
        }
    }

    #[test]
    fn debug_redacts_credentials() {
        let rendered = format!("{:?}", test_config());
        assert!(!rendered.contains("minioadmin"));
        assert!(!rendered.contains("supersecret"));
        assert!(rendered.contains("[REDACTED]"));
        assert!(rendered.contains("http://localhost:8181/catalog"));
    }

    #[test]
    fn catalog_props_include_s3_settings() {
        let props = test_config().catalog_props();
        assert_eq!(props.get("uri").unwrap(), "http://localhost:8181/catalog");
        assert_eq!(props.get("warehouse").unwrap(), "warehouse");
        assert_eq!(
            props.get(iceberg::io::S3_ENDPOINT).unwrap(),
            "http://localhost:9000"
        );
        assert_eq!(
            props.get(iceberg::io::S3_PATH_STYLE_ACCESS).unwrap(),
            "true"
        );
    }

    #[test]
    fn catalog_props_omit_empty_optionals() {
        let mut cfg = test_config();
        cfg.s3_endpoint = String::new();
        cfg.s3_access_key_id = String::new();
        cfg.s3_secret_access_key = String::new();
        cfg.s3_path_style = false;
        let props = cfg.catalog_props();
        assert!(!props.contains_key(iceberg::io::S3_ENDPOINT));
        assert!(!props.contains_key(iceberg::io::S3_ACCESS_KEY_ID));
        assert!(!props.contains_key(iceberg::io::S3_SECRET_ACCESS_KEY));
        assert!(!props.contains_key(iceberg::io::S3_PATH_STYLE_ACCESS));
    }
}
