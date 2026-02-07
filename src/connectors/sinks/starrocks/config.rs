// Copyright 2025
// Licensed under the Elastic License v2.0

//! StarRocks Sink Configuration
//!
//! This module handles configuration parsing and validation for the StarRocks
//! sink connector. It supports configuration from both the new generic
//! `SinkConfig` and legacy environment variables.

use anyhow::{Result, anyhow};

use crate::config::SinkConfig;

/// Default HTTP port for StarRocks Stream Load API
const DEFAULT_HTTP_PORT: u16 = 8040;

/// Default MySQL protocol port for DDL operations
const DEFAULT_MYSQL_PORT: u16 = 9030;

/// StarRocks-specific sink configuration.
///
/// This struct contains all the configuration needed to connect to StarRocks
/// for both Stream Load (HTTP) and DDL operations (MySQL protocol).
#[derive(Clone)]
pub struct StarRocksSinkConfig {
    /// HTTP URL for Stream Load API (e.g., "http://starrocks:8040")
    pub http_url: String,

    /// MySQL protocol port for DDL operations (default: 9030)
    pub mysql_port: u16,

    /// Target database name
    pub database: String,

    /// Username for authentication
    pub user: String,

    /// Password for authentication
    pub password: String,

    /// Connection timeout in seconds (default: 30)
    #[allow(dead_code)]
    pub timeout_secs: u64,

    /// Maximum filter ratio for Stream Load (default: 0.2)
    #[allow(dead_code)]
    pub max_filter_ratio: f64,
}

impl std::fmt::Debug for StarRocksSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StarRocksSinkConfig")
            .field("http_url", &self.http_url)
            .field("mysql_port", &self.mysql_port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("timeout_secs", &self.timeout_secs)
            .field("max_filter_ratio", &self.max_filter_ratio)
            .finish()
    }
}

#[allow(dead_code)]
impl StarRocksSinkConfig {
    /// Creates a StarRocks configuration from the generic SinkConfig.
    ///
    /// # Arguments
    ///
    /// * `config` - Generic sink configuration
    ///
    /// # Returns
    ///
    /// StarRocks-specific configuration
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing or invalid
    pub fn from_sink_config(config: &SinkConfig) -> Result<Self> {
        // Parse HTTP URL - ensure it has the correct port
        let http_url = Self::normalize_http_url(&config.url)?;

        Ok(Self {
            http_url,
            mysql_port: config.port,
            database: config.database.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
            timeout_secs: 30,
            max_filter_ratio: 0.2,
        })
    }

    /// Normalizes the HTTP URL for Stream Load.
    ///
    /// - Adds `http://` prefix if missing
    /// - Ensures port 8040 is used for Stream Load
    fn normalize_http_url(url: &str) -> Result<String> {
        let url = url.trim();

        // Add scheme if missing
        let url = if !url.starts_with("http://") && !url.starts_with("https://") {
            format!("http://{}", url)
        } else {
            url.to_string()
        };

        // Parse to check if port is included
        // URL format: http://host:port or http://host
        let parts: Vec<&str> = url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split(':')
            .collect();

        let scheme = if url.starts_with("https://") { "https" } else { "http" };

        match parts.len() {
            1 => {
                // No port specified, add default HTTP port
                Ok(format!("{}://{}:{}", scheme, parts[0], DEFAULT_HTTP_PORT))
            }
            2 => {
                // Port specified, use as-is
                Ok(url)
            }
            _ => Err(anyhow!("Invalid URL format: {}", url)),
        }
    }

    /// Extracts the hostname from the HTTP URL.
    pub fn hostname(&self) -> String {
        self.http_url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split(':')
            .next()
            .unwrap_or("localhost")
            .to_string()
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.database.is_empty() {
            return Err(anyhow!("Database name is required"));
        }

        if self.user.is_empty() {
            return Err(anyhow!("Username is required"));
        }

        if self.http_url.is_empty() {
            return Err(anyhow!("HTTP URL is required"));
        }

        if self.timeout_secs == 0 {
            return Err(anyhow!("Timeout must be greater than 0"));
        }

        if self.max_filter_ratio < 0.0 || self.max_filter_ratio > 1.0 {
            return Err(anyhow!("max_filter_ratio must be between 0.0 and 1.0"));
        }

        Ok(())
    }
}

impl Default for StarRocksSinkConfig {
    fn default() -> Self {
        Self {
            http_url: format!("http://localhost:{}", DEFAULT_HTTP_PORT),
            mysql_port: DEFAULT_MYSQL_PORT,
            database: String::new(),
            user: "root".to_string(),
            password: String::new(),
            timeout_secs: 30,
            max_filter_ratio: 0.2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkType};
    use crate::config::StarRocksSinkConfig as ConfigStarRocksSinkConfig;

    #[test]
    fn test_from_sink_config() {
        let config = SinkConfig {
            sink_type: SinkType::StarRocks,
            url: "starrocks.example.com".to_string(),
            port: 9030,
            database: "cdc_db".to_string(),
            user: "admin".to_string(),
            password: "secret".to_string(),
            starrocks: Some(ConfigStarRocksSinkConfig {}),
        };

        let sr_config = StarRocksSinkConfig::from_sink_config(&config).unwrap();

        assert_eq!(sr_config.http_url, "http://starrocks.example.com:8040");
        assert_eq!(sr_config.mysql_port, 9030);
        assert_eq!(sr_config.database, "cdc_db");
        assert_eq!(sr_config.user, "admin");
        assert_eq!(sr_config.password, "secret");
    }

    #[test]
    fn test_normalize_http_url() {
        // Without scheme or port
        assert_eq!(
            StarRocksSinkConfig::normalize_http_url("starrocks").unwrap(),
            "http://starrocks:8040"
        );

        // With scheme, without port
        assert_eq!(
            StarRocksSinkConfig::normalize_http_url("http://starrocks").unwrap(),
            "http://starrocks:8040"
        );

        // With custom port
        assert_eq!(
            StarRocksSinkConfig::normalize_http_url("http://starrocks:8041").unwrap(),
            "http://starrocks:8041"
        );

        // HTTPS
        assert_eq!(
            StarRocksSinkConfig::normalize_http_url("https://starrocks.example.com").unwrap(),
            "https://starrocks.example.com:8040"
        );
    }

    #[test]
    fn test_hostname_extraction() {
        let config = StarRocksSinkConfig {
            http_url: "http://starrocks.example.com:8040".to_string(),
            ..Default::default()
        };

        assert_eq!(config.hostname(), "starrocks.example.com");
    }

    #[test]
    fn test_validation() {
        let config = StarRocksSinkConfig {
            database: "test_db".to_string(),
            ..Default::default()
        };

        assert!(config.validate().is_ok());

        // Empty database should fail
        let config_empty = StarRocksSinkConfig {
            database: String::new(),
            ..Default::default()
        };
        assert!(config_empty.validate().is_err());
    }

    #[test]
    fn test_default_values() {
        let config = StarRocksSinkConfig::default();

        assert_eq!(config.http_url, "http://localhost:8040");
        assert_eq!(config.mysql_port, 9030);
        assert_eq!(config.user, "root");
        assert_eq!(config.timeout_secs, 30);
        assert!((config.max_filter_ratio - 0.2).abs() < f64::EPSILON);
    }
}
