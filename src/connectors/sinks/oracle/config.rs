// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle Sink Configuration
//!
//! This module handles configuration parsing and validation for the Oracle
//! sink connector.

use anyhow::{anyhow, Result};

use crate::config::SinkConfig;

/// Default Oracle database port
const DEFAULT_ORACLE_PORT: u16 = 1521;

/// Oracle-specific sink configuration.
#[derive(Clone)]
pub struct OracleSinkConfig {
    /// Oracle host
    pub host: String,
    /// Oracle listener port
    pub port: u16,
    /// Oracle service name (e.g., ORCLCDB, FREEPDB1)
    pub service_name: String,
    /// Username for authentication
    pub user: String,
    /// Password for authentication
    pub password: String,
    /// Target schema (default: the user's default schema)
    pub schema: String,
    /// Connection timeout in seconds (default: 30)
    pub timeout_secs: u64,
    /// Batch size for write operations (default: 1000)
    pub batch_size: usize,
}

impl std::fmt::Debug for OracleSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleSinkConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("service_name", &self.service_name)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("schema", &self.schema)
            .field("timeout_secs", &self.timeout_secs)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl OracleSinkConfig {
    /// Creates an Oracle configuration from the generic SinkConfig.
    ///
    /// # Arguments
    ///
    /// * `config` - Generic sink configuration
    ///
    /// # Returns
    ///
    /// Oracle-specific configuration
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing or invalid
    pub fn from_sink_config(config: &SinkConfig) -> Result<Self> {
        // Parse host:port/service_name from URL
        let url = if config.url.is_empty() {
            anyhow::bail!("Oracle SINK_URL is required (e.g., host:1521/ORCLCDB)")
        } else {
            config.url.clone()
        };

        let (host, port, service_name) = Self::parse_connect_string(&url, config.port)?;

        Ok(Self {
            host,
            port,
            service_name,
            user: config.user.clone(),
            password: config.password.clone(),
            schema: config.database.clone(),
            timeout_secs: 30,
            batch_size: 1000,
        })
    }

    /// Parse a connect string (host:port/service_name).
    fn parse_connect_string(url: &str, default_port: u16) -> Result<(String, u16, String)> {
        let url = url.trim();

        // Remove any scheme prefix
        let url = url
            .trim_start_matches("oracle://")
            .trim_start_matches("oracle:");

        // Split by '/' to separate host:port from service
        if let Some(slash_pos) = url.find('/') {
            let host_port = &url[..slash_pos];
            let service_name = url[slash_pos + 1..].to_string();

            if service_name.is_empty() {
                anyhow::bail!("Oracle service name is required (host:port/service_name)");
            }

            let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
                let host = host_port[..colon_pos].to_string();
                let port: u16 = host_port[colon_pos + 1..]
                    .parse()
                    .unwrap_or(DEFAULT_ORACLE_PORT);
                (host, port)
            } else {
                (host_port.to_string(), default_port)
            };

            Ok((host, port, service_name))
        } else {
            // No service name provided
            anyhow::bail!(
                "Oracle connection string must include service name: 'host:port/service_name'. Got: '{}'",
                url
            );
        }
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.host.is_empty() {
            return Err(anyhow!("Oracle host is required"));
        }
        if self.service_name.is_empty() {
            return Err(anyhow!("Oracle service name is required"));
        }
        if self.user.is_empty() {
            return Err(anyhow!("Oracle username is required"));
        }
        if self.schema.is_empty() {
            return Err(anyhow!("Oracle schema is required"));
        }
        if self.port == 0 {
            return Err(anyhow!("Oracle port must be greater than 0"));
        }
        if self.timeout_secs == 0 {
            return Err(anyhow!("Timeout must be greater than 0"));
        }
        if self.batch_size == 0 {
            return Err(anyhow!("Batch size must be greater than 0"));
        }
        Ok(())
    }
}

impl Default for OracleSinkConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: DEFAULT_ORACLE_PORT,
            service_name: String::new(),
            user: String::new(),
            password: String::new(),
            schema: String::new(),
            timeout_secs: 30,
            batch_size: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SinkConfig, SinkSpecificConfig, SinkType};

    #[test]
    fn test_from_sink_config() {
        let config = SinkConfig {
            sink_type: SinkType::Oracle,
            url: "oraclehost:1521/ORCLCDB".to_string(),
            port: 1521,
            database: "CDC_SCHEMA".to_string(),
            user: "cdc_user".to_string(),
            password: "cdc_pass".to_string(),
            specific: SinkSpecificConfig::Oracle,
        };

        let oracle_config = OracleSinkConfig::from_sink_config(&config).unwrap();

        assert_eq!(oracle_config.host, "oraclehost");
        assert_eq!(oracle_config.port, 1521);
        assert_eq!(oracle_config.service_name, "ORCLCDB");
        assert_eq!(oracle_config.user, "cdc_user");
        assert_eq!(oracle_config.password, "cdc_pass");
        assert_eq!(oracle_config.schema, "CDC_SCHEMA");
        assert_eq!(oracle_config.timeout_secs, 30);
        assert_eq!(oracle_config.batch_size, 1000);
    }

    #[test]
    fn test_parse_connect_string() {
        let (host, port, service) =
            OracleSinkConfig::parse_connect_string("oraclehost:1521/ORCLCDB", 1521).unwrap();
        assert_eq!(host, "oraclehost");
        assert_eq!(port, 1521);
        assert_eq!(service, "ORCLCDB");
    }

    #[test]
    fn test_parse_with_oracle_prefix() {
        let (host, port, service) =
            OracleSinkConfig::parse_connect_string("oracle://host:1521/service", 1521).unwrap();
        assert_eq!(host, "host");
        assert_eq!(port, 1521);
        assert_eq!(service, "service");
    }

    #[test]
    fn test_parse_default_port() {
        let (host, port, service) =
            OracleSinkConfig::parse_connect_string("host/FREEPDB1", 1521).unwrap();
        assert_eq!(host, "host");
        assert_eq!(port, 1521);
        assert_eq!(service, "FREEPDB1");
    }

    #[test]
    fn test_validation() {
        let config = OracleSinkConfig {
            host: "localhost".to_string(),
            port: 1521,
            service_name: "ORCLCDB".to_string(),
            user: "cdc_user".to_string(),
            password: "cdc_pass".to_string(),
            schema: "CDC_SCHEMA".to_string(),
            timeout_secs: 30,
            batch_size: 1000,
        };

        assert!(config.validate().is_ok());

        // Empty host should fail
        let config_empty = OracleSinkConfig {
            host: String::new(),
            ..Default::default()
        };
        assert!(config_empty.validate().is_err());
    }

    #[test]
    fn test_default_values() {
        let config = OracleSinkConfig::default();
        assert_eq!(config.port, 1521);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.batch_size, 1000);
    }
}
