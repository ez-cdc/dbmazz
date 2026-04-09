// Copyright 2025
// Licensed under the Elastic License v2.0

//! Snowflake Sink Configuration
//!
//! Handles configuration parsing and validation for the Snowflake sink connector.
//! Supports key-pair JWT auth (preferred) and username/password fallback.

use anyhow::{anyhow, Result};

use crate::config::SinkConfig;

/// Default MERGE interval in milliseconds
const DEFAULT_MERGE_INTERVAL_MS: u64 = 30_000;

/// Snowflake-specific sink configuration.
#[derive(Clone)]
pub struct SnowflakeSinkConfig {
    /// Snowflake account identifier (e.g., "xy12345.us-east-1")
    pub account: String,

    /// Account URL (e.g., "https://xy12345.us-east-1.snowflakecomputing.com")
    pub account_url: String,

    /// Warehouse for COPY/MERGE compute
    pub warehouse: String,

    /// Role to USE ROLE (optional)
    pub role: String,

    /// Target database
    pub database: String,

    /// Target schema for final tables (default: "PUBLIC")
    pub schema: String,

    /// Snowflake username
    pub user: String,

    /// Password (if not using key-pair)
    pub password: String,

    /// Path to RSA private key (.p8) for JWT auth
    pub private_key_path: String,

    /// Passphrase for encrypted private key
    pub private_key_passphrase: String,

    /// Job name for raw table and metadata tracking
    pub job_name: String,

    /// MERGE frequency in milliseconds
    pub merge_interval_ms: u64,

    /// Soft delete mode (true = soft delete, false = hard DELETE)
    pub soft_delete: bool,
}

impl std::fmt::Debug for SnowflakeSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeSinkConfig")
            .field("account", &self.account)
            .field("account_url", &self.account_url)
            .field("warehouse", &self.warehouse)
            .field("role", &self.role)
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field(
                "private_key_path",
                &if self.private_key_path.is_empty() {
                    "(none)"
                } else {
                    &self.private_key_path
                },
            )
            .field("job_name", &self.job_name)
            .field("merge_interval_ms", &self.merge_interval_ms)
            .field("soft_delete", &self.soft_delete)
            .finish()
    }
}

impl SnowflakeSinkConfig {
    /// Creates a Snowflake configuration from the generic SinkConfig + env vars.
    pub fn from_sink_config(config: &SinkConfig) -> Result<Self> {
        let account = std::env::var("SINK_SNOWFLAKE_ACCOUNT")
            .map_err(|_| anyhow!("SINK_SNOWFLAKE_ACCOUNT must be set"))?;

        let warehouse = std::env::var("SINK_SNOWFLAKE_WAREHOUSE")
            .map_err(|_| anyhow!("SINK_SNOWFLAKE_WAREHOUSE must be set"))?;

        let role = std::env::var("SINK_SNOWFLAKE_ROLE").unwrap_or_default();

        let private_key_path = std::env::var("SINK_SNOWFLAKE_PRIVATE_KEY_PATH").unwrap_or_default();
        let private_key_passphrase =
            std::env::var("SINK_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").unwrap_or_default();

        let merge_interval_ms: u64 = std::env::var("SINK_SNOWFLAKE_MERGE_INTERVAL_MS")
            .unwrap_or_else(|_| DEFAULT_MERGE_INTERVAL_MS.to_string())
            .parse()
            .unwrap_or(DEFAULT_MERGE_INTERVAL_MS);

        let soft_delete = std::env::var("SINK_SNOWFLAKE_SOFT_DELETE")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == "true";

        let schema = std::env::var("SINK_SCHEMA").unwrap_or_else(|_| "PUBLIC".to_string());

        let job_name =
            std::env::var("SOURCE_SLOT_NAME").unwrap_or_else(|_| "dbmazz_slot".to_string());

        // Build account URL from SINK_URL or account
        let account_url = Self::normalize_account_url(&config.url, &account)?;

        Ok(Self {
            account,
            account_url,
            warehouse,
            role,
            database: config.database.clone(),
            schema,
            user: config.user.clone(),
            password: config.password.clone(),
            private_key_path,
            private_key_passphrase,
            job_name,
            merge_interval_ms,
            soft_delete,
        })
    }

    /// Returns true if key-pair JWT auth should be used.
    pub fn use_jwt_auth(&self) -> bool {
        !self.private_key_path.is_empty()
    }

    /// Normalizes the account URL to https://<account>.snowflakecomputing.com
    fn normalize_account_url(url: &str, account: &str) -> Result<String> {
        let url = url.trim();

        if url.starts_with("https://") && url.contains("snowflakecomputing.com") {
            return Ok(url.to_string());
        }

        if !url.is_empty() && url.contains("snowflakecomputing.com") {
            return Ok(format!("https://{}", url));
        }

        // Build from account identifier
        Ok(format!("https://{}.snowflakecomputing.com", account))
    }

    #[allow(dead_code)]
    pub fn validate(&self) -> Result<()> {
        if self.account.is_empty() {
            return Err(anyhow!("Snowflake account is required"));
        }
        if self.warehouse.is_empty() {
            return Err(anyhow!("Snowflake warehouse is required"));
        }
        if self.database.is_empty() {
            return Err(anyhow!("Database name is required"));
        }
        if self.user.is_empty() {
            return Err(anyhow!("Username is required"));
        }
        if !self.use_jwt_auth() && self.password.is_empty() {
            return Err(anyhow!(
                "Either SINK_SNOWFLAKE_PRIVATE_KEY_PATH or SINK_PASSWORD must be set"
            ));
        }
        Ok(())
    }

    /// Returns the sanitized job name for use in SQL identifiers.
    /// Uppercased to match Snowflake's unquoted identifier convention and to
    /// stay consistent with the JOB_NAME VARCHAR value stored in _METADATA
    /// (which is case-sensitive when compared in WHERE clauses).
    pub fn safe_job_name(&self) -> String {
        self.job_name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>()
            .to_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_account_url() {
        assert_eq!(
            SnowflakeSinkConfig::normalize_account_url(
                "https://xy12345.us-east-1.snowflakecomputing.com",
                "xy12345.us-east-1"
            )
            .unwrap(),
            "https://xy12345.us-east-1.snowflakecomputing.com"
        );

        assert_eq!(
            SnowflakeSinkConfig::normalize_account_url(
                "xy12345.us-east-1.snowflakecomputing.com",
                "xy12345.us-east-1"
            )
            .unwrap(),
            "https://xy12345.us-east-1.snowflakecomputing.com"
        );

        assert_eq!(
            SnowflakeSinkConfig::normalize_account_url("", "xy12345.us-east-1").unwrap(),
            "https://xy12345.us-east-1.snowflakecomputing.com"
        );
    }

    #[test]
    fn test_use_jwt_auth() {
        let config = SnowflakeSinkConfig {
            account: "test".to_string(),
            account_url: "https://test.snowflakecomputing.com".to_string(),
            warehouse: "COMPUTE_WH".to_string(),
            role: String::new(),
            database: "test_db".to_string(),
            schema: "PUBLIC".to_string(),
            user: "user".to_string(),
            password: "pass".to_string(),
            private_key_path: String::new(),
            private_key_passphrase: String::new(),
            job_name: "test_slot".to_string(),
            merge_interval_ms: 30_000,
            soft_delete: true,
        };

        assert!(!config.use_jwt_auth());

        let config_jwt = SnowflakeSinkConfig {
            private_key_path: "/path/to/key.p8".to_string(),
            ..config
        };
        assert!(config_jwt.use_jwt_auth());
    }

    #[test]
    fn test_safe_job_name() {
        let config = SnowflakeSinkConfig {
            account: "test".to_string(),
            account_url: "https://test.snowflakecomputing.com".to_string(),
            warehouse: "WH".to_string(),
            role: String::new(),
            database: "db".to_string(),
            schema: "PUBLIC".to_string(),
            user: "user".to_string(),
            password: "pass".to_string(),
            private_key_path: String::new(),
            private_key_passphrase: String::new(),
            job_name: "my-slot.name".to_string(),
            merge_interval_ms: 30_000,
            soft_delete: true,
        };

        assert_eq!(config.safe_job_name(), "MY_SLOT_NAME");
    }
}
