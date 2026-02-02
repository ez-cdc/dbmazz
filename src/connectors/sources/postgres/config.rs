// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL source configuration and validation
//!
//! This module provides configuration structures and validation logic
//! for PostgreSQL CDC sources.

use anyhow::{bail, Result};
use url::Url;

/// PostgreSQL-specific source configuration
///
/// This configuration is used alongside the generic SourceConfig
/// to provide PostgreSQL-specific settings.
#[derive(Debug, Clone)]
pub struct PostgresSourceConfig {
    /// Replication slot name
    pub slot_name: String,

    /// Publication name containing tables to replicate
    pub publication_name: String,

    /// Optional: Starting LSN for resumption
    pub start_lsn: Option<u64>,

    /// Optional: Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: Option<u64>,

    /// Optional: Maximum batch size for events
    pub max_batch_size: Option<usize>,
}

impl PostgresSourceConfig {
    /// Create a new PostgresSourceConfig with required fields
    pub fn new(slot_name: String, publication_name: String) -> Self {
        Self {
            slot_name,
            publication_name,
            start_lsn: None,
            heartbeat_interval_ms: None,
            max_batch_size: None,
        }
    }

    /// Set the starting LSN
    pub fn with_start_lsn(mut self, lsn: u64) -> Self {
        self.start_lsn = Some(lsn);
        self
    }

    /// Set the heartbeat interval
    pub fn with_heartbeat_interval(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = Some(ms);
        self
    }

    /// Set the maximum batch size
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = Some(size);
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        // Validate slot name
        if self.slot_name.is_empty() {
            bail!("Replication slot name cannot be empty");
        }

        if self.slot_name.len() > 63 {
            bail!("Replication slot name cannot exceed 63 characters");
        }

        // PostgreSQL identifier rules: lowercase letters, digits, underscores
        if !self
            .slot_name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            bail!(
                "Replication slot name must contain only lowercase letters, digits, and underscores"
            );
        }

        // Validate publication name
        if self.publication_name.is_empty() {
            bail!("Publication name cannot be empty");
        }

        if self.publication_name.len() > 63 {
            bail!("Publication name cannot exceed 63 characters");
        }

        Ok(())
    }
}

/// Validate a PostgreSQL connection URL
pub fn validate_postgres_url(url: &str) -> Result<()> {
    let parsed = Url::parse(url).map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;

    match parsed.scheme() {
        "postgres" | "postgresql" => {}
        other => bail!("Invalid scheme '{}', expected 'postgres' or 'postgresql'", other),
    }

    if parsed.host_str().is_none() {
        bail!("URL must include a host");
    }

    // Warn about missing database
    if parsed.path().is_empty() || parsed.path() == "/" {
        eprintln!("WARNING: No database specified in URL, will use default database");
    }

    Ok(())
}

/// Parse an LSN string in PostgreSQL format (X/Y) to u64
pub fn parse_lsn(lsn_str: &str) -> Result<u64> {
    let parts: Vec<&str> = lsn_str.split('/').collect();
    if parts.len() != 2 {
        bail!("Invalid LSN format: expected X/Y, got '{}'", lsn_str);
    }

    let high =
        u64::from_str_radix(parts[0], 16).map_err(|e| anyhow::anyhow!("Invalid LSN high: {}", e))?;
    let low =
        u64::from_str_radix(parts[1], 16).map_err(|e| anyhow::anyhow!("Invalid LSN low: {}", e))?;

    Ok((high << 32) | low)
}

/// Format a u64 LSN to PostgreSQL format (X/Y)
pub fn format_lsn(lsn: u64) -> String {
    if lsn == 0 {
        "0/0".to_string()
    } else {
        format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        // Valid config
        let config = PostgresSourceConfig::new("my_slot".to_string(), "my_pub".to_string());
        assert!(config.validate().is_ok());

        // Empty slot name
        let config = PostgresSourceConfig::new("".to_string(), "my_pub".to_string());
        assert!(config.validate().is_err());

        // Invalid characters in slot name
        let config = PostgresSourceConfig::new("My-Slot".to_string(), "my_pub".to_string());
        assert!(config.validate().is_err());

        // Too long slot name
        let long_name = "a".repeat(64);
        let config = PostgresSourceConfig::new(long_name, "my_pub".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_url_validation() {
        // Valid URLs
        assert!(validate_postgres_url("postgres://localhost/mydb").is_ok());
        assert!(validate_postgres_url("postgresql://user:pass@host:5432/db").is_ok());

        // Invalid scheme
        assert!(validate_postgres_url("mysql://localhost/db").is_err());

        // Missing host
        assert!(validate_postgres_url("postgres:///db").is_err());

        // Invalid URL
        assert!(validate_postgres_url("not a url").is_err());
    }

    #[test]
    fn test_lsn_parsing() {
        // Standard format
        assert_eq!(parse_lsn("0/0").unwrap(), 0);
        assert_eq!(parse_lsn("0/1").unwrap(), 1);
        assert_eq!(parse_lsn("1/0").unwrap(), 0x100000000);
        assert_eq!(parse_lsn("16/B374D848").unwrap(), 0x16B374D848);

        // Invalid format
        assert!(parse_lsn("invalid").is_err());
        assert!(parse_lsn("0").is_err());
        assert!(parse_lsn("0/0/0").is_err());
    }

    #[test]
    fn test_lsn_formatting() {
        assert_eq!(format_lsn(0), "0/0");
        assert_eq!(format_lsn(1), "0/1");
        assert_eq!(format_lsn(0x100000000), "1/0");
        assert_eq!(format_lsn(0x16B374D848), "16/B374D848");
    }

    #[test]
    fn test_lsn_roundtrip() {
        let test_values = [0u64, 1, 1000, 0x100000000, 0x16B374D848, u64::MAX >> 1];

        for val in test_values {
            let formatted = format_lsn(val);
            let parsed = parse_lsn(&formatted).unwrap();
            assert_eq!(val, parsed, "Roundtrip failed for {}", val);
        }
    }
}
