// Copyright 2025
// Licensed under the Elastic License v2.0

use anyhow::{Context, Result};
use log::warn;
use std::env;

// =============================================================================
// Source Configuration
// =============================================================================

/// Supported source database types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceType {
    Postgres,
    // Future: Mysql, Oracle, etc.
}

impl SourceType {
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" => Ok(SourceType::Postgres),
            other => anyhow::bail!("Unsupported source type: '{}'. Supported: postgres", other),
        }
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::Postgres => write!(f, "postgres"),
        }
    }
}

/// PostgreSQL-specific source configuration
#[derive(Debug, Clone)]
pub struct PostgresSourceConfig {
    pub slot_name: String,
    #[allow(dead_code)]
    pub publication_name: String,
}

/// Generic source configuration
#[derive(Debug, Clone)]
pub struct SourceConfig {
    pub source_type: SourceType,
    #[allow(dead_code)]
    pub url: String,
    #[allow(dead_code)]
    pub tables: Vec<String>,
    pub postgres: Option<PostgresSourceConfig>,
}

// =============================================================================
// Sink Configuration
// =============================================================================

/// Supported sink database types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkType {
    StarRocks,
    // Future: ClickHouse, Snowflake, etc.
}

impl SinkType {
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "starrocks" => Ok(SinkType::StarRocks),
            other => anyhow::bail!("Unsupported sink type: '{}'. Supported: starrocks", other),
        }
    }
}

impl std::fmt::Display for SinkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkType::StarRocks => write!(f, "starrocks"),
        }
    }
}

/// StarRocks-specific sink configuration
#[derive(Debug, Clone)]
pub struct StarRocksSinkConfig {
    // StarRocks-specific options can be added here
    // e.g., stream_load_url, timeout settings, etc.
}

/// Generic sink configuration
#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub sink_type: SinkType,
    pub url: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    #[allow(dead_code)]
    pub starrocks: Option<StarRocksSinkConfig>,
}

// =============================================================================
// Main Configuration
// =============================================================================

/// Central configuration for dbmazz loaded from environment variables
///
/// This struct maintains backward compatibility with the original flat structure
/// while also providing new nested `source` and `sink` configurations.
///
/// **New code** should use the nested `source` and `sink` fields.
/// **Legacy fields** are kept for backward compatibility and will be removed in a future version.
#[derive(Debug, Clone)]
pub struct Config {
    // =========================================================================
    // New nested configuration (preferred)
    // =========================================================================
    pub source: SourceConfig,
    pub sink: SinkConfig,

    // =========================================================================
    // Legacy fields (kept for backward compatibility)
    // These mirror the nested config values and will be removed in v0.3.0
    // =========================================================================

    // PostgreSQL (legacy)
    pub database_url: String,
    pub slot_name: String,
    pub publication_name: String,
    pub tables: Vec<String>,

    // StarRocks (legacy)
    pub starrocks_url: String,
    pub starrocks_port: u16,
    pub starrocks_db: String,
    pub starrocks_user: String,
    pub starrocks_pass: String,

    // Pipeline
    pub flush_size: usize,
    pub flush_interval_ms: u64,

    // gRPC
    pub grpc_port: u16,
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Read environment variable with fallback to legacy name, logging deprecation warning
fn env_with_fallback(new_name: &str, legacy_name: &str) -> Option<String> {
    if let Ok(value) = env::var(new_name) {
        return Some(value);
    }

    if let Ok(value) = env::var(legacy_name) {
        warn!(
            "Environment variable '{}' is deprecated, use '{}' instead",
            legacy_name, new_name
        );
        return Some(value);
    }

    None
}

/// Read required environment variable with fallback
fn required_env_with_fallback(new_name: &str, legacy_name: &str) -> Result<String> {
    env_with_fallback(new_name, legacy_name)
        .with_context(|| format!("{} (or legacy {}) must be set", new_name, legacy_name))
}

/// Read optional environment variable with fallback and default
fn optional_env_with_fallback(new_name: &str, legacy_name: &str, default: &str) -> String {
    env_with_fallback(new_name, legacy_name).unwrap_or_else(|| default.to_string())
}

// =============================================================================
// Config Implementation
// =============================================================================

impl Config {
    /// Load configuration from environment variables
    ///
    /// Supports both new generic variable names and legacy names for backward compatibility.
    /// When legacy names are used, a deprecation warning is logged.
    ///
    /// # New Variables (preferred)
    /// - SOURCE_URL, SOURCE_TYPE, SOURCE_SLOT_NAME, SOURCE_PUBLICATION_NAME
    /// - SINK_URL, SINK_TYPE, SINK_PORT, SINK_DATABASE, SINK_USER, SINK_PASSWORD
    ///
    /// # Legacy Variables (deprecated)
    /// - DATABASE_URL, SLOT_NAME, PUBLICATION_NAME
    /// - STARROCKS_URL, STARROCKS_PORT, STARROCKS_DB, STARROCKS_USER, STARROCKS_PASS
    pub fn from_env() -> Result<Self> {
        // Source configuration
        let source_type_str = env::var("SOURCE_TYPE").unwrap_or_else(|_| "postgres".to_string());
        let source_type = SourceType::from_str(&source_type_str)?;

        let source_url = required_env_with_fallback("SOURCE_URL", "DATABASE_URL")?;

        let tables: Vec<String> = env::var("TABLES")
            .unwrap_or_else(|_| "orders,order_items".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Source-specific config (Postgres)
        let slot_name =
            optional_env_with_fallback("SOURCE_SLOT_NAME", "SLOT_NAME", "dbmazz_slot");
        let publication_name =
            optional_env_with_fallback("SOURCE_PUBLICATION_NAME", "PUBLICATION_NAME", "dbmazz_pub");

        let postgres_config = match source_type {
            SourceType::Postgres => Some(PostgresSourceConfig {
                slot_name: slot_name.clone(),
                publication_name: publication_name.clone(),
            }),
        };

        let source = SourceConfig {
            source_type,
            url: source_url.clone(),
            tables: tables.clone(),
            postgres: postgres_config,
        };

        // Sink configuration
        let sink_type_str = env::var("SINK_TYPE").unwrap_or_else(|_| "starrocks".to_string());
        let sink_type = SinkType::from_str(&sink_type_str)?;

        let sink_url = required_env_with_fallback("SINK_URL", "STARROCKS_URL")?;

        let sink_port: u16 = optional_env_with_fallback("SINK_PORT", "STARROCKS_PORT", "9030")
            .parse()
            .unwrap_or(9030);

        let sink_database = required_env_with_fallback("SINK_DATABASE", "STARROCKS_DB")?;

        let sink_user = optional_env_with_fallback("SINK_USER", "STARROCKS_USER", "root");

        let sink_password = optional_env_with_fallback("SINK_PASSWORD", "STARROCKS_PASS", "");

        // Build sink-specific config
        let starrocks_config = match sink_type {
            SinkType::StarRocks => Some(StarRocksSinkConfig {}),
        };

        let sink = SinkConfig {
            sink_type,
            url: sink_url.clone(),
            port: sink_port,
            database: sink_database.clone(),
            user: sink_user.clone(),
            password: sink_password.clone(),
            starrocks: starrocks_config,
        };

        // Pipeline configuration
        let flush_size: usize = env::var("FLUSH_SIZE")
            .unwrap_or_else(|_| "10000".to_string())
            .parse()
            .unwrap_or(10000);

        let flush_interval_ms: u64 = env::var("FLUSH_INTERVAL_MS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000);

        // gRPC configuration
        let grpc_port: u16 = env::var("GRPC_PORT")
            .unwrap_or_else(|_| "50051".to_string())
            .parse()
            .unwrap_or(50051);

        Ok(Self {
            // New nested config
            source,
            sink,

            // Legacy fields (mirroring nested values for backward compatibility)
            database_url: source_url,
            slot_name,
            publication_name,
            tables,
            starrocks_url: sink_url,
            starrocks_port: sink_port,
            starrocks_db: sink_database,
            starrocks_user: sink_user,
            starrocks_pass: sink_password,

            // Common fields
            flush_size,
            flush_interval_ms,
            grpc_port,
        })
    }

    /// Print banner with configuration
    pub fn print_banner(&self) {
        println!("Starting dbmazz (High Performance Mode)...");

        // Source info
        match &self.source.source_type {
            SourceType::Postgres => {
                if let Some(pg) = &self.source.postgres {
                    println!("Source: Postgres (slot: {})", pg.slot_name);
                } else {
                    println!("Source: Postgres");
                }
            }
        }

        // Sink info
        match &self.sink.sink_type {
            SinkType::StarRocks => {
                println!("Sink: StarRocks (db: {})", self.sink.database);
            }
        }

        println!(
            "Flush: {} msgs or {}ms interval",
            self.flush_size, self.flush_interval_ms
        );
        println!("gRPC: port {}", self.grpc_port);
        println!("Tables: {:?}", self.tables);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;

    fn clear_env_vars() {
        // Clear new variables
        env::remove_var("SOURCE_URL");
        env::remove_var("SOURCE_TYPE");
        env::remove_var("SOURCE_SLOT_NAME");
        env::remove_var("SOURCE_PUBLICATION_NAME");
        env::remove_var("SINK_URL");
        env::remove_var("SINK_TYPE");
        env::remove_var("SINK_PORT");
        env::remove_var("SINK_DATABASE");
        env::remove_var("SINK_USER");
        env::remove_var("SINK_PASSWORD");

        // Clear legacy variables
        env::remove_var("DATABASE_URL");
        env::remove_var("SLOT_NAME");
        env::remove_var("PUBLICATION_NAME");
        env::remove_var("STARROCKS_URL");
        env::remove_var("STARROCKS_PORT");
        env::remove_var("STARROCKS_DB");
        env::remove_var("STARROCKS_USER");
        env::remove_var("STARROCKS_PASS");

        // Clear common variables
        env::remove_var("TABLES");
        env::remove_var("FLUSH_SIZE");
        env::remove_var("FLUSH_INTERVAL_MS");
        env::remove_var("GRPC_PORT");

    }

    #[test]
    #[serial]
    fn test_new_env_vars() {
        clear_env_vars();

        env::set_var("SOURCE_URL", "postgres://localhost/testdb");
        env::set_var("SOURCE_TYPE", "postgres");
        env::set_var("SOURCE_SLOT_NAME", "test_slot");
        env::set_var("SOURCE_PUBLICATION_NAME", "test_pub");
        env::set_var("SINK_URL", "starrocks.local");
        env::set_var("SINK_TYPE", "starrocks");
        env::set_var("SINK_PORT", "9030");
        env::set_var("SINK_DATABASE", "testdb");
        env::set_var("SINK_USER", "admin");
        env::set_var("SINK_PASSWORD", "secret");
        env::set_var("TABLES", "table1,table2");

        let config = Config::from_env().unwrap();

        // Test new nested structure
        assert_eq!(config.source.url, "postgres://localhost/testdb");
        assert_eq!(config.source.source_type, SourceType::Postgres);
        assert_eq!(
            config.source.postgres.as_ref().unwrap().slot_name,
            "test_slot"
        );
        assert_eq!(
            config.source.postgres.as_ref().unwrap().publication_name,
            "test_pub"
        );
        assert_eq!(config.sink.url, "starrocks.local");
        assert_eq!(config.sink.sink_type, SinkType::StarRocks);
        assert_eq!(config.sink.port, 9030);
        assert_eq!(config.sink.database, "testdb");
        assert_eq!(config.sink.user, "admin");
        assert_eq!(config.sink.password, "secret");

        // Test legacy fields mirror the values
        assert_eq!(config.database_url, "postgres://localhost/testdb");
        assert_eq!(config.slot_name, "test_slot");
        assert_eq!(config.publication_name, "test_pub");
        assert_eq!(config.starrocks_url, "starrocks.local");
        assert_eq!(config.starrocks_port, 9030);
        assert_eq!(config.starrocks_db, "testdb");
        assert_eq!(config.starrocks_user, "admin");
        assert_eq!(config.starrocks_pass, "secret");
        assert_eq!(config.tables, vec!["table1", "table2"]);

        clear_env_vars();
    }

    #[test]
    #[serial]
    fn test_legacy_env_vars_fallback() {
        clear_env_vars();

        // Use legacy variable names
        env::set_var("DATABASE_URL", "postgres://legacy/db");
        env::set_var("SLOT_NAME", "legacy_slot");
        env::set_var("PUBLICATION_NAME", "legacy_pub");
        env::set_var("STARROCKS_URL", "legacy.starrocks");
        env::set_var("STARROCKS_PORT", "9031");
        env::set_var("STARROCKS_DB", "legacydb");
        env::set_var("STARROCKS_USER", "legacyuser");
        env::set_var("STARROCKS_PASS", "legacypass");

        let config = Config::from_env().unwrap();

        // Both nested and legacy should have the values
        assert_eq!(config.source.url, "postgres://legacy/db");
        assert_eq!(config.database_url, "postgres://legacy/db");
        assert_eq!(
            config.source.postgres.as_ref().unwrap().slot_name,
            "legacy_slot"
        );
        assert_eq!(config.slot_name, "legacy_slot");
        assert_eq!(
            config.source.postgres.as_ref().unwrap().publication_name,
            "legacy_pub"
        );
        assert_eq!(config.publication_name, "legacy_pub");
        assert_eq!(config.sink.url, "legacy.starrocks");
        assert_eq!(config.starrocks_url, "legacy.starrocks");
        assert_eq!(config.sink.port, 9031);
        assert_eq!(config.starrocks_port, 9031);
        assert_eq!(config.sink.database, "legacydb");
        assert_eq!(config.starrocks_db, "legacydb");
        assert_eq!(config.sink.user, "legacyuser");
        assert_eq!(config.starrocks_user, "legacyuser");
        assert_eq!(config.sink.password, "legacypass");
        assert_eq!(config.starrocks_pass, "legacypass");

        clear_env_vars();
    }

    #[test]
    #[serial]
    fn test_new_vars_take_precedence() {
        clear_env_vars();

        // Set both new and legacy - new should win
        env::set_var("SOURCE_URL", "postgres://new/db");
        env::set_var("DATABASE_URL", "postgres://legacy/db");
        env::set_var("SINK_URL", "new.starrocks");
        env::set_var("STARROCKS_URL", "legacy.starrocks");
        env::set_var("SINK_DATABASE", "newdb");
        env::set_var("STARROCKS_DB", "legacydb");

        let config = Config::from_env().unwrap();

        assert_eq!(config.source.url, "postgres://new/db");
        assert_eq!(config.database_url, "postgres://new/db");
        assert_eq!(config.sink.url, "new.starrocks");
        assert_eq!(config.starrocks_url, "new.starrocks");
        assert_eq!(config.sink.database, "newdb");
        assert_eq!(config.starrocks_db, "newdb");

        clear_env_vars();
    }

    #[test]
    #[serial]
    fn test_defaults() {
        clear_env_vars();

        env::set_var("SOURCE_URL", "postgres://localhost/db");
        env::set_var("SINK_URL", "starrocks.local");
        env::set_var("SINK_DATABASE", "mydb");

        let config = Config::from_env().unwrap();

        // Check defaults
        assert_eq!(config.source.source_type, SourceType::Postgres);
        assert_eq!(config.sink.sink_type, SinkType::StarRocks);
        assert_eq!(
            config.source.postgres.as_ref().unwrap().slot_name,
            "dbmazz_slot"
        );
        assert_eq!(config.slot_name, "dbmazz_slot");
        assert_eq!(
            config.source.postgres.as_ref().unwrap().publication_name,
            "dbmazz_pub"
        );
        assert_eq!(config.publication_name, "dbmazz_pub");
        assert_eq!(config.sink.port, 9030);
        assert_eq!(config.starrocks_port, 9030);
        assert_eq!(config.sink.user, "root");
        assert_eq!(config.starrocks_user, "root");
        assert_eq!(config.sink.password, "");
        assert_eq!(config.starrocks_pass, "");
        assert_eq!(config.flush_size, 10000);
        assert_eq!(config.flush_interval_ms, 5000);
        assert_eq!(config.grpc_port, 50051);

        clear_env_vars();
    }

    #[test]
    fn test_source_type_parsing() {
        assert_eq!(
            SourceType::from_str("postgres").unwrap(),
            SourceType::Postgres
        );
        assert_eq!(
            SourceType::from_str("postgresql").unwrap(),
            SourceType::Postgres
        );
        assert_eq!(
            SourceType::from_str("POSTGRES").unwrap(),
            SourceType::Postgres
        );
        assert!(SourceType::from_str("mysql").is_err());
    }

    #[test]
    fn test_sink_type_parsing() {
        assert_eq!(
            SinkType::from_str("starrocks").unwrap(),
            SinkType::StarRocks
        );
        assert_eq!(
            SinkType::from_str("STARROCKS").unwrap(),
            SinkType::StarRocks
        );
        assert!(SinkType::from_str("clickhouse").is_err());
    }

    #[test]
    #[serial]
    fn test_tables_parsing() {
        clear_env_vars();

        env::set_var("SOURCE_URL", "postgres://localhost/db");
        env::set_var("SINK_URL", "starrocks.local");
        env::set_var("SINK_DATABASE", "mydb");
        env::set_var("TABLES", "  table1 , table2 ,table3  ");

        let config = Config::from_env().unwrap();

        assert_eq!(config.tables, vec!["table1", "table2", "table3"]);

        clear_env_vars();
    }

    #[test]
    #[serial]
    fn test_missing_required_vars() {
        clear_env_vars();

        // Missing source URL
        env::set_var("SINK_URL", "starrocks.local");
        env::set_var("SINK_DATABASE", "mydb");
        assert!(Config::from_env().is_err());

        clear_env_vars();

        // Missing sink URL
        env::set_var("SOURCE_URL", "postgres://localhost/db");
        env::set_var("SINK_DATABASE", "mydb");
        assert!(Config::from_env().is_err());

        clear_env_vars();

        // Missing sink database
        env::set_var("SOURCE_URL", "postgres://localhost/db");
        env::set_var("SINK_URL", "starrocks.local");
        assert!(Config::from_env().is_err());

        clear_env_vars();
    }

    #[test]
    #[serial]
    fn test_backward_compatibility_with_original_api() {
        clear_env_vars();

        // Simulate original environment setup
        env::set_var("DATABASE_URL", "postgres://localhost/db");
        env::set_var("SLOT_NAME", "my_slot");
        env::set_var("PUBLICATION_NAME", "my_pub");
        env::set_var("STARROCKS_URL", "starrocks.local");
        env::set_var("STARROCKS_PORT", "9030");
        env::set_var("STARROCKS_DB", "mydb");
        env::set_var("STARROCKS_USER", "myuser");
        env::set_var("STARROCKS_PASS", "mypass");
        env::set_var("TABLES", "orders,items");
        env::set_var("FLUSH_SIZE", "5000");
        env::set_var("FLUSH_INTERVAL_MS", "3000");
        env::set_var("GRPC_PORT", "50052");

        let config = Config::from_env().unwrap();

        // All legacy field access should work exactly as before
        assert_eq!(config.database_url, "postgres://localhost/db");
        assert_eq!(config.slot_name, "my_slot");
        assert_eq!(config.publication_name, "my_pub");
        assert_eq!(config.starrocks_url, "starrocks.local");
        assert_eq!(config.starrocks_port, 9030);
        assert_eq!(config.starrocks_db, "mydb");
        assert_eq!(config.starrocks_user, "myuser");
        assert_eq!(config.starrocks_pass, "mypass");
        assert_eq!(config.tables, vec!["orders", "items"]);
        assert_eq!(config.flush_size, 5000);
        assert_eq!(config.flush_interval_ms, 3000);
        assert_eq!(config.grpc_port, 50052);

        clear_env_vars();
    }
}
