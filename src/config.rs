// Copyright 2025
// Licensed under the Elastic License v2.0

use anyhow::{Context, Result};
use std::env;
use tracing::{info, warn};

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
#[derive(Clone)]
pub struct SourceConfig {
    pub source_type: SourceType,
    #[allow(dead_code)]
    pub url: String,
    #[allow(dead_code)]
    pub tables: Vec<String>,
    pub postgres: Option<PostgresSourceConfig>,
}

impl std::fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Extract password from URL if present and redact it
        let redacted_url = if self.url.contains("://") {
            // Pattern: scheme://user:password@host:port/db
            if let Some(at_pos) = self.url.rfind('@') {
                if let Some(scheme_end) = self.url.find("://") {
                    let scheme_part = &self.url[..=scheme_end + 2];
                    let after_at = &self.url[at_pos..];

                    // Check if there's a colon between scheme and @
                    let between = &self.url[scheme_end + 3..at_pos];
                    if between.contains(':') {
                        format!("{}[REDACTED]{}", scheme_part, after_at)
                    } else {
                        self.url.clone()
                    }
                } else {
                    self.url.clone()
                }
            } else {
                self.url.clone()
            }
        } else {
            self.url.clone()
        };

        f.debug_struct("SourceConfig")
            .field("source_type", &self.source_type)
            .field("url", &redacted_url)
            .field("tables", &self.tables)
            .field("postgres", &self.postgres)
            .finish()
    }
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
#[derive(Clone)]
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

impl std::fmt::Debug for SinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkConfig")
            .field("sink_type", &self.sink_type)
            .field("url", &self.url)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("starrocks", &self.starrocks)
            .finish()
    }
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
#[derive(Clone)]
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

    // Snapshot / backfill
    pub do_snapshot: bool,
    pub snapshot_chunk_size: u64,
    pub snapshot_parallel_workers: u32,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact password from database_url
        let redacted_db_url = if self.database_url.contains("://") {
            if let Some(at_pos) = self.database_url.rfind('@') {
                if let Some(scheme_end) = self.database_url.find("://") {
                    let scheme_part = &self.database_url[..=scheme_end + 2];
                    let after_at = &self.database_url[at_pos..];
                    let between = &self.database_url[scheme_end + 3..at_pos];
                    if between.contains(':') {
                        format!("{}[REDACTED]{}", scheme_part, after_at)
                    } else {
                        self.database_url.clone()
                    }
                } else {
                    self.database_url.clone()
                }
            } else {
                self.database_url.clone()
            }
        } else {
            self.database_url.clone()
        };

        f.debug_struct("Config")
            .field("source", &self.source)
            .field("sink", &self.sink)
            .field("database_url", &redacted_db_url)
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("tables", &self.tables)
            .field("starrocks_url", &self.starrocks_url)
            .field("starrocks_port", &self.starrocks_port)
            .field("starrocks_db", &self.starrocks_db)
            .field("starrocks_user", &self.starrocks_user)
            .field("starrocks_pass", &"[REDACTED]")
            .field("flush_size", &self.flush_size)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("grpc_port", &self.grpc_port)
            .finish()
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Read environment variable with fallback to legacy name, logging deprecation warning
fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{} must be set", name))
}

fn optional_env(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

// =============================================================================
// Config Implementation
// =============================================================================

impl Config {
    /// Load configuration from environment variables
    ///
    /// # Variables
    /// - SOURCE_URL, SOURCE_TYPE, SOURCE_SLOT_NAME, SOURCE_PUBLICATION_NAME
    /// - SINK_URL, SINK_TYPE, SINK_PORT, SINK_DATABASE, SINK_USER, SINK_PASSWORD
    pub fn from_env() -> Result<Self> {
        // Source configuration
        let source_type_str = env::var("SOURCE_TYPE").unwrap_or_else(|_| "postgres".to_string());
        let source_type = SourceType::from_str(&source_type_str)?;

        let source_url = required_env("SOURCE_URL")?;

        let tables: Vec<String> = env::var("TABLES")
            .unwrap_or_else(|_| "orders,order_items".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Source-specific config (Postgres)
        let slot_name = optional_env("SOURCE_SLOT_NAME", "dbmazz_slot");
        let publication_name = optional_env("SOURCE_PUBLICATION_NAME", "dbmazz_pub");

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

        let sink_url = required_env("SINK_URL")?;

        let sink_port: u16 = optional_env("SINK_PORT", "9030")
            .parse()
            .unwrap_or(9030);

        let sink_database = required_env("SINK_DATABASE")?;

        let sink_user = optional_env("SINK_USER", "root");

        let sink_password = optional_env("SINK_PASSWORD", "");

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

        // Snapshot / backfill configuration
        let do_snapshot = env::var("DO_SNAPSHOT")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase() == "true";

        let snapshot_chunk_size: u64 = env::var("SNAPSHOT_CHUNK_SIZE")
            .unwrap_or_else(|_| "500000".to_string())
            .parse()
            .unwrap_or(500_000);

        let snapshot_parallel_workers: u32 = env::var("SNAPSHOT_PARALLEL_WORKERS")
            .unwrap_or_else(|_| "2".to_string())
            .parse()
            .unwrap_or(2);

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

            // Snapshot
            do_snapshot,
            snapshot_chunk_size,
            snapshot_parallel_workers,
        })
    }

    /// Print banner with configuration
    pub fn print_banner(&self) {
        info!("Starting dbmazz (High Performance Mode)...");

        // Source info
        match &self.source.source_type {
            SourceType::Postgres => {
                if let Some(pg) = &self.source.postgres {
                    info!("Source: Postgres (slot: {})", pg.slot_name);
                } else {
                    info!("Source: Postgres");
                }
            }
        }

        // Sink info
        match &self.sink.sink_type {
            SinkType::StarRocks => {
                info!("Sink: StarRocks (db: {})", self.sink.database);
            }
        }

        info!(
            "Flush: {} msgs or {}ms interval",
            self.flush_size, self.flush_interval_ms
        );
        info!("gRPC: port {}", self.grpc_port);
        info!("Tables: {:?}", self.tables);
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
    #[serial]
    fn test_backward_compatibility_with_original_api() {
        clear_env_vars();

        // Use current env var names (SOURCE_URL/SINK_URL API)
        env::set_var("SOURCE_URL", "postgres://localhost/db");
        env::set_var("SOURCE_SLOT_NAME", "my_slot");
        env::set_var("SOURCE_PUBLICATION_NAME", "my_pub");
        env::set_var("SINK_URL", "starrocks.local");
        env::set_var("SINK_PORT", "9030");
        env::set_var("SINK_DATABASE", "mydb");
        env::set_var("SINK_USER", "myuser");
        env::set_var("SINK_PASSWORD", "mypass");
        env::set_var("TABLES", "orders,items");
        env::set_var("FLUSH_SIZE", "5000");
        env::set_var("FLUSH_INTERVAL_MS", "3000");
        env::set_var("GRPC_PORT", "50052");

        let config = Config::from_env().unwrap();

        // Verify flat accessors still work
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
