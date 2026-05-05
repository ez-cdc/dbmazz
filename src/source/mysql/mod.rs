// Copyright 2025
// Licensed under the Elastic License v2.0

#![allow(dead_code)]

//! MySQL change data capture source.
//!
//! Provides the `MysqlSource` struct implementing the `Source` trait for MySQL.
//! Reads the MySQL binary log (binlog) using GTID-based replication.
//!
//! All code in this module is behind `#[cfg(feature = "mysql-source")]`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::config::{MysqlSourceConfig, SourceType};
use crate::core::traits::SourceTableSchema;
use crate::core::{ReplicationStream, Source, SourcePosition};

pub mod binlog_stream;
pub mod converter;
pub mod gtid;
pub mod parser;
pub mod schema;
pub mod setup;

/// Parsed MySQL connection details derived from a URL.
///
/// Created from a `mysql://user:password@host:port/db` URL.
/// This struct is private to the mysql source module.
///
/// Note: manual Debug impl redacts the password to prevent credential leaks.
#[derive(Clone)]
struct MysqlConnectionConfig {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    server_id: u32,
    tls_skip_verify: bool,
}

impl std::fmt::Debug for MysqlConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlConnectionConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("server_id", &self.server_id)
            .field("tls_skip_verify", &self.tls_skip_verify)
            .finish()
    }
}

impl MysqlConnectionConfig {
    /// Parse a MySQL URL into connection configuration.
    ///
    /// # Format
    /// `mysql://user:password@host:port/database`
    ///
    /// # Errors
    /// Returns an error if the URL cannot be parsed or is missing required fields.
    fn from_url(url: &str, server_id: u32, tls_skip_verify: bool) -> Result<Self> {
        let parsed = url::Url::parse(url)
            .context("Failed to parse MySQL URL. Expected: mysql://user:pass@host:port/db")?;

        let scheme = parsed.scheme();
        anyhow::ensure!(
            scheme == "mysql",
            "Unsupported URL scheme '{}' for MySQL source. Expected 'mysql://'",
            scheme
        );

        let host = parsed
            .host_str()
            .context("Missing host in MySQL URL")?
            .to_string();
        let port = parsed.port().unwrap_or(3306);
        let database = parsed.path().trim_start_matches('/').to_string();

        // url crate percent-decodes the username automatically
        let user = if !parsed.username().is_empty() {
            parsed.username().to_string()
        } else {
            "root".to_string()
        };

        // Password must be extracted raw (url crate already percent-decodes)
        let password = parsed.password().unwrap_or("").to_string();

        anyhow::ensure!(
            !database.is_empty(),
            "Missing database name in MySQL URL. Expected: mysql://user:pass@host:port/db"
        );

        Ok(Self {
            host,
            port,
            database,
            user,
            password,
            server_id,
            tls_skip_verify,
        })
    }

    /// Build `mysql_async::Opts` for creating connections.
    ///
    /// TLS is enabled with full verification by default. When the connection
    /// config has `tls_skip_verify = true`, certificate validation is
    /// disabled (development only).
    fn to_opts(&self) -> mysql_async::Opts {
        let builder = mysql_async::OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .db_name(Some(self.database.clone()))
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .ssl_opts(mysql_ssl_opts(self.tls_skip_verify));
        mysql_async::Opts::from(builder)
    }
}

/// Build `SslOpts` for MySQL connections.
///
/// Default: rely on the system trust store. When `skip_verify` is true,
/// the danger flag is set — emit a `warn!` once at startup so the unsafe
/// posture is visible in logs.
pub(crate) fn mysql_ssl_opts(skip_verify: bool) -> mysql_async::SslOpts {
    let opts = mysql_async::SslOpts::default();
    if skip_verify {
        opts.with_danger_accept_invalid_certs(true)
    } else {
        opts
    }
}

/// MySQL source for Change Data Capture.
///
/// Manages two MySQL connections: `query_conn` for metadata queries and
/// `binlog_conn` for binary log reading. The live `(file, position,
/// gtid_executed)` checkpoint is owned by the replication loop and
/// persisted to `StateStore`; this struct holds no in-memory checkpoint.
pub struct MysqlSource {
    config: MysqlConnectionConfig,
    server_id: u32,
    use_gtid: bool,
    /// Connection for metadata queries, wrapped in `Mutex` because `mysql_async::Conn` is not `Sync`.
    query_conn: Option<Mutex<mysql_async::Conn>>,
    /// Dedicated connection for binlog streaming.
    binlog_conn: Option<Mutex<mysql_async::Conn>>,
    /// Cached table schemas keyed by fully-qualified table name (`db.table`).
    schema_cache: Arc<Mutex<HashMap<String, SourceTableSchema>>>,
}

impl MysqlSource {
    /// Creates a new `MysqlSource` struct without connecting.
    ///
    /// Connections are opened lazily in [`Source::setup()`].
    pub async fn new(url: &str, config: &MysqlSourceConfig) -> Result<Self> {
        let conn_config =
            MysqlConnectionConfig::from_url(url, config.server_id, config.tls_skip_verify)?;
        info!(
            "MySQL source configured (server_id: {}, gtid: {})",
            config.server_id, config.gtid_enabled
        );
        if config.tls_skip_verify {
            warn!(
                "MySQL TLS certificate verification disabled \
                 (MYSQL_TLS_SKIP_VERIFY=true). This is unsafe in production."
            );
        }
        Ok(Self {
            config: conn_config,
            server_id: config.server_id,
            use_gtid: config.gtid_enabled,
            query_conn: None,
            binlog_conn: None,
            schema_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Helper: open a connection using the stored config.
    async fn open_connection(&self) -> Result<mysql_async::Conn> {
        let pool = mysql_async::Pool::new(self.config.to_opts());
        pool.get_conn()
            .await
            .context("Failed to connect to MySQL server")
    }
}

#[async_trait]
impl Source for MysqlSource {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn source_type(&self) -> SourceType {
        SourceType::Mysql
    }

    /// Validate MySQL source prerequisites.
    ///
    /// Validation requires a live connection, so actual checks are deferred
    /// to [`setup()`](Source::setup). This returns `Ok(())` unconditionally.
    async fn validate(&self) -> Result<()> {
        // Validation happens in setup() since we need a connection.
        Ok(())
    }

    /// Setup: connect to MySQL, verify prerequisites, and introspect schemas.
    ///
    /// Checks: server version >= 5.7, GTID mode, binlog_format=ROW,
    /// binlog_row_image=FULL, REPLICATION SLAVE privilege, and detects Aurora.
    /// Opens two connections: one for queries and one for binlog streaming.
    async fn setup(&mut self, tables: &[String]) -> Result<()> {
        // 1. Open query connection
        let mut conn = self.open_connection().await?;

        // 2. Verify server version >= 5.7
        let version_row: Vec<(String,)> = conn
            .query("SELECT VERSION()")
            .await
            .context("Failed to get MySQL version")?;
        let version_str = version_row
            .first()
            .map(|r| r.0.as_str())
            .unwrap_or("unknown");
        info!("MySQL server version: {}", version_str);

        let major_ok: bool = version_str
            .split('.')
            .next()
            .and_then(|s: &str| s.parse::<u32>().ok())
            .map(|major: u32| major >= 5)
            .unwrap_or(false);
        anyhow::ensure!(
            major_ok,
            "MySQL server version must be >= 5.7 for CDC. Got: {}",
            version_str
        );
        info!("  Server version check passed ✓");

        // 3. Verify GTID mode (if enabled)
        if self.use_gtid {
            let gtid_row: Vec<(String,)> = conn
                .query("SELECT @@global.gtid_mode")
                .await
                .context("Failed to check GTID mode")?;
            let gtid_mode = gtid_row.first().map(|r| r.0.as_str()).unwrap_or("OFF");
            anyhow::ensure!(
                gtid_mode.eq_ignore_ascii_case("ON"),
                "GTID mode must be ON for MySQL CDC. Current: {}. \
                 Set gtid_mode=ON and enforce_gtid_consistency=ON",
                gtid_mode
            );
            info!("  GTID mode: ON ✓");
        } else {
            info!("  GTID mode: not checked (disabled)");
        }

        // 4. Verify binlog_format = ROW
        let format_row: Vec<(String,)> = conn
            .query("SELECT @@global.binlog_format")
            .await
            .context("Failed to check binlog_format")?;
        let binlog_format = format_row
            .first()
            .map(|r| r.0.as_str())
            .unwrap_or("unknown");
        anyhow::ensure!(
            binlog_format.eq_ignore_ascii_case("ROW"),
            "binlog_format must be ROW for MySQL CDC. Current: {}",
            binlog_format
        );
        info!("  binlog_format: ROW ✓");

        // 5. Verify binlog_row_image = FULL
        let image_row: Vec<(String,)> = conn
            .query("SELECT @@global.binlog_row_image")
            .await
            .context("Failed to check binlog_row_image")?;
        let row_image = image_row.first().map(|r| r.0.as_str()).unwrap_or("unknown");
        anyhow::ensure!(
            row_image.eq_ignore_ascii_case("FULL"),
            "binlog_row_image must be FULL for MySQL CDC. Current: {}",
            row_image
        );
        info!("  binlog_row_image: FULL ✓");

        // 6. Check REPLICATION SLAVE privilege
        // Try mysql.user first; fall back to SHOW GRANTS for RDS/Aurora/Cloud SQL
        let has_replication = match conn
            .exec_first::<(String,), _, _>(
                "SELECT Repl_slave_priv FROM mysql.user WHERE User = ?",
                (self.config.user.as_str(),),
            )
            .await
        {
            Ok(Some(row)) => row.0.to_uppercase() == "Y",
            _ => {
                let grant_rows: Vec<(String,)> = conn
                    .query("SHOW GRANTS")
                    .await
                    .context("Failed to check MySQL privileges")?;
                grant_rows.iter().any(|row| {
                    let upper = row.0.to_uppercase();
                    upper.contains("REPLICATION SLAVE") || upper.contains("ALL PRIVILEGES")
                })
            }
        };
        anyhow::ensure!(
            has_replication,
            "MySQL user missing REPLICATION SLAVE privilege. \
             Required for binlog reading. \
             Run: GRANT REPLICATION SLAVE ON *.* TO '<user>'@'<host>';"
        );
        info!("  REPLICATION SLAVE privilege: ✓");

        // 7. Detect Amazon Aurora (best-effort)
        let is_aurora: bool = conn
            .query_first::<(String,), &str>("SELECT @@aurora_version")
            .await
            .ok()
            .flatten()
            .is_some();
        if is_aurora {
            info!("  Detected Amazon Aurora MySQL");
        }

        // 8. Open a separate binlog connection
        let binlog_conn = self.open_connection().await?;

        // 9. Introspect table schemas
        if !tables.is_empty() {
            let schemas =
                schema::introspect_mysql_schemas_inner(&mut conn, tables, &self.config.database)
                    .await
                    .context("Failed to introspect MySQL table schemas")?;

            let mut cache = self.schema_cache.lock().await;
            for s in schemas {
                let key = format!("{}.{}", s.schema, s.name);
                cache.insert(key, s);
            }
            info!("  Cached schemas for {} table(s)", tables.len());
        }

        self.query_conn = Some(Mutex::new(conn));
        self.binlog_conn = Some(Mutex::new(binlog_conn));

        info!("MySQL setup complete ✓");
        Ok(())
    }

    /// Start MySQL binlog replication. Consumes the binlog connection and returns a MysqlBinlogStream.
    async fn start_replication(
        &mut self,
        position: Option<SourcePosition>,
    ) -> Result<Box<dyn ReplicationStream>> {
        use crate::source::mysql::binlog_stream::MysqlBinlogStream;

        let binlog_conn: mysql_async::Conn = self
            .binlog_conn
            .take()
            .ok_or_else(|| anyhow::anyhow!("binlog connection not available"))?
            .into_inner();

        let server_id = self.config.server_id;
        let resume_gtid = extract_resume_gtid_set(&position);
        match resume_gtid.as_deref() {
            Some(gtid) if !gtid.is_empty() => {
                info!("Resuming MySQL binlog from Gtid_set: {}", gtid)
            }
            _ => info!("Starting MySQL binlog from earliest available position"),
        }
        let stream = MysqlBinlogStream::new(binlog_conn, server_id, resume_gtid.as_deref()).await?;

        Ok(Box::new(stream))
    }

    /// MySQL has no in-source checkpoint state — the live triple
    /// `(file, position, gtid_executed)` is owned by the replication loop
    /// and persisted to `StateStore::save_mysql_checkpoint`.
    fn checkpoint_position(&self) -> Option<SourcePosition> {
        None
    }

    /// Cleanup: drop connections and release resources.
    async fn cleanup(&mut self) -> Result<()> {
        self.query_conn = None;
        self.binlog_conn = None;
        self.schema_cache.lock().await.clear();
        info!("MySQL cleanup complete");
        Ok(())
    }

    async fn create_loop(
        &mut self,
        position: Option<SourcePosition>,
    ) -> Result<Box<dyn crate::engine::replication::ReplicationLoop>> {
        let resume_gtid = extract_resume_gtid_set(&position);
        let typed_stream = self.start_replication_typed(resume_gtid.as_deref()).await?;
        Ok(Box::new(
            crate::engine::replication::MysqlReplicationLoop::new(typed_stream),
        ))
    }
}

/// Pick the GTID-set string the binlog stream should resume from.
/// Only `MysqlBinlog { gtid_executed }` carries a meaningful resume value;
/// every other variant means "no checkpoint, start from the earliest
/// available binlog".
fn extract_resume_gtid_set(position: &Option<SourcePosition>) -> Option<String> {
    match position {
        Some(SourcePosition::MysqlBinlog { gtid_executed, .. }) if !gtid_executed.is_empty() => {
            Some(gtid_executed.clone())
        }
        _ => None,
    }
}

impl MysqlSource {
    /// Start a typed binlog stream (used by `create_loop`).
    pub async fn start_replication_typed(
        &mut self,
        initial_gtid_set: Option<&str>,
    ) -> Result<mysql_async::BinlogStream> {
        use crate::source::mysql::binlog_stream::MysqlBinlogStream;
        let conn: mysql_async::Conn = self
            .binlog_conn
            .take()
            .ok_or_else(|| anyhow::anyhow!("binlog connection not available"))?
            .into_inner();
        let stream = MysqlBinlogStream::new(conn, self.config.server_id, initial_gtid_set).await?;
        Ok(stream.into_inner())
    }
}

// Manual Debug impl to avoid leaking credentials
impl std::fmt::Debug for MysqlSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSource")
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("database", &self.config.database)
            .field("user", &self.config.user)
            .field("server_id", &self.server_id)
            .field("use_gtid", &self.use_gtid)
            .field("has_query_conn", &self.query_conn.is_some())
            .field("has_binlog_conn", &self.binlog_conn.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_connection_config_parsing() {
        let cfg = MysqlConnectionConfig::from_url(
            "mysql://user:pass@localhost:3306/mydb",
            5400,
            false,
        )
        .unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 3306);
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "pass");
        assert!(!cfg.tls_skip_verify);

        let cfg = MysqlConnectionConfig::from_url("mysql://host.com/db", 5401, true).unwrap();
        assert_eq!(cfg.host, "host.com");
        assert_eq!(cfg.port, 3306);
        assert_eq!(cfg.database, "db");
        assert_eq!(cfg.user, "root");
        assert_eq!(cfg.password, "");
        assert!(cfg.tls_skip_verify);

        let result = MysqlConnectionConfig::from_url("mysql://user:pass@localhost", 5402, false);
        assert!(result.is_err()); // no database path

        let result = MysqlConnectionConfig::from_url("postgres://localhost/db", 5403, false);
        assert!(result.is_err()); // wrong scheme

        let cfg = MysqlConnectionConfig::from_url(
            "mysql://user%40host:p%40ss@localhost/mydb",
            5404,
            false,
        )
        .unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.user, "user%40host");
        assert_eq!(cfg.password, "p%40ss");
    }

    #[tokio::test]
    async fn test_mysql_source_new() {
        let mysql_config = MysqlSourceConfig {
            server_id: 6000,
            gtid_enabled: true,
            tls_skip_verify: false,
        };
        let source = MysqlSource::new("mysql://root@localhost/testdb", &mysql_config).await;
        assert!(source.is_ok());
        let source = source.unwrap();
        assert_eq!(source.server_id, 6000);
        assert!(source.use_gtid);
        assert!(source.query_conn.is_none());
        assert!(source.binlog_conn.is_none());
    }

    #[test]
    fn test_source_trait_methods() {
        let mysql_config = MysqlSourceConfig {
            server_id: 6000,
            gtid_enabled: true,
            tls_skip_verify: false,
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let source = rt
            .block_on(MysqlSource::new(
                "mysql://root@localhost/testdb",
                &mysql_config,
            ))
            .unwrap();

        assert_eq!(source.name(), "mysql");
        assert_eq!(source.source_type(), SourceType::Mysql);
        assert!(source.checkpoint_position().is_none());
    }

    #[test]
    fn test_debug_no_credentials() {
        let mysql_config = MysqlSourceConfig {
            server_id: 6000,
            gtid_enabled: true,
            tls_skip_verify: false,
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let source = rt
            .block_on(MysqlSource::new(
                "mysql://user:secret@host:3306/db",
                &mysql_config,
            ))
            .unwrap();

        let debug_str = format!("{:?}", source);
        // Password should never appear in debug output
        assert!(!debug_str.contains("secret"));
        // Sanity checks
        assert!(debug_str.contains("host"));
        assert!(debug_str.contains("user"));
        assert!(debug_str.contains("database"));
        assert!(debug_str.contains("server_id"));
    }

    #[test]
    fn test_mysql_source_type_display() {
        assert_eq!(SourceType::Mysql.to_string(), "mysql");
    }

    #[test]
    fn test_typo_mapping_exhaustive() {
        // Verify all expected MySQL type names map without panic
        let cases = [
            ("tinyint", "Int16"),
            ("smallint", "Int16"),
            ("mediumint", "Int32"),
            ("int", "Int32"),
            ("bigint", "Int64"),
            ("float", "Float32"),
            ("double", "Float64"),
            ("decimal", "Decimal"),
            ("date", "Date"),
            ("time", "Time"),
            ("datetime", "Timestamp"),
            ("timestamp", "Timestamp"),
            ("char", "String"),
            ("varchar", "String"),
            ("text", "Text"),
            ("blob", "Bytes"),
            ("json", "Json"),
            ("bool", "Boolean"),
            ("boolean", "Boolean"),
            ("unknown_type", "String"),
        ];
        for (mysql_type, expected_variant) in &cases {
            let dt = crate::source::mysql::schema::mysql_type_to_data_type(mysql_type);
            let actual = format!("{:?}", dt);
            assert!(
                actual.starts_with(expected_variant),
                "mysql_type={} -> {:?} (expected starts with {})",
                mysql_type,
                dt,
                expected_variant
            );
        }
    }
}
