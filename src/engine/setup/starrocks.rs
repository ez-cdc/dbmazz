use anyhow::Result;
use mysql_async::{Pool, Conn, prelude::Queryable};
use tracing::info;

use super::error::SetupError;
use crate::config::Config;
use crate::utils::validate_sql_identifier;

/// CDC audit columns that must exist in StarRocks
const AUDIT_COLUMNS: &[(&str, &str)] = &[
    ("dbmazz_op_type", "TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE'"),
    ("dbmazz_is_deleted", "BOOLEAN COMMENT 'Soft delete flag'"),
    ("dbmazz_synced_at", "DATETIME COMMENT 'Timestamp CDC'"),
    ("dbmazz_cdc_version", "BIGINT COMMENT 'LSN PostgreSQL'"),
];

pub struct StarRocksSetup<'a> {
    pool: &'a Pool,
    config: &'a Config,
}

impl<'a> StarRocksSetup<'a> {
    pub fn new(pool: &'a Pool, config: &'a Config) -> Self {
        Self { pool, config }
    }

    /// Execute complete StarRocks setup
    pub async fn run(&self) -> Result<(), SetupError> {
        info!("StarRocks Setup:");

        // 1. Verify connectivity
        self.verify_connection().await?;

        // 2. Verify that tables exist
        self.verify_tables_exist().await?;

        // 3. Add audit columns
        self.ensure_audit_columns().await?;

        info!("[OK] StarRocks setup complete");
        Ok(())
    }

    /// Verify connectivity to StarRocks
    async fn verify_connection(&self) -> Result<(), SetupError> {
        let mut conn = self.pool
            .get_conn()
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;

        // Test simple query
        let _result: Option<i32> = conn
            .query_first("SELECT 1")
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;

        info!("  [OK] StarRocks connection OK");
        Ok(())
    }

    /// Verify that all tables exist in StarRocks
    async fn verify_tables_exist(&self) -> Result<(), SetupError> {
        let mut conn = self.pool
            .get_conn()
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;

        for table in &self.config.tables {
            // Extract table name without schema (StarRocks doesn't use schemas)
            let table_name = table.split('.').next_back().unwrap_or(table);

            let exists: Option<i32> = conn
                .exec_first(
                    "SELECT 1 FROM information_schema.tables
                     WHERE table_schema = ? AND table_name = ?",
                    (&self.config.starrocks_db, table_name),
                )
                .await
                .map_err(|e| SetupError::SrConnectionFailed {
                    host: self.config.starrocks_url.clone(),
                    error: e.to_string(),
                })?;

            if exists.is_none() {
                return Err(SetupError::SrTableNotFound {
                    table: table.clone(),
                });
            }

            info!("  [OK] Table {} exists in StarRocks", table_name);
        }

        Ok(())
    }

    /// Ensure all tables have audit columns
    async fn ensure_audit_columns(&self) -> Result<(), SetupError> {
        for table in &self.config.tables {
            let table_name = table.split('.').next_back().unwrap_or(table);
            self.ensure_audit_columns_for_table(table_name).await?;
        }
        Ok(())
    }

    /// Add audit columns to a specific table
    async fn ensure_audit_columns_for_table(&self, table: &str) -> Result<(), SetupError> {
        // Validate table name to prevent SQL injection
        validate_sql_identifier(table).map_err(|e| SetupError::SrConnectionFailed {
            host: self.config.starrocks_url.clone(),
            error: format!("Invalid table name: {}", e),
        })?;

        // Validate database name
        validate_sql_identifier(&self.config.starrocks_db).map_err(|e| SetupError::SrConnectionFailed {
            host: self.config.starrocks_url.clone(),
            error: format!("Invalid database name: {}", e),
        })?;

        let mut conn = self.pool
            .get_conn()
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;

        // Get existing columns
        let existing_columns = self.get_table_columns(&mut conn, table).await?;

        // Add missing columns
        for (col_name, col_def) in AUDIT_COLUMNS {
            // Validate column name (although these are constants, it's good practice)
            validate_sql_identifier(col_name).map_err(|e| SetupError::SrAuditColumnsFailed {
                table: table.to_string(),
                error: format!("Invalid column name '{}': {}", col_name, e),
            })?;

            if !existing_columns.contains(&col_name.to_string()) {
                info!("  Adding audit column {} to {}", col_name, table);

                let sql = format!(
                    "ALTER TABLE {}.{} ADD COLUMN {} {}",
                    self.config.starrocks_db, table, col_name, col_def
                );

                conn.query_drop(sql)
                    .await
                    .map_err(|e| SetupError::SrAuditColumnsFailed {
                        table: table.to_string(),
                        error: e.to_string(),
                    })?;

                info!("  [OK] Column {} added to {}", col_name, table);
            } else {
                info!("  [OK] Column {} already exists in {}", col_name, table);
            }
        }

        Ok(())
    }

    /// Get list of columns for a table
    async fn get_table_columns(&self, conn: &mut Conn, table: &str) -> Result<Vec<String>, SetupError> {
        let rows: Vec<(String,)> = conn
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.columns
                 WHERE table_schema = ? AND table_name = ?",
                (&self.config.starrocks_db, table),
            )
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }
}

/// Helper to create StarRocks connection pool
pub fn create_starrocks_pool(config: &Config) -> Result<Pool, SetupError> {
    // Extract host from URL
    let host = config.starrocks_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split(':')
        .next()
        .unwrap_or("localhost");

    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(config.starrocks_port) // Puerto MySQL de StarRocks desde config
        .user(Some(config.starrocks_user.clone()))
        .pass(Some(config.starrocks_pass.clone()))
        .db_name(Some(config.starrocks_db.clone()))
        .prefer_socket(false); // Force TCP, don't use socket

    Ok(Pool::new(opts))
}

