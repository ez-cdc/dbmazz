use std::collections::{HashMap, HashSet};

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

    /// Execute complete StarRocks setup.
    pub async fn run(&self) -> Result<(), SetupError> {
        info!("StarRocks Setup:");

        let mut conn = self.get_conn().await?;

        // 1. Verify connectivity
        let _: Option<i32> = conn.query_first("SELECT 1").await
            .map_err(|e| self.sr_error(e.to_string()))?;
        info!("  [OK] StarRocks connection OK");

        // 2. Verify all tables exist
        self.verify_tables_exist(&mut conn).await?;

        // 3. Batch ensure audit columns
        let tables: Vec<&str> = self.config.tables.iter()
            .map(|t| t.split('.').next_back().unwrap_or(t))
            .collect();

        self.ensure_audit_columns_batch(&mut conn, &tables).await?;

        info!("[OK] StarRocks setup complete");
        Ok(())
    }

    /// Verify that all configured tables exist in StarRocks.
    async fn verify_tables_exist(&self, conn: &mut Conn) -> Result<(), SetupError> {
        let rows: Vec<(String,)> = conn
            .exec(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                (&self.config.starrocks_db,),
            )
            .await
            .map_err(|e| self.sr_error(e.to_string()))?;

        let existing: HashSet<String> = rows.into_iter().map(|(n,)| n).collect();

        for table in &self.config.tables {
            let table_name = table.split('.').next_back().unwrap_or(table);
            if existing.contains(table_name) {
                info!("  [OK] Table {} exists in StarRocks", table_name);
            } else {
                return Err(SetupError::SrTableNotFound {
                    table: table.clone(),
                });
            }
        }

        Ok(())
    }

    /// Get a connection from the pool.
    async fn get_conn(&self) -> Result<Conn, SetupError> {
        self.pool.get_conn().await.map_err(|e| self.sr_error(e.to_string()))
    }

    /// Shorthand for SrConnectionFailed error.
    fn sr_error(&self, error: String) -> SetupError {
        SetupError::SrConnectionFailed {
            host: self.config.starrocks_url.clone(),
            error,
        }
    }

    /// Batch ensure audit columns for multiple pre-existing tables.
    /// Uses 1 query to get all columns for all tables, then only ALTERs what's missing.
    async fn ensure_audit_columns_batch(
        &self,
        conn: &mut Conn,
        tables: &[&str],
    ) -> Result<(), SetupError> {
        validate_sql_identifier(&self.config.starrocks_db)
            .map_err(|e| self.sr_error(format!("Invalid database name: {}", e)))?;

        // 1 query: get all columns for all tables at once
        let rows: Vec<(String, String)> = conn
            .exec(
                "SELECT table_name, COLUMN_NAME FROM information_schema.columns
                 WHERE table_schema = ?",
                (&self.config.starrocks_db,),
            )
            .await
            .map_err(|e| self.sr_error(e.to_string()))?;

        // Build lookup: table_name -> set of column names
        let mut table_columns: HashMap<&str, HashSet<String>> = HashMap::new();
        for (tbl, col) in &rows {
            table_columns.entry(tbl.as_str()).or_default().insert(col.clone());
        }

        // Only ALTER what's actually missing
        for table in tables {
            validate_sql_identifier(table)
                .map_err(|e| self.sr_error(format!("Invalid table name: {}", e)))?;

            let existing = table_columns.get(table);

            for (col_name, col_def) in AUDIT_COLUMNS {
                let has_col = existing.is_some_and(|cols| cols.contains(*col_name));

                if !has_col {
                    info!("  Adding audit column {} to {}", col_name, table);
                    let sql = format!(
                        "ALTER TABLE `{}`.`{}` ADD COLUMN `{}` {}",
                        self.config.starrocks_db, table, col_name, col_def
                    );
                    conn.query_drop(sql).await.map_err(|e| SetupError::SrAuditColumnsFailed {
                        table: table.to_string(),
                        error: e.to_string(),
                    })?;
                    info!("  [OK] Column {} added to {}", col_name, table);
                }
            }
        }

        Ok(())
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
