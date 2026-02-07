// Copyright 2025
// Licensed under the Elastic License v2.0

//! StarRocks Schema Setup
//!
//! This module handles schema setup and management for StarRocks tables,
//! including:
//! - Table existence verification
//! - CDC audit column creation
//! - Schema evolution (adding columns)
//!
//! All DDL operations use the MySQL protocol (port 9030).

use anyhow::{Result, anyhow};
use mysql_async::{Pool, Conn, OptsBuilder, prelude::Queryable};
use tracing::info;

use super::config::StarRocksSinkConfig;
use super::types::TypeMapper;
use crate::core::DataType;
use crate::utils::validate_sql_identifier;

/// CDC audit columns that must exist in all replicated StarRocks tables.
#[allow(dead_code)]
const AUDIT_COLUMNS: &[(&str, &str)] = &[
    ("dbmazz_op_type", "TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE'"),
    ("dbmazz_is_deleted", "BOOLEAN COMMENT 'Soft delete flag'"),
    ("dbmazz_synced_at", "DATETIME COMMENT 'Timestamp when record was synced'"),
    ("dbmazz_cdc_version", "BIGINT COMMENT 'Source LSN/position for ordering'"),
];

/// StarRocks schema setup and management.
///
/// Handles DDL operations via MySQL protocol including:
/// - Connection validation
/// - Table existence checks
/// - Audit column management
/// - Schema evolution
#[allow(dead_code)]
pub struct StarRocksSetup {
    /// MySQL connection pool for DDL operations
    pool: Pool,
    /// StarRocks configuration
    config: StarRocksSinkConfig,
    /// Type mapper for data type conversions
    type_mapper: TypeMapper,
}

#[allow(dead_code)]
impl StarRocksSetup {
    /// Creates a new StarRocks setup instance.
    ///
    /// # Arguments
    ///
    /// * `config` - StarRocks configuration
    ///
    /// # Returns
    ///
    /// A new `StarRocksSetup` instance with initialized MySQL pool
    pub fn new(config: StarRocksSinkConfig) -> Result<Self> {
        let pool = Self::create_pool(&config)?;

        Ok(Self {
            pool,
            config,
            type_mapper: TypeMapper::new(),
        })
    }

    /// Creates a MySQL connection pool for DDL operations.
    fn create_pool(config: &StarRocksSinkConfig) -> Result<Pool> {
        let hostname = config.hostname();

        let opts = OptsBuilder::default()
            .ip_or_hostname(hostname)
            .tcp_port(config.mysql_port)
            .user(Some(config.user.clone()))
            .pass(Some(config.password.clone()))
            .db_name(Some(config.database.clone()))
            .prefer_socket(false); // Force TCP, StarRocks doesn't support Unix socket

        Ok(Pool::new(opts))
    }

    /// Runs the complete setup process.
    ///
    /// This includes:
    /// 1. Verifying connection to StarRocks
    /// 2. Verifying target tables exist
    /// 3. Adding audit columns to tables
    ///
    /// # Arguments
    ///
    /// * `tables` - List of table names to set up
    pub async fn run(&self, tables: &[String]) -> Result<()> {
        info!("StarRocks Setup:");

        // 1. Verify connectivity
        self.verify_connection().await?;

        // 2. Verify tables exist
        self.verify_tables_exist(tables).await?;

        // 3. Ensure audit columns exist
        self.ensure_audit_columns(tables).await?;

        info!("[OK] StarRocks setup complete");
        Ok(())
    }

    /// Verifies connectivity to StarRocks via MySQL protocol.
    pub async fn verify_connection(&self) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // Test simple query
        let _result: Option<i32> = conn
            .query_first("SELECT 1")
            .await
            .map_err(|e| anyhow!("Connection test failed: {}", e))?;

        info!("  [OK] StarRocks MySQL connection OK");
        Ok(())
    }

    /// Verifies that all specified tables exist in StarRocks.
    pub async fn verify_tables_exist(&self, tables: &[String]) -> Result<()> {
        let mut conn = self.get_connection().await?;

        for table in tables {
            // Extract table name without schema prefix
            let table_name = table.split('.').last().unwrap_or(table);

            let exists: Option<i32> = conn
                .exec_first(
                    "SELECT 1 FROM information_schema.tables
                     WHERE table_schema = ? AND table_name = ?",
                    (&self.config.database, table_name),
                )
                .await
                .map_err(|e| anyhow!("Failed to check table existence: {}", e))?;

            if exists.is_none() {
                return Err(anyhow!(
                    "Table '{}' does not exist in database '{}'",
                    table_name,
                    self.config.database
                ));
            }

            info!("  [OK] Table {} exists", table_name);
        }

        Ok(())
    }

    /// Ensures all tables have the required CDC audit columns.
    pub async fn ensure_audit_columns(&self, tables: &[String]) -> Result<()> {
        for table in tables {
            let table_name = table.split('.').last().unwrap_or(table);
            self.ensure_audit_columns_for_table(table_name).await?;
        }
        Ok(())
    }

    /// Adds audit columns to a specific table if they don't exist.
    async fn ensure_audit_columns_for_table(&self, table: &str) -> Result<()> {
        // Validate table name to prevent SQL injection
        validate_sql_identifier(table)
            .map_err(|e| anyhow!("Invalid table name '{}': {}", table, e))?;

        // Validate database name
        validate_sql_identifier(&self.config.database)
            .map_err(|e| anyhow!("Invalid database name '{}': {}", self.config.database, e))?;

        let mut conn = self.get_connection().await?;

        // Get existing columns
        let existing_columns = self.get_table_columns(&mut conn, table).await?;

        for (col_name, col_def) in AUDIT_COLUMNS {
            // Validate column name (although these are constants, it's good practice)
            validate_sql_identifier(col_name)
                .map_err(|e| anyhow!("Invalid column name '{}': {}", col_name, e))?;

            if !existing_columns.contains(&col_name.to_string()) {
                info!("  Adding audit column {} to {}", col_name, table);

                let sql = format!(
                    "ALTER TABLE {}.{} ADD COLUMN {} {}",
                    self.config.database, table, col_name, col_def
                );

                conn.query_drop(sql)
                    .await
                    .map_err(|e| anyhow!("Failed to add column {} to {}: {}", col_name, table, e))?;

                info!("  [OK] Column {} added to {}", col_name, table);
            } else {
                info!("  [OK] Column {} already exists in {}", col_name, table);
            }
        }

        Ok(())
    }

    /// Adds a new column to a table (for schema evolution).
    ///
    /// # Arguments
    ///
    /// * `table` - Target table name
    /// * `column_name` - New column name
    /// * `data_type` - Column data type
    pub async fn add_column(
        &self,
        table: &str,
        column_name: &str,
        data_type: &DataType,
    ) -> Result<()> {
        // Validate table name to prevent SQL injection
        validate_sql_identifier(table)
            .map_err(|e| anyhow!("Invalid table name '{}': {}", table, e))?;

        // Validate column name
        validate_sql_identifier(column_name)
            .map_err(|e| anyhow!("Invalid column name '{}': {}", column_name, e))?;

        // Validate database name
        validate_sql_identifier(&self.config.database)
            .map_err(|e| anyhow!("Invalid database name '{}': {}", self.config.database, e))?;

        let sr_type = self.type_mapper.to_starrocks_type(data_type);

        let sql = format!(
            "ALTER TABLE {}.{} ADD COLUMN {} {}",
            self.config.database, table, column_name, sr_type
        );

        let mut conn = self.get_connection().await?;

        match conn.query_drop(&sql).await {
            Ok(_) => {
                info!(
                    "Schema evolution: added column {} ({}) to {}",
                    column_name, sr_type, table
                );
                Ok(())
            }
            Err(e) => {
                let err_msg = e.to_string();
                // Ignore if column already exists
                if err_msg.contains("Duplicate column") || err_msg.contains("already exists") {
                    info!("Column {} already exists in {}, skipping", column_name, table);
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Failed to add column {} to {}: {}",
                        column_name, table, err_msg
                    ))
                }
            }
        }
    }

    /// Gets the list of columns for a table.
    async fn get_table_columns(&self, conn: &mut Conn, table: &str) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = conn
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.columns
                 WHERE table_schema = ? AND table_name = ?",
                (&self.config.database, table),
            )
            .await
            .map_err(|e| anyhow!("Failed to get columns for {}: {}", table, e))?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    /// Gets a connection from the pool.
    async fn get_connection(&self) -> Result<Conn> {
        self.pool
            .get_conn()
            .await
            .map_err(|e| anyhow!("Failed to get MySQL connection: {}", e))
    }

    /// Closes the connection pool.
    pub async fn close(self) -> Result<()> {
        // Pool will be dropped and connections closed
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audit_columns_definition() {
        // Verify all audit columns are defined
        assert_eq!(AUDIT_COLUMNS.len(), 4);

        // Check column names
        let col_names: Vec<&str> = AUDIT_COLUMNS.iter().map(|(name, _)| *name).collect();
        assert!(col_names.contains(&"dbmazz_op_type"));
        assert!(col_names.contains(&"dbmazz_is_deleted"));
        assert!(col_names.contains(&"dbmazz_synced_at"));
        assert!(col_names.contains(&"dbmazz_cdc_version"));
    }
}
