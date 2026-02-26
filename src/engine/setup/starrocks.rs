use std::collections::{HashMap, HashSet};

use anyhow::Result;
use mysql_async::{Pool, Conn, prelude::Queryable};
use tracing::info;

use super::error::SetupError;
use super::postgres::PgTableSchema;
use crate::config::Config;
use crate::connectors::sources::postgres::types::pg_type_to_data_type;
use crate::connectors::sinks::starrocks::types::TypeMapper;
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
    ///
    /// - `pg_schemas`: PG table schemas only for tables that need to be created (lazy-loaded).
    /// - `existing_sr_tables`: set of table names already in StarRocks (from orchestrator, avoids re-query).
    pub async fn run(
        &self,
        pg_schemas: &HashMap<String, PgTableSchema>,
        existing_sr_tables: &HashSet<String>,
    ) -> Result<(), SetupError> {
        info!("StarRocks Setup:");

        let mut conn = self.get_conn().await?;

        // 1. Verify connectivity
        let _: Option<i32> = conn.query_first("SELECT 1").await
            .map_err(|e| self.sr_error(e.to_string()))?;
        info!("  [OK] StarRocks connection OK");

        // 2. Create missing tables (DDL can't be batched, but we only iterate missing ones)
        let mut newly_created: HashSet<String> = HashSet::new();

        for table in &self.config.tables {
            let table_name = table.split('.').next_back().unwrap_or(table);

            if existing_sr_tables.contains(table_name) {
                info!("  [OK] Table {} exists in StarRocks", table_name);
            } else if let Some(pg_schema) = pg_schemas.get(table) {
                info!("  Table {} not found in StarRocks, creating from source schema...", table_name);
                self.create_table_from_source(&mut conn, table_name, pg_schema).await?;
                info!("  [OK] Table {} created in StarRocks", table_name);
                newly_created.insert(table_name.to_string());
            } else {
                return Err(SetupError::SrTableNotFound {
                    table: table.clone(),
                });
            }
        }

        // 3. Batch ensure audit columns (1 query for all pre-existing tables)
        //    Skip newly created tables — they already have audit columns in the DDL.
        let pre_existing: Vec<&str> = self.config.tables.iter()
            .map(|t| t.split('.').next_back().unwrap_or(t))
            .filter(|t| !newly_created.contains(*t))
            .collect();

        if !pre_existing.is_empty() {
            self.ensure_audit_columns_batch(&mut conn, &pre_existing).await?;
        }

        info!("[OK] StarRocks setup complete");
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

    /// Create a StarRocks table based on the source PostgreSQL schema.
    async fn create_table_from_source(
        &self,
        conn: &mut Conn,
        table_name: &str,
        schema: &PgTableSchema,
    ) -> Result<(), SetupError> {
        validate_sql_identifier(table_name)
            .map_err(|e| self.sr_error(format!("Invalid table name: {}", e)))?;
        validate_sql_identifier(&self.config.starrocks_db)
            .map_err(|e| self.sr_error(format!("Invalid database name: {}", e)))?;

        let type_mapper = TypeMapper::new();

        // Build column definitions from PG schema
        let mut col_defs = Vec::new();
        for col in &schema.columns {
            let data_type = pg_type_to_data_type(col.type_oid, col.type_mod);
            let sr_type = type_mapper.to_starrocks_type(&data_type);
            let not_null = if col.not_null { " NOT NULL" } else { "" };
            col_defs.push(format!("    `{}` {}{}", col.name, sr_type, not_null));
        }

        // Add audit columns
        for (col_name, col_def) in AUDIT_COLUMNS {
            col_defs.push(format!("    `{}` {}", col_name, col_def));
        }

        // Determine key type and distribution
        let (key_clause, dist_cols) = if schema.pk_columns.is_empty() {
            let first_col = &schema.columns[0].name;
            (
                format!("DUPLICATE KEY (`{}`)", first_col),
                format!("`{}`", first_col),
            )
        } else {
            let pk_list: String = schema.pk_columns.iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ");
            (
                format!("PRIMARY KEY ({})", pk_list),
                pk_list.clone(),
            )
        };

        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n{}\n) ENGINE=OLAP\n{}\nDISTRIBUTED BY HASH({}) BUCKETS 4\nPROPERTIES ('replication_num' = '1')",
            self.config.starrocks_db,
            table_name,
            col_defs.join(",\n"),
            key_clause,
            dist_cols,
        );

        conn.query_drop(&ddl).await.map_err(|e| SetupError::SrConnectionFailed {
            host: self.config.starrocks_url.clone(),
            error: format!("Failed to create table {}: {}", table_name, e),
        })?;

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
