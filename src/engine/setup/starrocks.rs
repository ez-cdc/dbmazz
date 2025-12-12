use anyhow::Result;
use mysql_async::{Pool, Conn, prelude::Queryable};

use super::error::SetupError;
use crate::config::Config;

/// Columnas de auditoría CDC que deben existir en StarRocks
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

    /// Ejecutar todo el setup de StarRocks
    pub async fn run(&self) -> Result<(), SetupError> {
        println!("🔧 StarRocks Setup:");
        
        // 1. Verificar conectividad
        self.verify_connection().await?;
        
        // 2. Verificar que las tablas existen
        self.verify_tables_exist().await?;
        
        // 3. Agregar columnas de auditoría
        self.ensure_audit_columns().await?;
        
        println!("✅ StarRocks setup complete");
        Ok(())
    }

    /// Verificar conectividad a StarRocks
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

        println!("  ✓ StarRocks connection OK");
        Ok(())
    }

    /// Verificar que todas las tablas existen en StarRocks
    async fn verify_tables_exist(&self) -> Result<(), SetupError> {
        let mut conn = self.pool
            .get_conn()
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;

        for table in &self.config.tables {
            // Extraer nombre de tabla sin schema (StarRocks no usa schemas)
            let table_name = table.split('.').last().unwrap_or(table);

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

            println!("  ✓ Table {} exists in StarRocks", table_name);
        }

        Ok(())
    }

    /// Asegurar que todas las tablas tienen columnas de auditoría
    async fn ensure_audit_columns(&self) -> Result<(), SetupError> {
        for table in &self.config.tables {
            let table_name = table.split('.').last().unwrap_or(table);
            self.ensure_audit_columns_for_table(table_name).await?;
        }
        Ok(())
    }

    /// Agregar columnas de auditoría a una tabla específica
    async fn ensure_audit_columns_for_table(&self, table: &str) -> Result<(), SetupError> {
        let mut conn = self.pool
            .get_conn()
            .await
            .map_err(|e| SetupError::SrConnectionFailed {
                host: self.config.starrocks_url.clone(),
                error: e.to_string(),
            })?;

        // Obtener columnas existentes
        let existing_columns = self.get_table_columns(&mut conn, table).await?;

        // Agregar las que faltan
        for (col_name, col_def) in AUDIT_COLUMNS {
            if !existing_columns.contains(&col_name.to_string()) {
                println!("  🔧 Adding audit column {} to {}", col_name, table);
                
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

                println!("  ✅ Column {} added to {}", col_name, table);
            } else {
                println!("  ✓ Column {} already exists in {}", col_name, table);
            }
        }

        Ok(())
    }

    /// Obtener lista de columnas de una tabla
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

/// Helper para crear pool de conexiones a StarRocks
pub fn create_starrocks_pool(config: &Config) -> Result<Pool, SetupError> {
    // Extraer host del URL
    let host = config.starrocks_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split(':')
        .next()
        .unwrap_or("localhost");

    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(9030) // Puerto MySQL de StarRocks
        .user(Some(config.starrocks_user.clone()))
        .pass(Some(config.starrocks_pass.clone()))
        .db_name(Some(config.starrocks_db.clone()))
        .prefer_socket(false); // Forzar TCP, no usar socket

    Ok(Pool::new(opts))
}

