// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle database client for CDC sink operations.
//!
//! This module wraps the `oracle-rs` crate and provides a clean async API
//! for connecting to Oracle, executing DML/DDL, and managing transactions.

use anyhow::{Context, Result};
use oracle_rs::{BatchBuilder, Connection, Value as OracleValue};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

use super::config::OracleSinkConfig;

/// Oracle database client for CDC sink operations.
pub struct OracleClient {
    /// Oracle connection string (host:port/service_name)
    connect_string: String,
    /// Username
    user: String,
    /// Password
    password: String,
    /// Target schema (Oracle username)
    schema: String,
    /// Shared connection wrapped in a Mutex for thread-safe access
    conn: Arc<Mutex<Option<Connection>>>,
}

impl OracleClient {
    /// Creates a new Oracle client from configuration.
    pub fn new(config: &OracleSinkConfig) -> Self {
        Self {
            connect_string: format!("{}:{}/{}", config.host, config.port, config.service_name),
            user: config.user.clone(),
            password: config.password.clone(),
            schema: config.schema.clone(),
            conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Ensure a connection exists, creating one if needed.
    pub async fn ensure_connection(&self) -> Result<()> {
        let mut guard = self.conn.lock().await;
        if guard.is_none() {
            let conn = Connection::connect(
                &self.connect_string,
                &self.user,
                &self.password,
            )
            .await
            .with_context(|| {
                format!(
                    "OracleClient: failed to connect to {}@{}",
                    self.user, self.connect_string
                )
            })?;
            *guard = Some(conn);
            info!(
                "OracleClient: connected to {}@{}",
                self.user, self.connect_string
            );
        }
        Ok(())
    }

    /// Verifies connectivity by executing a simple query.
    /// Creates a temporary connection for validation (does not share the main one).
    pub async fn verify_connection(&self) -> Result<()> {
        let conn = Connection::connect(
            &self.connect_string,
            &self.user,
            &self.password,
        )
        .await
        .with_context(|| "OracleClient: connection validation failed")?;

        let result = conn
            .query("SELECT 'OK' FROM dual", &[])
            .await
            .context("OracleClient: validation query failed")?;

        let status = result
            .rows
            .first()
            .and_then(|r| r.get_string(0))
            .unwrap_or("UNKNOWN");

        info!("OracleClient: connection OK (status: {})", status);

        // Get Oracle version
        let version_result = conn
            .query("SELECT version FROM v$instance", &[])
            .await
            .context("OracleClient: version query failed")?;

        if let Some(version) = version_result
            .rows
            .first()
            .and_then(|r| r.get_string(0))
        {
            info!("OracleClient: server version: {}", version);
        }

        conn.close().await?;
        Ok(())
    }

    /// Execute a DDL statement (CREATE, ALTER, DROP).
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        self.ensure_connection().await?;
        let guard = self.conn.lock().await;
        let conn = guard.as_ref().unwrap();
        debug!("OracleClient: executing DDL: {}", sql);
        conn.execute_dml_sql(sql, &[])
            .await
            .with_context(|| format!("OracleClient: DDL failed: {}", sql))?;
        Ok(())
    }

    /// Execute a DML statement with bind params.
    pub async fn execute_dml(&self, sql: &str, params: &[OracleValue]) -> Result<u64> {
        self.ensure_connection().await?;
        let guard = self.conn.lock().await;
        let conn = guard.as_ref().unwrap();
        let rows = conn
            .execute_dml_sql(sql, params)
            .await
            .with_context(|| format!("OracleClient: DML failed: {}", sql))?;
        Ok(rows)
    }

    /// Execute a query and return the result.
    pub async fn query(&self, sql: &str, params: &[OracleValue]) -> Result<oracle_rs::QueryResult> {
        self.ensure_connection().await?;
        let guard = self.conn.lock().await;
        let conn = guard.as_ref().unwrap();
        let result = conn
            .query(sql, params)
            .await
            .with_context(|| format!("OracleClient: query failed: {}", sql))?;
        Ok(result)
    }

    /// Execute a batch INSERT using Oracle array DML.
    pub async fn execute_batch_insert(
        &self,
        sql: &str,
        rows: &[Vec<OracleValue>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut batch = BatchBuilder::new(sql);
        for row in rows {
            batch = batch.add_row(row.clone());
        }
        let batch_binds = batch.with_row_counts().build();

        self.ensure_connection().await?;
        let guard = self.conn.lock().await;
        let conn = guard.as_ref().unwrap();
        let result = conn
            .execute_batch(&batch_binds)
            .await
            .with_context(|| "OracleClient: batch insert failed")?;

        Ok(result.total_rows_affected)
    }

    /// Check if a table exists in the schema.
    pub async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let result = self
            .query(
                "SELECT COUNT(*) FROM all_tables WHERE owner = UPPER(:1) AND table_name = UPPER(:2)",
                &[OracleValue::String(self.schema.clone()), OracleValue::String(table_name.to_string())],
            )
            .await?;

        let count = result
            .rows
            .first()
            .and_then(|r| r.get_i64(0))
            .unwrap_or(0);
        Ok(count > 0)
    }

    /// Get column names for a table.
    pub async fn get_table_columns(&self, table_name: &str) -> Result<Vec<String>> {
        let result = self
            .query(
                "SELECT column_name FROM all_tab_columns WHERE owner = UPPER(:1) AND table_name = UPPER(:2) ORDER BY column_id",
                &[OracleValue::String(self.schema.clone()), OracleValue::String(table_name.to_string())],
            )
            .await?;

        let columns: Vec<String> = result
            .rows
            .iter()
            .filter_map(|r| r.get_string(0).map(|s| s.to_string()))
            .collect();
        Ok(columns)
    }

    /// Commit the current transaction.
    pub async fn commit(&self) -> Result<()> {
        let guard = self.conn.lock().await;
        if let Some(ref conn) = *guard {
            conn.commit()
                .await
                .context("OracleClient: commit failed")?;
            debug!("OracleClient: transaction committed");
        }
        Ok(())
    }

    /// Rollback the current transaction.
    #[allow(dead_code)]
    pub async fn rollback(&self) -> Result<()> {
        let guard = self.conn.lock().await;
        if let Some(ref conn) = *guard {
            conn.rollback()
                .await
                .context("OracleClient: rollback failed")?;
            debug!("OracleClient: transaction rolled back");
        }
        Ok(())
    }

    /// Close the connection.
    pub async fn close(&self) -> Result<()> {
        let mut guard = self.conn.lock().await;
        if let Some(conn) = guard.take() {
            conn.close()
                .await
                .context("OracleClient: close failed")?;
            info!("OracleClient: connection closed");
        }
        Ok(())
    }

    /// Returns the target schema.
    pub fn schema(&self) -> &str {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> OracleSinkConfig {
        OracleSinkConfig {
            host: "localhost".to_string(),
            port: 1521,
            service_name: "ORCLCDB".to_string(),
            user: "cdc_user".to_string(),
            password: "cdc_pass".to_string(),
            schema: "CDC_SCHEMA".to_string(),
            timeout_secs: 30,
            batch_size: 1000,
        }
    }

    #[test]
    fn test_client_creation() {
        let config = test_config();
        let client = OracleClient::new(&config);
        assert_eq!(client.schema(), "CDC_SCHEMA");
    }

    #[test]
    fn test_connect_string_format() {
        let config = test_config();
        let client = OracleClient::new(&config);
        assert_eq!(client.connect_string, "localhost:1521/ORCLCDB");
    }
}
