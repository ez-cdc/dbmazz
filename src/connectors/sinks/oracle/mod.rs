// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle sink connector.
//!
//! Replicates CDC changes to a target Oracle Database using MERGE (upsert)
//! for inserts/updates and direct DELETE for deletions. The Oracle crate
//! (kubo/rust-oracle) is synchronous, so all Oracle operations run via
//! `tokio::task::spawn_blocking` on the Tokio blocking thread pool.

mod merge_generator;
mod schema_tracking;
mod setup;
mod types;

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tracing::{error, info};

/// Module-level cache of source table schemas.
/// Set once by the main engine's `setup()` call, read by ALL sink instances
/// (including snapshot workers created via `sink_factory`).
static SOURCE_SCHEMAS: OnceLock<Vec<SourceTableSchema>> = OnceLock::new();

use crate::config::SinkConfig;
use crate::core::record::{CdcRecord, ColumnValue, Value};
use crate::core::traits::{
    LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult, SourceColumn, SourceTableSchema,
};
use merge_generator::{generate_delete, generate_merge};
use types::{value_to_oracle_expr, value_to_oracle_typed_expr};

/// Double-quote an Oracle identifier, safely escaping embedded `"`.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Oracle sink — writes CDC records to a target Oracle Database.
pub struct OracleSink {
    /// Oracle connection string (e.g., "//host:1521/service")
    connect_string: String,
    /// Target schema/owner name in Oracle
    schema: String,
    /// Job name for metadata tracking
    job_name: String,
    /// Database name (for logging)
    #[allow(dead_code)]
    database: String,
    /// Oracle user
    user: String,
    /// Oracle password
    password: String,
    /// Lazy connection — established on first use
    conn: Option<oracle::Connection>,
    /// Sink operating mode
    #[allow(dead_code)]
    mode: SinkMode,
}

impl OracleSink {
    /// Parse an oracle:// URL into (user, password, connect_string).
    /// The oracle crate expects connect_string as "//host:port/service".
    fn parse_oracle_url(raw_url: &str, env_user: &str, env_password: &str) -> (String, String, String) {
        if let Ok(parsed) = url::Url::parse(raw_url) {
            let user = if !parsed.username().is_empty() {
                parsed.username().to_string()
            } else {
                env_user.to_string()
            };
            let password = match parsed.password() {
                Some(p) => p.to_string(),
                None => env_password.to_string(),
            };
            let host = parsed.host_str().unwrap_or("localhost");
            let port = parsed.port().unwrap_or(1521);
            let path = parsed.path().trim_start_matches('/');
            let connect_string = format!("//{}:{}/{}", host, port, path);
            (user, password, connect_string)
        } else {
            // Not a valid URL — assume the raw value is already a connect_string
            (env_user.to_string(), env_password.to_string(), raw_url.to_string())
        }
    }
    /// Create a new OracleSink from the provided configuration.
    /// Connection is lazy — established on first use.
    pub fn new(config: &SinkConfig, mode: SinkMode) -> Result<Self> {
        let oracle_config = config.oracle_config()?;

        // In Oracle, the schema IS the user (upper-case by convention).
        // Use the configured schema if explicitly set and non-empty AND not
        // the CLI's default placeholder "APP"; otherwise default to the
        // connected user.
        let schema = if oracle_config.schema.is_empty()
            || oracle_config.schema.eq_ignore_ascii_case("APP")
        {
            config.user.to_uppercase()
        } else {
            oracle_config.schema.clone()
        };

        let (user, password, connect_string) =
            Self::parse_oracle_url(&config.url, &config.user, &config.password);

        info!(
            "OracleSink initialized: db={}, schema={}, job={}",
            config.database, schema, oracle_config.job_name
        );

        Ok(Self {
            connect_string,
            schema,
            job_name: oracle_config.job_name.clone(),
            database: config.database.clone(),
            user,
            password,
            conn: None,
            mode,
        })
    }

    /// Connect to Oracle synchronously.
    fn connect_sync(url: &str, user: &str, password: &str) -> Result<oracle::Connection> {
        let conn = oracle::Connection::connect(user, password, url)
            .with_context(|| format!("OracleSink: failed to connect to {}", url))?;
        info!("OracleSink: connected to {}", url);
        Ok(conn)
    }

    /// Look up a source table schema by table name (from the global cache).
    fn find_schema(table_name: &str) -> Option<SourceTableSchema> {
        SOURCE_SCHEMAS.get()?.iter().find(|s| s.name == table_name).cloned()
    }

    /// Group CDC records by table name.
    fn group_by_table(records: &[CdcRecord]) -> HashMap<String, Vec<&CdcRecord>> {
        let mut groups: HashMap<String, Vec<&CdcRecord>> = HashMap::new();
        for record in records {
            let table_name = match record {
                CdcRecord::Insert { table, .. }
                | CdcRecord::Update { table, .. }
                | CdcRecord::Delete { table, .. }
                | CdcRecord::SchemaChange { table, .. } => &table.name,
                _ => continue,
            };
            groups.entry(table_name.clone()).or_default().push(record);
        }
        groups
    }

    /// Extract columns from a CdcRecord variant as &[ColumnValue].
    fn record_columns(record: &CdcRecord) -> &[ColumnValue] {
        match record {
            CdcRecord::Insert { columns, .. } => columns,
            CdcRecord::Update { new_columns, .. } => new_columns,
            CdcRecord::Delete { columns, .. } => columns,
            _ => &[],
        }
    }

    /// Build the USING subquery for a MERGE statement with inline values.
    /// Returns "SELECT val1 AS \"col1\", val2 AS \"col2\" FROM DUAL".
    fn build_using_select(col_values: &[ColumnValue], schema_cols: &[SourceColumn]) -> String {
        let pairs: Vec<String> = schema_cols
            .iter()
            .map(|sc| {
                let val = col_values
                    .iter()
                    .find(|cv| cv.name == sc.name)
                    .map(|cv| &cv.value)
                    .unwrap_or(&Value::Null);
                let expr = value_to_oracle_typed_expr(val, sc);
                let qcol = quote_ident(&sc.name.to_uppercase());
                let mut col = String::with_capacity(expr.len() + qcol.len() + 4);
                col.push_str(&expr);
                col.push_str(" AS ");
                col.push_str(&qcol);
                col
            })
            .collect();
        let mut result = String::with_capacity(128 + pairs.join(", ").len());
        result.push_str("SELECT ");
        result.push_str(&pairs.join(", "));
        result.push_str(" FROM DUAL");
        result
    }

    /// Build a DELETE WHERE clause with PK columns and their inline values.
    fn build_delete_where(col_values: &[ColumnValue], pk_cols: &[String], schema_cols: &[SourceColumn]) -> String {
        let conditions: Vec<String> = pk_cols
            .iter()
            .map(|pk| {
                let val = col_values
                    .iter()
                    .find(|cv| cv.name == *pk)
                    .map(|cv| &cv.value)
                    .unwrap_or(&Value::Null);
                let sch_col = schema_cols.iter().find(|sc| sc.name == *pk);
                let qpk = quote_ident(&pk.to_uppercase());
                match sch_col {
                    Some(sc) => {
                        let val_expr = value_to_oracle_typed_expr(val, sc);
                        let mut cond = String::with_capacity(qpk.len() + val_expr.len() + 4);
                        cond.push_str(&qpk);
                        cond.push_str(" = ");
                        cond.push_str(&val_expr);
                        cond
                    }
                    None => {
                        let val_expr = value_to_oracle_expr(val);
                        let mut cond = String::with_capacity(qpk.len() + val_expr.len() + 4);
                        cond.push_str(&qpk);
                        cond.push_str(" = ");
                        cond.push_str(&val_expr);
                        cond
                    }
                }
            })
            .collect();
        conditions.join(" AND ")
    }
}

#[async_trait]
impl Sink for OracleSink {
    fn name(&self) -> &'static str {
        "oracle"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: false,
            supports_transactions: true,
            loading_model: LoadingModel::Streaming,
            min_batch_size: Some(1),
            max_batch_size: Some(250_000),
            optimal_flush_interval_ms: 10_000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        let connect_string = self.connect_string.clone();
        let user = self.user.clone();
        let password = self.password.clone();

        tokio::task::spawn_blocking(move || {
            let conn = oracle::Connection::connect(&user, &password, &connect_string)
                .with_context(|| format!("OracleSink: failed to connect to {}", connect_string))?;

            // Verify connectivity with a simple query
            let mut stmt = conn.statement("SELECT 1 FROM DUAL").build()?;
            let rows = stmt.query(&[])?;
            match rows.into_iter().next() {
                Some(Ok(_)) => {
                    info!("OracleSink: connection validated successfully");
                    conn.close().ok();
                    Ok(())
                }
                Some(Err(e)) => {
                    anyhow::bail!("OracleSink: query error: {}", e)
                }
                None => anyhow::bail!("OracleSink: SELECT 1 FROM DUAL returned no rows"),
            }
        })
        .await
        .context("Failed to join spawn_blocking for Oracle validation")?
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        info!(
            "OracleSink: setting up {} tables in schema '{}'",
            source_schemas.len(),
            self.schema
        );

        // Cache schemas globally so snapshot worker instances can read them
        SOURCE_SCHEMAS.set(source_schemas.to_vec()).ok();

        // Delegate to setup module
        setup::run_setup(
            &self.connect_string,
            &self.schema,
            &self.job_name,
            &self.user,
            &self.password,
            source_schemas,
        )
        .await
        .context("OracleSink: setup failed")?;

        info!(
            "OracleSink: setup complete ({} tables)",
            source_schemas.len()
        );
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult::default());
        }

        info!("OracleSink: write_batch called with {} records", records.len());

        // Track last position for checkpoint
        let last_position = records.iter().rev().find_map(|r| match r {
            CdcRecord::Insert { position, .. }
            | CdcRecord::Update { position, .. }
            | CdcRecord::Delete { position, .. }
            | CdcRecord::Commit { position, .. }
            | CdcRecord::Heartbeat { position, .. } => Some(position.clone()),
            _ => None,
        });

        let batch_size = records.len();

        // Take existing connection if any (prevents connect churn)
        let existing_conn = self.conn.take();

        // Clone data needed for the blocking call
        let connect_string = self.connect_string.clone();
        let schema = self.schema.clone();
        let user = self.user.clone();
        let password = self.password.clone();
        let source_schemas = SOURCE_SCHEMAS.get().cloned().unwrap_or_default();

        // Execute the batch in spawn_blocking
        let batch_result = tokio::task::spawn_blocking(move || -> Result<(usize, u64, Option<oracle::Connection>)> {
            let conn = match existing_conn {
                Some(c) => c,
                None => oracle::Connection::connect(&user, &password, &connect_string)
                    .with_context(|| format!("OracleSink: failed to connect to {}", connect_string))?,
            };

            let mut total_bytes = 0u64;

            // Group records by table name inside the closure
            let mut group_map: HashMap<String, Vec<CdcRecord>> = HashMap::new();
            for record in records {
                let table_name = match &record {
                    CdcRecord::Insert { table, .. }
                    | CdcRecord::Update { table, .. }
                    | CdcRecord::Delete { table, .. }
                    | CdcRecord::SchemaChange { table, .. } => table.name.clone(),
                    _ => continue,
                };
                group_map.entry(table_name).or_default().push(record);
            }

            for (table_name, table_records) in &group_map {
                // Find schema for this table
                let table_schema = match source_schemas.iter().find(|s| &s.name == table_name) {
                    Some(s) => s,
                    None => {
                        error!("OracleSink: no schema found for table '{}'", table_name);
                        continue;
                    }
                };

                for record in table_records.iter() {
                    let columns = Self::record_columns(record);
                    match record {
                        CdcRecord::Insert { .. } | CdcRecord::Update { .. } => {
                            // Collect column names with Value::Unchanged (PG TOAST columns
                            // not modified in an UPDATE). These must be excluded from the
                            // UPDATE SET clause to avoid overwriting existing values with NULL.
                            let unchanged_cols: Vec<String> = columns.iter()
                                .filter(|cv| matches!(cv.value, Value::Unchanged))
                                .map(|cv| cv.name.clone())
                                .collect();

                            // Generate MERGE statement template
                            let merge_template = generate_merge(&schema, table_schema, &unchanged_cols);
                            // Replace the __USING_SELECT__ placeholder with inline values
                            let using_select = Self::build_using_select(columns, &table_schema.columns);
                            let merge_sql = merge_template.replace("__USING_SELECT__", &using_select);

                            conn.execute(&merge_sql, &[])
                                .with_context(|| {
                                    format!(
                                        "OracleSink: MERGE failed for table '{}'. SQL: {}",
                                        table_name, merge_sql
                                    )
                                })?;
                            total_bytes += 1; // byte estimate per record
                        }
                        CdcRecord::Delete { .. } => {
                            // Generate DELETE with PK WHERE clause
                            let delete_prefix = generate_delete(&schema, table_name);
                            let where_clause = Self::build_delete_where(columns, &table_schema.primary_keys, &table_schema.columns);
                            let delete_sql = format!("{}{}", delete_prefix, where_clause);

                            conn.execute(&delete_sql, &[])
                                .with_context(|| {
                                    format!(
                                        "OracleSink: DELETE failed for table '{}'. SQL: {}",
                                        table_name, delete_sql
                                    )
                                })?;
                            total_bytes += 1;
                        }
                        _ => {} // Skip Begin, Commit, Heartbeat, SchemaChange
                    }
                }
            }

            // Commit the transaction (don't close — return connection for reuse)
            conn.commit()?;

            info!("OracleSink: batch write succeeded: {} tables, {} records", group_map.len(), batch_size);

            Ok((batch_size, total_bytes, Some(conn)))
        })
        .await
        .context("Failed to join spawn_blocking for Oracle batch write")??;

        // Store connection back for reuse
        self.conn = batch_result.2;

        let (records_written, bytes_written) = (batch_result.0, batch_result.1);
        Ok(SinkResult { records_written, bytes_written, last_position, schema_evolution_skipped: 0 })
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(conn) = self.conn.take() {
            tokio::task::spawn_blocking(move || {
                conn.close().ok();
            })
            .await?;
            info!("OracleSink: connection closed");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::position::SourcePosition;
    use crate::core::record::{ColumnValue, DataType, TableRef, Value};

    #[test]
    fn test_sink_name() {
        // Sink name should match the CLI datasource type
        let test_pass = std::env::var("ORACLE_TEST_PASS").unwrap_or_else(|_| "dummy".to_string());
        let sink = OracleSink {
            connect_string: "//localhost:1521/FREEPDB1".to_string(),
            schema: "C##DBMAZZ".to_string(),
            job_name: "test_job".to_string(),
            database: "oracle".to_string(),
            user: "dummy_user".to_string(),
            password: test_pass,
            conn: None,
            mode: SinkMode::Primary,
        };
        assert_eq!(sink.name(), "oracle");
    }

    #[test]
    fn test_capabilities() {
        let test_pass = std::env::var("ORACLE_TEST_PASS").unwrap_or_else(|_| "dummy".to_string());
        let sink = OracleSink {
            connect_string: "//localhost:1521/FREEPDB1".to_string(),
            schema: "C##DBMAZZ".to_string(),
            job_name: "test_job".to_string(),
            database: "oracle".to_string(),
            user: "dummy_user".to_string(),
            password: test_pass,
            conn: None,
            mode: SinkMode::Primary,
        };
        let caps = sink.capabilities();
        assert!(caps.supports_upsert);
        assert!(caps.supports_delete);
        assert!(caps.supports_transactions);
    }

    #[test]
    fn test_group_by_table() {
        let records = vec![
            CdcRecord::Insert {
                table: TableRef::new(Some("PUBLIC".into()), "users".into()),
                columns: vec![],
                position: SourcePosition::Lsn(0x123456),
            },
            CdcRecord::Insert {
                table: TableRef::new(Some("PUBLIC".into()), "orders".into()),
                columns: vec![],
                position: SourcePosition::Lsn(0x123457),
            },
            CdcRecord::Insert {
                table: TableRef::new(Some("PUBLIC".into()), "users".into()),
                columns: vec![],
                position: SourcePosition::Lsn(0x123458),
            },
        ];
        let groups = OracleSink::group_by_table(&records);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.get("users").unwrap().len(), 2);
        assert_eq!(groups.get("orders").unwrap().len(), 1);
    }

    #[test]
    fn test_build_using_select() {
        let col_values = vec![
            ColumnValue::new("id".into(), Value::Int64(42)),
            ColumnValue::new("name".into(), Value::String("Alice".into())),
        ];
        let schema_cols = vec![
            SourceColumn {
                name: "id".to_string(),
                data_type: crate::core::record::DataType::Int64,
                nullable: false,
                pg_type_id: None,
            },
            SourceColumn {
                name: "name".to_string(),
                data_type: crate::core::record::DataType::String,
                nullable: false,
                pg_type_id: None,
            },
        ];
        let result = OracleSink::build_using_select(&col_values, &schema_cols);
        assert!(result.contains("42 AS \"ID\""));
        assert!(result.contains("'Alice' AS \"NAME\""));
        assert!(result.starts_with("SELECT "));
        assert!(result.contains("FROM DUAL"));
    }

    #[test]
    fn test_build_delete_where() {
        let col_values = vec![
            ColumnValue::new("id".into(), Value::Int64(99)),
            ColumnValue::new("name".into(), Value::String("Bob".into())),
        ];
        let pk_cols = vec!["id".to_string()];
        let schema_cols = vec![SourceColumn {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
            pg_type_id: None,
        }];
        let result = OracleSink::build_delete_where(&col_values, &pk_cols, &schema_cols);
        assert_eq!(result, "\"ID\" = 99");
    }

    #[test]
    fn test_record_columns() {
        let columns = vec![ColumnValue::new("x".into(), Value::Int64(1))];
        let insert = CdcRecord::Insert {
            table: TableRef::new(None, "t".into()),
            columns: columns.clone(),
            position: SourcePosition::Lsn(0),
        };
        let result = OracleSink::record_columns(&insert);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "x");

        let update = CdcRecord::Update {
            table: TableRef::new(None, "t".into()),
            old_columns: None,
            new_columns: columns.clone(),
            position: SourcePosition::Lsn(0),
        };
        let result = OracleSink::record_columns(&update);
        assert_eq!(result.len(), 1);

        let delete = CdcRecord::Delete {
            table: TableRef::new(None, "t".into()),
            columns,
            position: SourcePosition::Lsn(0),
        };
        let result = OracleSink::record_columns(&delete);
        assert_eq!(result.len(), 1);
    }
}

