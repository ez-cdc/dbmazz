// Copyright 2025
// Licensed under the Elastic License v2.0

//! SQL Server sink connector.
//!
//! Replicates CDC changes to a target SQL Server database using direct
//! T-SQL MERGE statements with parameterized queries via tiberius.
//!
//! ## CDC Flow
//!
//! ```text
//! write_batch(Vec<CdcRecord>)
//!   ├── SchemaChange → ALTER TABLE ADD COLUMN (within txn)
//!   ├── INSERT/UPDATE → MERGE INTO target (within txn)
//!   ├── DELETE        → DELETE FROM target (within txn)
//!   └── COMMIT (atomic)
//! ```

mod merge_generator;
mod setup;
mod types;

use std::collections::HashMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use tiberius::Client as TiberiusClient;
use tiberius::Config as TiberiusConfig;
use tiberius::ToSql;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{info, warn};

use crate::config::SinkConfig;
use crate::connectors::sinks::schema_evolution::compute_schema_evolution_plan;
use crate::core::record::{
    CdcRecord, ColumnValue, DataType, TableRef, Value,
};
use crate::core::traits::{
    LoadingModel, Sink, SinkCapabilities, SinkMode, SinkResult, SourceTableSchema,
};

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// In-memory schema state keyed by `<schema>.<table>`.
type SchemaState = HashMap<String, SourceTableSchema>;

// ---------------------------------------------------------------------------
// Tiberius parameter helpers
// ---------------------------------------------------------------------------

/// Owned wrapper that can produce `&dyn QueryParam` references for any
/// `Value` variant. Used to build the heterogeneous param list that
/// tiberius expects.
///
/// Each null variant carries the correct TDS type so SQL Server doesn't
/// reject it with "Operand type clash" when the column expects e.g.
/// DATETIME2 (naively sending `Option<i32>` as int NULL).
enum ParamOwned {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    DateTime(NaiveDateTime),
    /// Typed nulls — these MUST match the target column type.
    NullBool,
    NullI16,
    NullI32,
    NullI64,
    NullF64,
    NullString,
    NullBytes,
    NullDateTime,
}

impl ParamOwned {
    /// Return a `&dyn ToSql` referencing the stored value.
    fn as_ref(&self) -> &dyn ToSql {
        match self {
            Self::Bool(v) => v as &dyn ToSql,
            Self::I16(v) => v as &dyn ToSql,
            Self::I32(v) => v as &dyn ToSql,
            Self::I64(v) => v as &dyn ToSql,
            Self::F64(v) => v as &dyn ToSql,
            Self::String(ref v) => v as &dyn ToSql,
            Self::Bytes(ref v) => v as &dyn ToSql,
            Self::DateTime(ref v) => v as &dyn ToSql,
            Self::NullBool => {
                static NULL: Option<bool> = None;
                &NULL as &dyn ToSql
            }
            Self::NullI16 => {
                static NULL: Option<i16> = None;
                &NULL as &dyn ToSql
            }
            Self::NullI32 => {
                static NULL: Option<i32> = None;
                &NULL as &dyn ToSql
            }
            Self::NullI64 => {
                static NULL: Option<i64> = None;
                &NULL as &dyn ToSql
            }
            Self::NullF64 => {
                static NULL: Option<f64> = None;
                &NULL as &dyn ToSql
            }
            Self::NullString => {
                static NULL: Option<String> = None;
                &NULL as &dyn ToSql
            }
            Self::NullBytes => {
                static NULL: Option<Vec<u8>> = None;
                &NULL as &dyn ToSql
            }
            Self::NullDateTime => {
                static NULL: Option<NaiveDateTime> = None;
                &NULL as &dyn ToSql
            }
        }
    }
}

/// Convert a `Value` to the corresponding `ParamOwned`, using the expected
/// column DataType to produce a correctly-typed NULL when the value is null.
fn value_to_param_owned(value: &Value, data_type: Option<&DataType>) -> ParamOwned {
    match value {
        Value::Null => match data_type {
            Some(DataType::Boolean) => ParamOwned::NullBool,
            Some(DataType::Int16) => ParamOwned::NullI16,
            Some(DataType::Int32) => ParamOwned::NullI32,
            Some(DataType::Int64 | DataType::UInt64) => ParamOwned::NullI64,
            Some(DataType::Float32 | DataType::Float64) => ParamOwned::NullF64,
            Some(DataType::Bytes) => ParamOwned::NullBytes,
            Some(DataType::Date | DataType::Time | DataType::Timestamp | DataType::TimestampTz) => {
                ParamOwned::NullDateTime
            }
            // Default: string null — NVARCHAR is the most compatible catch-all.
            _ => ParamOwned::NullString,
        },
        Value::Bool(b) => ParamOwned::Bool(*b),
        Value::Int64(i) => ParamOwned::I64(*i),
        Value::UInt64(u) => ParamOwned::I64(*u as i64),
        Value::Float64(f) => ParamOwned::F64(*f),
        Value::String(s) => ParamOwned::String(s.clone()),
        Value::Bytes(b) => ParamOwned::Bytes(b.clone()),
        Value::Json(j) => ParamOwned::String(j.clone()),
        Value::Timestamp(ts) => {
            // CDC timestamp is epoch microseconds. Convert to NaiveDateTime
            // so tiberius sends a DATETIME2-compatible wire type instead of
            // BIGINT (which clashes with DATETIME2 columns).
            ParamOwned::DateTime(
                NaiveDateTime::from_timestamp_micros(*ts).unwrap_or_default(),
            )
        }
        Value::Decimal(d) => ParamOwned::String(d.clone()),
        Value::Uuid(u) => ParamOwned::String(u.clone()),
        Value::Unchanged => {
            // Unchanged values are excluded from the param list at the
            // call site (they go into unchanged_cols). This arm should
            // never be hit in practice.
            ParamOwned::NullString
        }
    }
}

/// Build the tiberius parameter list from a slice of `ColumnValue` references,
/// using the corresponding column DataTypes to produce correctly-typed NULLs.
/// Returns owned values that can be passed to execute via iter().map(|p| p.as_ref()).
fn build_params(columns: &[&ColumnValue], col_types: &[DataType]) -> Vec<ParamOwned> {
    columns
        .iter()
        .zip(col_types.iter())
        .map(|(cv, dt)| value_to_param_owned(&cv.value, Some(dt)))
        .collect()
}

/// Build the tiberius parameter list from a slice of `ColumnValue`,
/// ordered by a specific list of column names (used for DELETE where
/// only PK columns are passed). Uses the table_schema to look up each
/// column's DataType for correctly-typed NULLs.
fn build_params_for_columns(
    columns: &[ColumnValue],
    col_names: &[String],
    table_schema: &SourceTableSchema,
) -> Vec<ParamOwned> {
    col_names
        .iter()
        .map(|name| {
            let dt = table_schema
                .columns
                .iter()
                .find(|c| c.name == *name)
                .map(|c| c.data_type.clone());
            columns
                .iter()
                .find(|cv| cv.name == *name)
                .map(|cv| value_to_param_owned(&cv.value, dt.as_ref()))
                .unwrap_or_else(|| value_to_param_owned(&Value::Null, dt.as_ref()))
        })
        .collect()
}

/// Estimate the byte size of a `Value` for SinkResult accounting.
fn value_byte_estimate(value: &Value) -> u64 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int64(_) => 8,
        Value::UInt64(_) => 8,
        Value::Float64(_) => 8,
        Value::String(s) => s.len() as u64,
        Value::Bytes(b) => b.len() as u64,
        Value::Json(j) => j.len() as u64,
        Value::Timestamp(_) => 8,
        Value::Decimal(d) => d.len() as u64,
        Value::Uuid(u) => u.len() as u64,
        Value::Unchanged => 0,
    }
}

// ---------------------------------------------------------------------------
// SqlServerSink
// ---------------------------------------------------------------------------

/// SQL Server sink — writes CDC records to a target SQL Server database
/// using direct parameterized T-SQL MERGE statements.
pub struct SqlServerSink {
    /// Target hostname or IP
    url: String,
    /// Target port (default: 1433)
    port: u16,
    /// Target database name
    database: String,
    /// Target schema (default: "dbo")
    schema: String,
    /// Job name for metadata tracking
    job_name: String,
    /// SQL Server login user
    user: String,
    /// SQL Server login password
    password: String,
    /// Lazy connection — established on connect()
    client: Option<TiberiusClient<tokio_util::compat::Compat<TcpStream>>>,
    /// Sink operating mode
    mode: SinkMode,
    /// In-memory schema cache keyed by `<schema>.<table>`.
    /// Updated after each successful schema-evolution batch.
    schema_state: SchemaState,
}

impl SqlServerSink {
    /// Create a new `SqlServerSink` from the provided configuration.
    /// Connection is lazy — established on first use.
    pub fn new(config: &SinkConfig, mode: SinkMode) -> Result<Self> {
        let ss_config = config.sqlserver_config()?;

        info!(
            "SqlServerSink initialized: host={}, port={}, db={}, schema={}, job={}",
            config.url, config.port, config.database, ss_config.schema, ss_config.job_name
        );

        Ok(Self {
            url: config.url.clone(),
            port: config.port,
            database: config.database.clone(),
            schema: ss_config.schema.clone(),
            job_name: ss_config.job_name.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
            client: None,
            mode,
            schema_state: HashMap::new(),
        })
    }

    /// Get or establish connection to target SQL Server.
    async fn connect(&mut self) -> Result<&mut TiberiusClient<tokio_util::compat::Compat<TcpStream>>> {
        if self.client.is_none() {
            let mut tiberius_config = TiberiusConfig::new();
            tiberius_config.host(&self.url);
            tiberius_config.port(self.port);
            tiberius_config.database(&self.database);
            tiberius_config.authentication(tiberius::AuthMethod::sql_server(
                &self.user,
                &self.password,
            ));

            let tcp = TcpStream::connect(format!("{}:{}", self.url, self.port))
                .await
                .context("SqlServerSink: failed to connect TCP to target SQL Server")?;

            let client = TiberiusClient::connect(tiberius_config, tcp.compat())
                .await
                .context("SqlServerSink: failed to establish tiberius connection")?;

            self.client = Some(client);
        }
        Ok(self.client.as_mut().unwrap())
    }

    /// Quote a SQL Server identifier with brackets.
    fn quote_ident(name: &str) -> String {
        let mut out = String::with_capacity(name.len() + 2);
        out.push('[');
        out.push_str(name);
        out.push(']');
        out
    }

    /// Build a quoted `[schema].[table]` string.
    fn quoted_table(schema: &str, table: &str) -> String {
        let mut out = String::with_capacity(schema.len() + table.len() + 4);
        out.push('[');
        out.push_str(schema);
        out.push_str("].[");
        out.push_str(table);
        out.push(']');
        out
    }
}

// ---------------------------------------------------------------------------
// Sink trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Sink for SqlServerSink {
    fn name(&self) -> &'static str {
        "sqlserver"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: true,
            supports_transactions: true,
            loading_model: LoadingModel::Streaming,
            min_batch_size: Some(1),
            max_batch_size: Some(5000),
            optimal_flush_interval_ms: 5000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        let mut tiberius_config = TiberiusConfig::new();
        tiberius_config.host(&self.url);
        tiberius_config.port(self.port);
        tiberius_config.database(&self.database);
        tiberius_config.authentication(tiberius::AuthMethod::sql_server(
            &self.user,
            &self.password,
        ));

        let tcp = TcpStream::connect(format!("{}:{}", self.url, self.port))
            .await
            .context("SqlServerSink: validation TCP connect failed")?;

        let mut client = TiberiusClient::connect(tiberius_config, tcp.compat())
            .await
            .context("SqlServerSink: validation tiberius connect failed")?;

        // Run SELECT 1 to verify connectivity
        let rows = client
            .query("SELECT 1 AS check_val", &[])
            .await
            .context("SqlServerSink: validation query failed")?;
        let _ = rows.into_first_result().await?;

        info!(
            "SqlServerSink: connection OK (host={}:{}, db={}, schema={})",
            self.url, self.port, self.database, self.schema
        );

        // Disconnect by letting client drop out of scope
        drop(client);
        Ok(())
    }

    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        // Populate initial schema state from source schemas.
        for src in source_schemas {
            let qn = format!("{}.{}", src.schema, src.name);
            self.schema_state.insert(qn, src.clone());
        }

        let schema = self.schema.clone();
        let job_name = self.job_name.clone();
        let client = self.connect().await?;
        setup::run_setup(client, &schema, &job_name, source_schemas)
            .await
            .context("SqlServerSink: setup failed")?;

        info!(
            "SqlServerSink: setup complete ({} tables, schema={}, job={})",
            source_schemas.len(),
            self.schema,
            self.job_name
        );
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult::default());
        }

        // Clone fields that will be needed below while we can borrow self.
        let schema = self.schema.clone();

        // ── Step 1: Compute schema evolution plan ──
        let (new_schema_state, pending_diffs) =
            compute_schema_evolution_plan(&self.schema_state, &records);

        // ── Step 2: Establish connection ──
        let client = self.connect().await?;

        // ── Step 3: Start transaction ──
        client
            .simple_query("BEGIN TRANSACTION")
            .await
            .context("SqlServerSink: failed to begin transaction")?;

        // ── Step 4: Apply schema evolution DDL ──
        for (table_ref, diff) in &pending_diffs {
            for added in &diff.added {
                let target_schema = &schema;
                let quoted = Self::quoted_table(target_schema, &table_ref.name);
                let col_name = Self::quote_ident(&added.name);
                let col_type = types::data_type_to_sqlserver(&added.data_type);

                let mut alter_sql = String::with_capacity(128);
                alter_sql.push_str("ALTER TABLE ");
                alter_sql.push_str(&quoted);
                alter_sql.push_str(" ADD ");
                alter_sql.push_str(&col_name);
                alter_sql.push(' ');
                alter_sql.push_str(col_type);

                if added.nullable {
                    alter_sql.push_str(" NULL");
                } else {
                    alter_sql.push_str(" NULL"); // New columns must be nullable on existing tables
                }

                client
                    .simple_query(&alter_sql)
                    .await
                    .with_context(|| {
                        format!(
                            "SqlServerSink: failed to add column {}.{}",
                            target_schema, added.name
                        )
                    })?;

                info!(
                    "Schema evolution: added column [{}.{}] {} to {}",
                    target_schema, added.name, col_type, quoted
                );
            }
        }

        // ── Step 5: Group data records by table ──
        // Only INSERT, UPDATE, DELETE records are data records.
        let mut data_records: Vec<(&TableRef, &CdcRecord)> = Vec::new();
        for record in &records {
            match record {
                CdcRecord::Insert { table, .. }
                | CdcRecord::Update { table, .. }
                | CdcRecord::Delete { table, .. } => {
                    data_records.push((table, record));
                }
                _ => {} // Skip Begin, Commit, Heartbeat, SchemaChange
            }
        }

        // Group by table qualified key (owned String keeps group keys alive).
        let mut by_table: HashMap<String, Vec<&CdcRecord>> = HashMap::new();
        for (table, record) in &data_records {
            let src_schema = table.schema.as_deref().unwrap_or("public");
            let qn = format!("{}.{}", src_schema, table.name);
            by_table.entry(qn).or_default().push(record);
        }

        // ── Step 6: Execute MERGE/DELETE per table group ──
        let mut records_written: usize = 0;
        let mut bytes_written: u64 = 0;

        for (qn, group) in &by_table {
            // Extract table ref from the first record in the group
            let first_table = match group.first() {
                Some(CdcRecord::Insert { table, .. })
                | Some(CdcRecord::Update { table, .. })
                | Some(CdcRecord::Delete { table, .. }) => table,
                _ => continue,
            };

            // Look up schema info from the updated schema state.
            let table_schema = match new_schema_state.get(qn.as_str()) {
                Some(s) => s,
                None => {
                    warn!(
                        "SqlServerSink: no schema found for {}, skipping group",
                        qn
                    );
                    continue;
                }
            };

            let pk_cols: Vec<String> = table_schema.primary_keys.clone();

            let quoted_schema = Self::quote_ident(&schema);
            let quoted_table = Self::quote_ident(&first_table.name);

            for record in group {
                match record {
                    CdcRecord::Insert { columns, .. } => {
                        // Identify unchanged columns (PG TOAST).
                        let unchanged: Vec<String> = columns
                            .iter()
                            .filter(|cv| cv.value.is_unchanged())
                            .map(|cv| cv.name.clone())
                            .collect();

                        // Collect non-unchanged column values for parameter binding.
                        let data_cols: Vec<&ColumnValue> = columns
                            .iter()
                            .filter(|cv| !cv.value.is_unchanged())
                            .collect();

                        // Build ColumnInfo for non-unchanged columns (same order as data_cols).
                        let merge_cols: Vec<merge_generator::ColumnInfo> = data_cols
                            .iter()
                            .map(|cv| {
                                let dt = table_schema
                                    .columns
                                    .iter()
                                    .find(|c| c.name == cv.name)
                                    .map(|c| c.data_type.clone())
                                    .unwrap_or(DataType::String);
                                merge_generator::ColumnInfo {
                                    name: cv.name.clone(),
                                    data_type: dt,
                                }
                            })
                            .collect();

                        let pk_for_merge: Vec<String> = pk_cols.clone();
                        let (sql, _) = merge_generator::generate_merge(
                            &quoted_table,
                            &quoted_schema,
                            &merge_cols,
                            &pk_for_merge,
                            &unchanged,
                        );

                        // Build params from data columns only (unchanged excluded).
                        let col_types: Vec<DataType> =
                            merge_cols.iter().map(|ci| ci.data_type.clone()).collect();
                        let owned_params = build_params(&data_cols, &col_types);
                        let param_refs: Vec<&dyn ToSql> = owned_params.iter().map(|p| p.as_ref()).collect();
                        let table_name = first_table.name.clone();
                        let sch = schema.clone();

                        // Log the MERGE SQL and param count for debugging
                        tracing::debug!(
                            "SqlServerSink: MERGE SQL ({} params): {}",
                            param_refs.len(),
                            sql
                        );

                        client
                            .execute(&sql, &param_refs)
                            .await
                            .with_context(|| {
                                format!(
                                    "SqlServerSink: MERGE failed for {}.{}",
                                    sch, table_name
                                )
                            })?;

                        // Log MERGE success
                        tracing::debug!(
                            "SqlServerSink: MERGE success for {}.{} ({}/{} params)",
                            sch, table_name,
                            param_refs.len(),
                            // param_count from generate_merge — we don't store it
                            "(from generate_merge)",
                        );

                        records_written += 1;
                        for cv in columns {
                            bytes_written =
                                bytes_written.saturating_add(value_byte_estimate(&cv.value));
                        }
                    }
                    CdcRecord::Update { new_columns, .. } => {
                        // Identify unchanged columns (PG TOAST).
                        let unchanged: Vec<String> = new_columns
                            .iter()
                            .filter(|cv| cv.value.is_unchanged())
                            .map(|cv| cv.name.clone())
                            .collect();

                        // Collect non-unchanged column values for parameter binding.
                        let data_cols: Vec<&ColumnValue> = new_columns
                            .iter()
                            .filter(|cv| !cv.value.is_unchanged())
                            .collect();

                        // Build ColumnInfo for non-unchanged columns (same order as data_cols).
                        let merge_cols: Vec<merge_generator::ColumnInfo> = data_cols
                            .iter()
                            .map(|cv| {
                                let dt = table_schema
                                    .columns
                                    .iter()
                                    .find(|c| c.name == cv.name)
                                    .map(|c| c.data_type.clone())
                                    .unwrap_or(DataType::String);
                                merge_generator::ColumnInfo {
                                    name: cv.name.clone(),
                                    data_type: dt,
                                }
                            })
                            .collect();

                        let pk_for_merge: Vec<String> = pk_cols.clone();
                        let (sql, _) = merge_generator::generate_merge(
                            &quoted_table,
                            &quoted_schema,
                            &merge_cols,
                            &pk_for_merge,
                            &unchanged,
                        );

                        let col_types: Vec<DataType> =
                            merge_cols.iter().map(|ci| ci.data_type.clone()).collect();
                        let owned_params = build_params(&data_cols, &col_types);
                        let param_refs: Vec<&dyn ToSql> = owned_params.iter().map(|p| p.as_ref()).collect();

                        let table_name = first_table.name.clone();
                        let sch = schema.clone();

                        // Log the MERGE SQL and param count for debugging
                        tracing::debug!(
                            "SqlServerSink: MERGE SQL ({} params): {}",
                            param_refs.len(),
                            sql
                        );

                        client
                            .execute(&sql, &param_refs)
                            .await
                            .with_context(|| {
                                format!(
                                    "SqlServerSink: MERGE failed for {}.{}",
                                    sch, table_name
                                )
                            })?;

                        // Log MERGE success
                        tracing::debug!(
                            "SqlServerSink: MERGE success for {}.{} ({}/{} params)",
                            sch, table_name,
                            param_refs.len(),
                            // param_count from generate_merge — we don't store it
                            "(from generate_merge)",
                        );

                        records_written += 1;
                        for cv in new_columns {
                            bytes_written =
                                bytes_written.saturating_add(value_byte_estimate(&cv.value));
                        }
                    }
                    CdcRecord::Delete { columns, .. } => {
                        let pk_cols_ref: Vec<String> = pk_cols.clone();
                        let sql = merge_generator::generate_delete(
                            &quoted_table,
                            &quoted_schema,
                            &pk_cols_ref,
                        );

                        // Build params from PK columns only.
                        let owned_params = build_params_for_columns(columns, &pk_cols, table_schema);
                        let param_refs: Vec<&dyn ToSql> = owned_params.iter().map(|p| p.as_ref()).collect();

                        let table_name = first_table.name.clone();
                        let sch = schema.clone();

                        client
                            .execute(&sql, &param_refs)
                            .await
                            .with_context(|| {
                                format!(
                                    "SqlServerSink: DELETE failed for {}.{}",
                                    sch, table_name
                                )
                            })?;

                        records_written += 1;
                        for cv in columns {
                            bytes_written =
                                bytes_written.saturating_add(value_byte_estimate(&cv.value));
                        }
                    }
                    _ => {}
                }
            }
        }

        // ── Step 7: Commit transaction ──
        client
            .simple_query("COMMIT")
            .await
            .context("SqlServerSink: failed to commit transaction")?;

        // ── Step 8: Update schema state ──
        self.schema_state = new_schema_state;

        info!(
            "SqlServerSink: batch committed ({} records, {} bytes)",
            records_written, bytes_written
        );

        Ok(SinkResult {
            records_written,
            bytes_written,
            schema_evolution_skipped: 0,
        })
    }

    async fn close(&mut self) -> Result<()> {
        info!("SqlServerSink: closing...");
        self.client = None;
        info!("SqlServerSink: closed");
        Ok(())
    }
}
