//! PostgreSQL source client.
//!
//! Connects to the source PG instance exposed by the e2e compose stack.
//! Used by the verify runner to seed data, apply CDC operations
//! (INSERT/UPDATE/DELETE), and read back for comparison with the target.

use std::collections::HashMap;

use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::{types::ToSql, Client, NoTls};

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("not connected: call connect() first")]
    NotConnected,

    #[error("postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("identifier contains a double-quote: {0:?}")]
    BadIdentifier(String),

    #[error("INSERT returned no row")]
    NoReturningRow,

    #[error("{0}")]
    Other(String),
}

// ── SqlValue ────────────────────────────────────────────────────────────────

/// Simple enum wrapping common types used in e2e test operations.
///
/// Since values coming from test operations are simple types (i64, String,
/// Option<String>), this enum covers the necessary cases without needing
/// a fully generic parameter system.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
}

impl ToSql for SqlValue {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use tokio_postgres::types::Type;
        match self {
            SqlValue::Null => Ok(tokio_postgres::types::IsNull::Yes),
            SqlValue::Int(v) => {
                // Serialize as text — the SQL uses ::bigint cast.
                // This avoids type mismatches (i64 vs INT4, etc).
                v.to_string().to_sql(ty, out)
            }
            SqlValue::Float(v) => {
                // Always serialize as text string — the SQL uses ::numeric cast
                // to convert. Binary FLOAT8 format is not compatible with NUMERIC.
                v.to_string().to_sql(ty, out)
            }
            SqlValue::Text(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

// ── Identifier quoting ──────────────────────────────────────────────────────

/// Quote a PostgreSQL identifier with double quotes.
///
/// Rejects identifiers containing double-quotes to prevent SQL injection.
/// Test inputs come from the runner's own constants, so this is
/// defense-in-depth, not input sanitization.
pub fn quote_ident(name: &str) -> Result<String, SourceError> {
    if name.contains('"') {
        return Err(SourceError::BadIdentifier(name.to_string()));
    }
    Ok(format!("\"{name}\""))
}

/// Render a SqlValue as a SQL literal for inline use in simple_query.
/// Returns `NULL`, a number literal, or a single-quoted escaped string.
fn sql_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".into(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

// ── Source trait ─────────────────────────────────────────────────────────────

/// Contract for the source database driver used by e2e tests.
#[async_trait]
pub trait SourceClient: Send + Sync {
    /// Open the connection. Must be called before any other method.
    async fn connect(&mut self) -> anyhow::Result<()>;

    /// Close the connection. Safe to call multiple times.
    async fn close(&mut self) -> anyhow::Result<()>;

    /// Human-readable name for error messages (e.g., "postgres").
    fn name(&self) -> &str;

    // ── reads ───────────────────────────────────────────────────────────

    /// Return total row count of a table.
    async fn count_rows(&self, table: &str) -> anyhow::Result<i64>;

    /// Return a single column value for a row, or None if not found.
    async fn fetch_value(
        &self,
        table: &str,
        pk_col: &str,
        pk_val: &SqlValue,
        column: &str,
    ) -> anyhow::Result<Option<SqlValue>>;

    /// Return all PK values in the table, sorted.
    async fn list_primary_keys(&self, table: &str, pk_col: &str) -> anyhow::Result<Vec<SqlValue>>;

    // ── writes ──────────────────────────────────────────────────────────

    /// Insert a single row. Returns the generated PK value.
    async fn insert_row(
        &self,
        table: &str,
        values: &HashMap<String, SqlValue>,
        pk_col: &str,
    ) -> anyhow::Result<SqlValue>;

    /// Insert multiple rows in a single transaction. Returns all generated PKs.
    async fn insert_many(
        &self,
        table: &str,
        rows: &[HashMap<String, SqlValue>],
        pk_col: &str,
    ) -> anyhow::Result<Vec<SqlValue>>;

    /// Update one or more columns of a row identified by PK.
    async fn update_row(
        &self,
        table: &str,
        pk_col: &str,
        pk_val: &SqlValue,
        set_values: &HashMap<String, SqlValue>,
    ) -> anyhow::Result<()>;

    /// Delete a row by PK.
    async fn delete_row(
        &self,
        table: &str,
        pk_col: &str,
        pk_val: &SqlValue,
    ) -> anyhow::Result<()>;

    /// Return column names of a table (from information_schema), ordered by position.
    async fn list_columns(&self, table: &str) -> anyhow::Result<Vec<String>>;

    // ── schema operations ───────────────────────────────────────────────

    /// Idempotent ALTER TABLE ADD COLUMN IF NOT EXISTS.
    async fn ensure_column(
        &self,
        table: &str,
        column: &str,
        sql_type: &str,
    ) -> anyhow::Result<()>;

    /// ALTER TABLE ADD COLUMN (non-idempotent). Used for schema evolution testing.
    async fn alter_add_column(
        &self,
        table: &str,
        column: &str,
        sql_type: &str,
    ) -> anyhow::Result<()>;

    /// ALTER TABLE DROP COLUMN IF EXISTS.
    async fn drop_column_if_exists(&self, table: &str, column: &str) -> anyhow::Result<()>;
}

// ── PostgresSource ──────────────────────────────────────────────────────────

/// PostgreSQL source client using `tokio-postgres`.
pub struct PostgresSource {
    dsn: String,
    client: Option<Client>,
    /// The connection task handle. Kept alive so the connection stays open.
    _conn_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Custom Debug that redacts the connection string.
impl std::fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSource")
            .field("dsn", &"***")
            .field("connected", &self.client.is_some())
            .finish()
    }
}

impl PostgresSource {
    pub fn new(dsn: &str) -> Self {
        Self {
            dsn: dsn.to_string(),
            client: None,
            _conn_handle: None,
        }
    }

    fn require_client(&self) -> Result<&Client, SourceError> {
        self.client.as_ref().ok_or(SourceError::NotConnected)
    }
}

#[async_trait]
impl SourceClient for PostgresSource {
    async fn connect(&mut self) -> anyhow::Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.dsn, NoTls).await?;

        // Spawn the connection handler in the background.
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres source connection error: {e}");
            }
        });

        self.client = Some(client);
        self._conn_handle = Some(handle);
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        // Dropping the client closes the connection; the spawned task will finish.
        self.client = None;
        if let Some(handle) = self._conn_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "postgres"
    }

    // ── reads ───────────────────────────────────────────────────────────

    async fn count_rows(&self, table: &str) -> anyhow::Result<i64> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let sql = format!("SELECT count(*) FROM {tbl}");
        let row = client.query_one(&sql, &[]).await?;
        let count: i64 = row.get(0);
        Ok(count)
    }

    async fn fetch_value(
        &self,
        table: &str,
        pk_col: &str,
        pk_val: &SqlValue,
        column: &str,
    ) -> anyhow::Result<Option<SqlValue>> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let pk = quote_ident(pk_col)?;
        let col = quote_ident(column)?;
        let sql = format!(
            "SELECT {col} FROM {tbl} WHERE {pk} = {}",
            sql_literal(pk_val)
        );
        let msgs = client.simple_query(&sql).await?;
        for msg in &msgs {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                return match row.get(0) {
                    Some(val) => {
                        if let Ok(i) = val.parse::<i64>() {
                            Ok(Some(SqlValue::Int(i)))
                        } else {
                            Ok(Some(SqlValue::Text(val.to_string())))
                        }
                    }
                    None => Ok(Some(SqlValue::Null)),
                };
            }
        }
        Ok(None)
    }

    async fn list_primary_keys(&self, table: &str, pk_col: &str) -> anyhow::Result<Vec<SqlValue>> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let pk = quote_ident(pk_col)?;
        let sql = format!("SELECT {pk} FROM {tbl} ORDER BY {pk}");
        let rows = client.query(&sql, &[]).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Ok(v) = row.try_get::<_, i32>(0) {
                result.push(SqlValue::Int(v as i64));
            } else if let Ok(v) = row.try_get::<_, i64>(0) {
                result.push(SqlValue::Int(v));
            } else if let Ok(v) = row.try_get::<_, String>(0) {
                result.push(SqlValue::Text(v));
            }
        }
        Ok(result)
    }

    async fn list_columns(&self, table: &str) -> anyhow::Result<Vec<String>> {
        let client = self.require_client()?;
        let sql = "SELECT column_name FROM information_schema.columns \
                   WHERE table_schema = 'public' AND table_name = $1 \
                   ORDER BY ordinal_position";
        let rows = client.query(sql, &[&table]).await?;
        let mut cols = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: String = row.get(0);
            cols.push(name);
        }
        Ok(cols)
    }

    // ── writes ──────────────────────────────────────────────────────────

    async fn insert_row(
        &self,
        table: &str,
        values: &HashMap<String, SqlValue>,
        pk_col: &str,
    ) -> anyhow::Result<SqlValue> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let pk_quoted = quote_ident(pk_col)?;

        let cols: Vec<&String> = values.keys().collect();
        let col_list: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect::<Result<Vec<_>, _>>()?;
        let col_str = col_list.join(", ");

        let val_list: Vec<String> = cols.iter().map(|c| sql_literal(&values[*c])).collect();
        let val_str = val_list.join(", ");

        let sql = format!("INSERT INTO {tbl} ({col_str}) VALUES ({val_str}) RETURNING {pk_quoted}");

        // Use simple_query (text protocol) to avoid binary type mismatches.
        let rows = client.simple_query(&sql).await?;
        for msg in &rows {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                if let Some(val) = row.get(0) {
                    // Try to parse as i64 first.
                    if let Ok(i) = val.parse::<i64>() {
                        return Ok(SqlValue::Int(i));
                    }
                    return Ok(SqlValue::Text(val.to_string()));
                }
            }
        }
        Err(SourceError::NoReturningRow.into())
    }

    async fn insert_many(
        &self,
        table: &str,
        rows: &[HashMap<String, SqlValue>],
        pk_col: &str,
    ) -> anyhow::Result<Vec<SqlValue>> {
        if rows.is_empty() {
            return Ok(vec![]);
        }

        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let pk_quoted = quote_ident(pk_col)?;

        let cols: Vec<&String> = rows[0].keys().collect();
        let col_list: Vec<String> = cols.iter().map(|c| quote_ident(c)).collect::<Result<Vec<_>, _>>()?;
        let col_str = col_list.join(", ");

        client.simple_query("BEGIN").await?;

        let mut pks = Vec::with_capacity(rows.len());
        let result = async {
            for r in rows {
                let val_list: Vec<String> = cols.iter().map(|c| sql_literal(&r[*c])).collect();
                let val_str = val_list.join(", ");
                let sql = format!(
                    "INSERT INTO {tbl} ({col_str}) VALUES ({val_str}) RETURNING {pk_quoted}"
                );
                let msgs = client.simple_query(&sql).await?;
                let mut found = false;
                for msg in &msgs {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                        if let Some(val) = row.get(0) {
                            if let Ok(i) = val.parse::<i64>() {
                                pks.push(SqlValue::Int(i));
                            } else {
                                pks.push(SqlValue::Text(val.to_string()));
                            }
                            found = true;
                        }
                    }
                }
                if !found {
                    return Err(SourceError::NoReturningRow.into());
                }
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match result {
            Ok(()) => {
                client.simple_query("COMMIT").await?;
                Ok(pks)
            }
            Err(e) => {
                let _ = client.simple_query("ROLLBACK").await;
                Err(e)
            }
        }
    }

    async fn update_row(
        &self,
        table: &str,
        pk_col: &str,
        pk_val: &SqlValue,
        set_values: &HashMap<String, SqlValue>,
    ) -> anyhow::Result<()> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let pk = quote_ident(pk_col)?;

        let assignments: Vec<String> = set_values
            .iter()
            .map(|(c, v)| {
                let quoted = quote_ident(c)?;
                Ok(format!("{quoted} = {}", sql_literal(v)))
            })
            .collect::<Result<Vec<_>, SourceError>>()?;
        let assign_str = assignments.join(", ");

        let sql = format!("UPDATE {tbl} SET {assign_str} WHERE {pk} = {}", sql_literal(pk_val));
        client.simple_query(&sql).await?;
        Ok(())
    }

    async fn delete_row(
        &self,
        table: &str,
        pk_col: &str,
        pk_val: &SqlValue,
    ) -> anyhow::Result<()> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let pk = quote_ident(pk_col)?;
        let sql = format!("DELETE FROM {tbl} WHERE {pk} = {}", sql_literal(pk_val));
        client.simple_query(&sql).await?;
        Ok(())
    }

    // ── schema operations ───────────────────────────────────────────────

    async fn ensure_column(
        &self,
        table: &str,
        column: &str,
        sql_type: &str,
    ) -> anyhow::Result<()> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let col = quote_ident(column)?;
        // sql_type is a SQL type string like "TEXT" -- not user input in the
        // e2e context, comes from test constants.
        let sql = format!("ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} {sql_type}");
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    async fn alter_add_column(
        &self,
        table: &str,
        column: &str,
        sql_type: &str,
    ) -> anyhow::Result<()> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let col = quote_ident(column)?;
        let sql = format!("ALTER TABLE {tbl} ADD COLUMN {col} {sql_type}");
        client.execute(&sql, &[]).await?;
        Ok(())
    }

    async fn drop_column_if_exists(&self, table: &str, column: &str) -> anyhow::Result<()> {
        let client = self.require_client()?;
        let tbl = quote_ident(table)?;
        let col = quote_ident(column)?;
        let sql = format!("ALTER TABLE {tbl} DROP COLUMN IF EXISTS {col}");
        client.execute(&sql, &[]).await?;
        Ok(())
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_normal() {
        assert_eq!(quote_ident("orders").unwrap(), "\"orders\"");
        assert_eq!(quote_ident("order_items").unwrap(), "\"order_items\"");
    }

    #[test]
    fn quote_ident_rejects_double_quote() {
        assert!(quote_ident("bad\"name").is_err());
    }

    #[test]
    fn sql_value_to_sql_is_implemented() {
        // Ensure SqlValue implements ToSql (compile-time check).
        fn _assert_to_sql<T: ToSql>() {}
        _assert_to_sql::<SqlValue>();
    }

    #[test]
    fn postgres_source_debug_redacts_dsn() {
        let src = PostgresSource::new("postgres://user:secret@localhost/db");
        let debug = format!("{src:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("***"));
    }
}
