//! PostgreSQL target backend.
//!
//! The dbmazz Postgres sink uses a COPY -> raw table -> MERGE normalizer pattern.
//! Target tables live in the configured schema (default `public`) with audit
//! columns `_dbmazz_synced_at` and `_dbmazz_op_type`. Metadata is tracked in
//! `_dbmazz._metadata`. Delete semantics are HARD delete: the MERGE removes
//! rows that correspond to CDC DELETE events.

use std::collections::HashMap;

use async_trait::async_trait;
use tokio_postgres::{types::ToSql, Client, NoTls, Row};

use super::{BackendCapabilities, ColumnInfo, SqlValue, TargetBackend};

const METADATA_SCHEMA: &str = "_dbmazz";
const METADATA_TABLE: &str = "_metadata";

/// Quote a PostgreSQL identifier with double-quotes.
fn quote(name: &str) -> Result<String, anyhow::Error> {
    if name.contains('"') {
        anyhow::bail!("identifier contains double-quote: {name:?}");
    }
    Ok(format!("\"{name}\""))
}

/// Convert a `SqlValue` to a boxed `ToSql` trait object for use as a query parameter.
fn sql_value_param(v: &SqlValue) -> Box<dyn ToSql + Sync + Send> {
    match v {
        SqlValue::Null => Box::new(Option::<String>::None),
        SqlValue::Bool(b) => Box::new(*b),
        SqlValue::Int(i) => Box::new(*i),
        SqlValue::Float(f) => Box::new(*f),
        SqlValue::Text(s) => Box::new(s.clone()),
    }
}

/// Extract a `SqlValue` from a row column, trying common types.
fn extract_value(row: &Row, idx: usize) -> SqlValue {
    if let Ok(v) = row.try_get::<_, i64>(idx) {
        return SqlValue::Int(v);
    }
    if let Ok(v) = row.try_get::<_, i32>(idx) {
        return SqlValue::Int(v as i64);
    }
    if let Ok(v) = row.try_get::<_, bool>(idx) {
        return SqlValue::Bool(v);
    }
    if let Ok(v) = row.try_get::<_, f64>(idx) {
        return SqlValue::Float(v);
    }
    if let Ok(v) = row.try_get::<_, String>(idx) {
        return SqlValue::Text(v);
    }
    // Check for NULL
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        if v.is_none() {
            return SqlValue::Null;
        }
    }
    SqlValue::Null
}

/// PostgreSQL target backend.
pub struct PostgresTarget {
    dsn: String,
    schema: String,
    client: Option<Client>,
    _conn_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Custom Debug that redacts the DSN.
impl std::fmt::Debug for PostgresTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresTarget")
            .field("dsn", &"***")
            .field("schema", &self.schema)
            .field("connected", &self.client.is_some())
            .finish()
    }
}

impl PostgresTarget {
    pub fn new(dsn: &str, schema: &str) -> Self {
        Self {
            dsn: dsn.to_string(),
            schema: schema.to_string(),
            client: None,
            _conn_handle: None,
        }
    }

    fn require_client(&self) -> anyhow::Result<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PostgresTarget.connect() was not called"))
    }

    fn qualified(&self, table: &str) -> Result<String, anyhow::Error> {
        Ok(format!("{}.{}", quote(&self.schema)?, quote(table)?))
    }
}

#[async_trait]
impl TargetBackend for PostgresTarget {
    async fn connect(&mut self) -> anyhow::Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.dsn, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres target connection error: {e}");
            }
        });
        self.client = Some(client);
        self._conn_handle = Some(handle);
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.client = None;
        if let Some(handle) = self._conn_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "postgres"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_hard_delete: true,
            supports_schema_evolution: true,
            supports_arrays: true,
            supports_enum: true,
            has_metadata_table: true,
            supports_hash_compare_sql: true,
            post_cdc_settle_seconds: 5.0,
            post_snapshot_settle_seconds: 3.0,
        }
    }

    fn expected_audit_columns(&self) -> Vec<String> {
        vec![
            "_dbmazz_synced_at".to_string(),
            "_dbmazz_op_type".to_string(),
        ]
    }

    // ── schema inspection ───────────────────────────────────────────────

    async fn list_tables(&self) -> anyhow::Result<Vec<String>> {
        let client = self.require_client()?;
        let sql = r#"
            SELECT tablename FROM pg_tables
            WHERE schemaname = $1
              AND tablename NOT LIKE '\_dbmazz\_%' ESCAPE '\'
              AND tablename NOT LIKE '\_raw\_%' ESCAPE '\'
            ORDER BY tablename
        "#;
        let rows = client.query(sql, &[&self.schema]).await?;
        Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
    }

    async fn table_exists(&self, table: &str) -> anyhow::Result<bool> {
        let client = self.require_client()?;
        let sql = "SELECT 1 FROM pg_tables WHERE schemaname = $1 AND tablename = $2";
        let rows = client.query(sql, &[&self.schema, &table]).await?;
        Ok(!rows.is_empty())
    }

    async fn get_columns(&self, table: &str) -> anyhow::Result<Vec<ColumnInfo>> {
        let client = self.require_client()?;
        let sql = r#"
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        "#;
        let rows = client.query(sql, &[&self.schema, &table]).await?;
        Ok(rows
            .iter()
            .map(|r| {
                let name: String = r.get(0);
                let sql_type: String = r.get(1);
                let nullable_str: String = r.get(2);
                ColumnInfo {
                    name,
                    sql_type: sql_type.to_uppercase(),
                    nullable: nullable_str == "YES",
                }
            })
            .collect())
    }

    async fn metadata_row_count(&self) -> anyhow::Result<i64> {
        let client = self.require_client()?;
        let sql = format!(
            "SELECT count(*) FROM {}.{}",
            quote(METADATA_SCHEMA)?,
            quote(METADATA_TABLE)?
        );
        let row = client.query_one(&sql, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    // ── row counting ────────────────────────────────────────────────────

    async fn count_rows(&self, table: &str, _exclude_deleted: bool) -> anyhow::Result<i64> {
        // Postgres uses hard delete, so exclude_deleted is a no-op.
        let client = self.require_client()?;
        let sql = format!("SELECT count(*) FROM {}", self.qualified(table)?);
        let row = client.query_one(&sql, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn count_duplicates_by_pk(&self, table: &str, pk_column: &str) -> anyhow::Result<i64> {
        let client = self.require_client()?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!(
            "SELECT count(*) FROM (\
               SELECT {pk} FROM {qual} \
               GROUP BY {pk} HAVING count(*) > 1\
             ) dupes"
        );
        let row = client.query_one(&sql, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn list_primary_keys(
        &self,
        table: &str,
        pk_column: &str,
    ) -> anyhow::Result<Vec<SqlValue>> {
        let client = self.require_client()?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT {pk} FROM {qual} ORDER BY {pk}");
        let rows = client.query(&sql, &[]).await?;
        Ok(rows.iter().map(|r| extract_value(r, 0)).collect())
    }

    // ── row queries ─────────────────────────────────────────────────────

    async fn row_exists(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
    ) -> anyhow::Result<bool> {
        let client = self.require_client()?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT 1 FROM {qual} WHERE {pk} = $1");
        let param = sql_value_param(pk_value);
        let rows = client.query(&sql, &[&*param]).await?;
        Ok(!rows.is_empty())
    }

    async fn row_is_live(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
    ) -> anyhow::Result<bool> {
        // Hard-delete backend: live == exists.
        self.row_exists(table, pk_column, pk_value).await
    }

    async fn fetch_value(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
        column: &str,
    ) -> anyhow::Result<Option<SqlValue>> {
        let client = self.require_client()?;
        let pk = quote(pk_column)?;
        let col = quote(column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT {col} FROM {qual} WHERE {pk} = $1");
        let param = sql_value_param(pk_value);
        let rows = client.query(&sql, &[&*param]).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(extract_value(&rows[0], 0)))
    }

    async fn fetch_row(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
    ) -> anyhow::Result<Option<HashMap<String, SqlValue>>> {
        let client = self.require_client()?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT * FROM {qual} WHERE {pk} = $1");
        let param = sql_value_param(pk_value);
        let rows = client.query(&sql, &[&*param]).await?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = &rows[0];
        let mut map = HashMap::new();
        for (i, col) in row.columns().iter().enumerate() {
            map.insert(col.name().to_string(), extract_value(row, i));
        }
        Ok(Some(map))
    }

    // ── cleanup ─────────────────────────────────────────────────────────

    async fn clean(&self, tables: &[String]) -> anyhow::Result<Vec<String>> {
        let client = self.require_client()?;
        let mut actions = Vec::new();

        let audit_cols = ["_dbmazz_synced_at", "_dbmazz_op_type"];
        for table in tables {
            if !self.table_exists(table).await? {
                continue;
            }
            let qual = self.qualified(table)?;
            client
                .execute(&format!("TRUNCATE TABLE {qual}"), &[])
                .await?;
            actions.push(format!("truncated {table}"));

            for col in &audit_cols {
                let col_quoted = quote(col)?;
                // Ignore errors (column may not exist).
                let sql = format!("ALTER TABLE {qual} DROP COLUMN IF EXISTS {col_quoted}");
                let _ = client.execute(&sql, &[]).await;
            }
            actions.push(format!("dropped audit columns from {table}"));
        }

        // Drop _dbmazz schema (metadata, raw tables).
        let schema_quoted = quote(METADATA_SCHEMA)?;
        match client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {schema_quoted} CASCADE"),
                &[],
            )
            .await
        {
            Ok(_) => actions.push("dropped _dbmazz schema (metadata, raw tables)".to_string()),
            Err(e) => actions.push(format!("failed to drop _dbmazz schema: {e}")),
        }

        Ok(actions)
    }

    // ── tier 2 helpers ──────────────────────────────────────────────────

    async fn hash_table(
        &self,
        table: &str,
        pk_column: &str,
        columns: &[String],
    ) -> anyhow::Result<String> {
        let client = self.require_client()?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let col_exprs: Vec<String> = columns
            .iter()
            .map(|c| {
                let q = quote(c)?;
                Ok(format!("COALESCE({q}::text, '\\N')"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let col_expr_str = col_exprs.join(", ");

        let sql = format!(
            "SELECT md5(string_agg(row_hash, '' ORDER BY {pk})) \
             FROM (\
               SELECT {pk}, md5(concat_ws('|', {col_expr_str})) AS row_hash \
               FROM {qual}\
             ) t"
        );
        let row = client.query_one(&sql, &[]).await?;
        let hash: Option<String> = row.get(0);
        Ok(hash.unwrap_or_default())
    }

    async fn fetch_all_rows(
        &self,
        table: &str,
        columns: &[String],
        order_by: &str,
    ) -> anyhow::Result<Vec<Vec<SqlValue>>> {
        let client = self.require_client()?;
        let qual = self.qualified(table)?;
        let col_list: Vec<String> = columns
            .iter()
            .map(|c| quote(c))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let col_str = col_list.join(", ");
        let order = quote(order_by)?;
        let sql = format!("SELECT {col_str} FROM {qual} ORDER BY {order}");
        let rows = client.query(&sql, &[]).await?;
        Ok(rows
            .iter()
            .map(|r| (0..columns.len()).map(|i| extract_value(r, i)).collect())
            .collect())
    }

    async fn missing_rows_for_pks(
        &self,
        table: &str,
        pk_column: &str,
        expected_pks: &[SqlValue],
    ) -> anyhow::Result<Vec<SqlValue>> {
        if expected_pks.is_empty() {
            return Ok(vec![]);
        }
        let client = self.require_client()?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;

        // Build an array of expected PKs as i64 for the unnest approach.
        let int_pks: Vec<i64> = expected_pks
            .iter()
            .filter_map(|v| v.as_i64())
            .collect();

        let sql = format!(
            "SELECT unnest($1::bigint[]) AS expected_pk \
             EXCEPT \
             SELECT {pk} FROM {qual}"
        );
        let rows = client.query(&sql, &[&int_pks]).await?;
        Ok(rows.iter().map(|r| SqlValue::Int(r.get::<_, i64>(0))).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_normal() {
        assert_eq!(quote("orders").unwrap(), "\"orders\"");
    }

    #[test]
    fn quote_rejects_double_quote() {
        assert!(quote("bad\"name").is_err());
    }

    #[test]
    fn postgres_target_debug_redacts_dsn() {
        let target = PostgresTarget::new("postgres://user:secret@host/db", "public");
        let debug = format!("{target:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("***"));
    }
}
