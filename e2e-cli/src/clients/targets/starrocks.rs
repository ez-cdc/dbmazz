//! StarRocks target backend.
//!   - `dbmazz_cdc_version` BIGINT
//!
//! StarRocks does NOT have a metadata table in the target schema.
//! Delete semantics: soft delete via `dbmazz_is_deleted=true`.

use std::collections::HashMap;

use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, Row as MysqlRow};

use super::{BackendCapabilities, ColumnInfo, SqlValue, TargetBackend};

/// Quote a StarRocks/MySQL identifier with backticks.
fn quote(name: &str) -> Result<String, anyhow::Error> {
    if name.contains('`') {
        anyhow::bail!("identifier contains backtick: {name:?}");
    }
    Ok(format!("`{name}`"))
}

/// Extract a `SqlValue` from a mysql_async `Row` by column index.
///
/// Uses `get_opt` instead of `get` to avoid panics when the wire type
/// doesn't match the requested Rust type (e.g. a VARCHAR value cannot
/// be decoded as i64).
fn extract_value(row: &MysqlRow, idx: usize) -> SqlValue {
    if let Some(Ok(v)) = row.get_opt::<i64, _>(idx) {
        return SqlValue::Int(v);
    }
    if let Some(Ok(v)) = row.get_opt::<f64, _>(idx) {
        return SqlValue::Float(v);
    }
    if let Some(Ok(v)) = row.get_opt::<String, _>(idx) {
        return SqlValue::Text(v);
    }
    SqlValue::Null
}

/// Convert a `SqlValue` to a `mysql_async::Value` for use as a query parameter.
fn to_mysql_value(v: &SqlValue) -> mysql_async::Value {
    match v {
        SqlValue::Null => mysql_async::Value::NULL,
        SqlValue::Bool(b) => mysql_async::Value::from(*b),
        SqlValue::Int(i) => mysql_async::Value::from(*i),
        SqlValue::Float(f) => mysql_async::Value::from(*f),
        SqlValue::Text(s) => mysql_async::Value::from(s.clone()),
    }
}

/// StarRocks target backend using `mysql_async`.
pub struct StarRocksTarget {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    pool: Option<Pool>,
}

/// Custom Debug that redacts the password.
impl std::fmt::Debug for StarRocksTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StarRocksTarget")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"***")
            .field("database", &self.database)
            .field("connected", &self.pool.is_some())
            .finish()
    }
}

impl StarRocksTarget {
    pub fn new(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
    ) -> Self {
        Self {
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            pool: None,
        }
    }

    async fn get_conn(&self) -> anyhow::Result<Conn> {
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("StarRocksTarget.connect() was not called"))?;
        let conn = pool.get_conn().await?;
        Ok(conn)
    }

    fn qualified(&self, table: &str) -> Result<String, anyhow::Error> {
        Ok(format!("{}.{}", quote(&self.database)?, quote(table)?))
    }
}

#[async_trait]
impl TargetBackend for StarRocksTarget {
    async fn connect(&mut self) -> anyhow::Result<()> {
        let opts: Opts = OptsBuilder::default()
            .ip_or_hostname(&self.host)
            .tcp_port(self.port)
            .user(Some(&self.user))
            .pass(Some(&self.password))
            .db_name(Some(&self.database))
            // StarRocks doesn't support the `@@socket` system variable that
            // mysql_async queries by default. Disable socket preference and
            // prepared statement cache to avoid incompatible init queries.
            .prefer_socket(false)
            .stmt_cache_size(0)
            .into();
        self.pool = Some(Pool::new(opts));
        // Test the connection by getting one.
        let _conn = self.get_conn().await?;
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(pool) = self.pool.take() {
            pool.disconnect().await?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "starrocks"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_hard_delete: false, // soft delete via dbmazz_is_deleted
            supports_schema_evolution: true,
            supports_arrays: false,
            supports_enum: false,
            has_metadata_table: false,
            supports_hash_compare_sql: true,
            post_cdc_settle_seconds: 2.0,
            post_snapshot_settle_seconds: 2.0,
        }
    }

    fn expected_audit_columns(&self) -> Vec<String> {
        vec![
            "dbmazz_op_type".to_string(),
            "dbmazz_is_deleted".to_string(),
            "dbmazz_synced_at".to_string(),
            "dbmazz_cdc_version".to_string(),
        ]
    }

    // ── schema inspection ───────────────────────────────────────────────

    async fn list_tables(&self) -> anyhow::Result<Vec<String>> {
        let mut conn = self.get_conn().await?;
        let rows: Vec<MysqlRow> = conn
            .exec(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = :db \
                   AND LEFT(table_name, 7) != 'dbmazz_' \
                 ORDER BY table_name",
                mysql_async::params! { "db" => &self.database },
            )
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| r.get::<String, _>(0))
            .collect())
    }

    async fn table_exists(&self, table: &str) -> anyhow::Result<bool> {
        let mut conn = self.get_conn().await?;
        let rows: Vec<MysqlRow> = conn
            .exec(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = :db AND table_name = :tbl",
                mysql_async::params! { "db" => &self.database, "tbl" => table },
            )
            .await?;
        Ok(!rows.is_empty())
    }

    async fn get_columns(&self, table: &str) -> anyhow::Result<Vec<ColumnInfo>> {
        let mut conn = self.get_conn().await?;
        let rows: Vec<MysqlRow> = conn
            .exec(
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns \
                 WHERE table_schema = :db AND table_name = :tbl \
                 ORDER BY ordinal_position",
                mysql_async::params! { "db" => &self.database, "tbl" => table },
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let name: String = r.get(0).unwrap_or_default();
                let sql_type: String = r.get(1).unwrap_or_default();
                let nullable_str: String = r.get(2).unwrap_or_default();
                ColumnInfo {
                    name,
                    sql_type: sql_type.to_uppercase(),
                    nullable: nullable_str == "YES",
                }
            })
            .collect())
    }

    async fn metadata_row_count(&self) -> anyhow::Result<i64> {
        anyhow::bail!(
            "StarRocks sink does not maintain a metadata table in the target schema"
        )
    }

    // ── row counting ────────────────────────────────────────────────────

    async fn count_rows(&self, table: &str, exclude_deleted: bool) -> anyhow::Result<i64> {
        let mut conn = self.get_conn().await?;
        let qual = self.qualified(table)?;
        let mut sql = format!("SELECT count(*) FROM {qual}");
        if exclude_deleted {
            sql.push_str(" WHERE `dbmazz_is_deleted` = false");
        }
        let rows: Vec<MysqlRow> = conn.query(&sql).await?;
        if let Some(row) = rows.first() {
            Ok(row.get::<i64, _>(0).unwrap_or(0))
        } else {
            Ok(0)
        }
    }

    async fn count_duplicates_by_pk(&self, table: &str, pk_column: &str) -> anyhow::Result<i64> {
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!(
            "SELECT count(*) FROM (\
               SELECT {pk} FROM {qual} \
               WHERE `dbmazz_is_deleted` = false \
               GROUP BY {pk} HAVING count(*) > 1\
             ) dupes"
        );
        let rows: Vec<MysqlRow> = conn.query(&sql).await?;
        if let Some(row) = rows.first() {
            Ok(row.get::<i64, _>(0).unwrap_or(0))
        } else {
            Ok(0)
        }
    }

    async fn list_primary_keys(
        &self,
        table: &str,
        pk_column: &str,
    ) -> anyhow::Result<Vec<SqlValue>> {
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!(
            "SELECT {pk} FROM {qual} \
             WHERE `dbmazz_is_deleted` = false \
             ORDER BY {pk}"
        );
        let rows: Vec<MysqlRow> = conn.query(&sql).await?;
        Ok(rows.iter().map(|r| extract_value(r, 0)).collect())
    }

    // ── row queries ─────────────────────────────────────────────────────

    async fn row_exists(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
    ) -> anyhow::Result<bool> {
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT 1 FROM {qual} WHERE {pk} = :pk");
        let val = to_mysql_value(pk_value);
        let rows: Vec<MysqlRow> = conn
            .exec(&sql, mysql_async::params! { "pk" => val })
            .await?;
        Ok(!rows.is_empty())
    }

    async fn row_is_live(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
    ) -> anyhow::Result<bool> {
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT `dbmazz_is_deleted` FROM {qual} WHERE {pk} = :pk");
        let val = to_mysql_value(pk_value);
        let rows: Vec<MysqlRow> = conn
            .exec(&sql, mysql_async::params! { "pk" => val })
            .await?;
        if let Some(row) = rows.first() {
            // dbmazz_is_deleted is BOOLEAN -- mysql_async returns i64 (0 or 1).
            let is_deleted: i64 = row.get(0).unwrap_or(1);
            Ok(is_deleted == 0)
        } else {
            Ok(false)
        }
    }

    async fn fetch_value(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
        column: &str,
    ) -> anyhow::Result<Option<SqlValue>> {
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let col = quote(column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT {col} FROM {qual} WHERE {pk} = :pk");
        let val = to_mysql_value(pk_value);
        let rows: Vec<MysqlRow> = conn
            .exec(&sql, mysql_async::params! { "pk" => val })
            .await?;
        if let Some(row) = rows.first() {
            Ok(Some(extract_value(row, 0)))
        } else {
            Ok(None)
        }
    }

    async fn fetch_row(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
    ) -> anyhow::Result<Option<HashMap<String, SqlValue>>> {
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!("SELECT * FROM {qual} WHERE {pk} = :pk");
        let val = to_mysql_value(pk_value);
        let rows: Vec<MysqlRow> = conn
            .exec(&sql, mysql_async::params! { "pk" => val })
            .await?;
        if let Some(row) = rows.first() {
            let columns = row.columns_ref();
            let mut map = HashMap::new();
            for (i, col) in columns.iter().enumerate() {
                let col_name = col.name_str().to_string();
                map.insert(col_name, extract_value(row, i));
            }
            Ok(Some(map))
        } else {
            Ok(None)
        }
    }

    // ── cleanup ─────────────────────────────────────────────────────────

    async fn clean(&self, tables: &[String]) -> anyhow::Result<Vec<String>> {
        let mut actions = Vec::new();

        let audit_cols = [
            "dbmazz_op_type",
            "dbmazz_is_deleted",
            "dbmazz_synced_at",
            "dbmazz_cdc_version",
        ];

        for table in tables {
            if !self.table_exists(table).await? {
                continue;
            }
            let qual = self.qualified(table)?;

            // Truncate
            {
                let mut conn = self.get_conn().await?;
                conn.query_drop(&format!("TRUNCATE TABLE {qual}")).await?;
                actions.push(format!("truncated {table}"));
            }

            // Drop audit columns (StarRocks has no DROP IF EXISTS for columns).
            for col in &audit_cols {
                let mut conn = self.get_conn().await?;
                let check_rows: Vec<MysqlRow> = conn
                    .exec(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
                         WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl AND COLUMN_NAME = :col",
                        mysql_async::params! {
                            "db" => &self.database,
                            "tbl" => table.as_str(),
                            "col" => *col,
                        },
                    )
                    .await?;
                if !check_rows.is_empty() {
                    let col_quoted = quote(col)?;
                    let _ = conn
                        .query_drop(&format!("ALTER TABLE {qual} DROP COLUMN {col_quoted}"))
                        .await;
                }
            }
            actions.push(format!("dropped audit columns from {table}"));
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
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let col_exprs: Vec<String> = columns
            .iter()
            .map(|c| {
                let q = quote(c)?;
                Ok(format!("ifnull(cast({q} as string), '\\\\N')"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let col_expr_str = col_exprs.join(", ");

        let sql = format!(
            "SELECT md5(group_concat(row_hash ORDER BY {pk} SEPARATOR '')) \
             FROM (\
               SELECT {pk}, md5(concat_ws('|', {col_expr_str})) AS row_hash \
               FROM {qual} \
               WHERE `dbmazz_is_deleted` = false\
             ) t"
        );
        let rows: Vec<MysqlRow> = conn.query(&sql).await?;
        if let Some(row) = rows.first() {
            Ok(row.get::<String, _>(0).unwrap_or_default())
        } else {
            Ok(String::new())
        }
    }

    async fn fetch_all_rows(
        &self,
        table: &str,
        columns: &[String],
        order_by: &str,
    ) -> anyhow::Result<Vec<Vec<SqlValue>>> {
        let mut conn = self.get_conn().await?;
        let qual = self.qualified(table)?;
        let col_list: Vec<String> = columns
            .iter()
            .map(|c| quote(c))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let col_str = col_list.join(", ");
        let order = quote(order_by)?;
        let sql = format!(
            "SELECT {col_str} FROM {qual} \
             WHERE `dbmazz_is_deleted` = false \
             ORDER BY {order}"
        );
        let rows: Vec<MysqlRow> = conn.query(&sql).await?;
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
        // StarRocks doesn't support unnest; fetch target PKs and diff in Rust.
        let mut conn = self.get_conn().await?;
        let pk = quote(pk_column)?;
        let qual = self.qualified(table)?;
        let sql = format!(
            "SELECT {pk} FROM {qual} WHERE `dbmazz_is_deleted` = false"
        );
        let rows: Vec<MysqlRow> = conn.query(&sql).await?;
        let target_pks: std::collections::HashSet<SqlValue> =
            rows.iter().map(|r| extract_value(r, 0)).collect();
        Ok(expected_pks
            .iter()
            .filter(|pk| !target_pks.contains(pk))
            .cloned()
            .collect())
    }
}

// We need SqlValue to be hashable for the HashSet above.
impl Eq for SqlValue {}
impl std::hash::Hash for SqlValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            SqlValue::Null => {}
            SqlValue::Bool(b) => b.hash(state),
            SqlValue::Int(i) => i.hash(state),
            SqlValue::Float(f) => f.to_bits().hash(state),
            SqlValue::Text(s) => s.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_normal() {
        assert_eq!(quote("orders").unwrap(), "`orders`");
    }

    #[test]
    fn quote_rejects_backtick() {
        assert!(quote("bad`name").is_err());
    }

    #[test]
    fn starrocks_target_debug_redacts_password() {
        let target = StarRocksTarget::new("localhost", 9030, "root", "secret123", "db");
        let debug = format!("{target:?}");
        assert!(!debug.contains("secret123"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn expected_audit_columns() {
        let target = StarRocksTarget::new("localhost", 9030, "root", "", "db");
        let cols = target.expected_audit_columns();
        assert_eq!(cols.len(), 4);
        assert!(cols.contains(&"dbmazz_is_deleted".to_string()));
    }
}
