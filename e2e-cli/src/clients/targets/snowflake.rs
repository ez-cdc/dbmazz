//! Snowflake target backend -- STUB implementation.
//!
//! Snowflake REST API implementation pending -- requires JWT auth + SQL-over-HTTP.
//! There is no native async Rust driver for Snowflake. A full implementation
//! would use the Snowflake SQL REST API with JWT (RSA key-pair) authentication
//! and send SQL statements via HTTP POST to the `/api/v2/statements` endpoint.
//!
//! For now, this module provides a compilable stub so the `TargetBackend` trait
//! is implemented and the crate builds. All methods return `todo!()`.
//!
//! Audit columns (from src/connectors/sinks/snowflake/setup.rs):
//!   - `_DBMAZZ_OP_TYPE`     NUMBER(3,0)
//!   - `_DBMAZZ_SYNCED_AT`   TIMESTAMP_NTZ
//!   - `_DBMAZZ_CDC_VERSION` NUMBER(20,0)
//!   - `_DBMAZZ_IS_DELETED`  BOOLEAN (only when soft-delete enabled)

use std::collections::HashMap;

use async_trait::async_trait;

use super::{BackendCapabilities, ColumnInfo, SqlValue, TargetBackend};

/// Snowflake target backend (stub).
///
/// Snowflake REST API implementation pending -- requires JWT auth + SQL-over-HTTP.
pub struct SnowflakeTarget {
    pub account: String,
    pub user: String,
    pub database: String,
    pub schema: String,
    pub warehouse: String,
    pub role: Option<String>,
    pub soft_delete: bool,
}

/// Custom Debug that redacts credentials.
impl std::fmt::Debug for SnowflakeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeTarget")
            .field("account", &self.account)
            .field("user", &self.user)
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("warehouse", &self.warehouse)
            .field("role", &self.role)
            .field("soft_delete", &self.soft_delete)
            .finish()
    }
}

impl SnowflakeTarget {
    pub fn new(
        account: &str,
        user: &str,
        database: &str,
        schema: &str,
        warehouse: &str,
        role: Option<&str>,
        soft_delete: bool,
    ) -> Self {
        Self {
            account: account.to_string(),
            user: user.to_string(),
            database: database.to_string(),
            schema: schema.to_string(),
            warehouse: warehouse.to_string(),
            role: role.map(|s| s.to_string()),
            soft_delete,
        }
    }
}

#[async_trait]
impl TargetBackend for SnowflakeTarget {
    async fn connect(&mut self) -> anyhow::Result<()> {
        todo!("Snowflake REST API connection -- requires JWT auth + SQL-over-HTTP")
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        todo!("Snowflake REST API close")
    }

    fn name(&self) -> &str {
        "snowflake"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            supports_hard_delete: !self.soft_delete,
            supports_schema_evolution: true,
            supports_arrays: false,
            supports_enum: false,
            has_metadata_table: true,
            supports_hash_compare_sql: true,
            post_cdc_settle_seconds: 35.0,
            post_snapshot_settle_seconds: 40.0,
        }
    }

    fn expected_audit_columns(&self) -> Vec<String> {
        let mut cols = vec![
            "_DBMAZZ_OP_TYPE".to_string(),
            "_DBMAZZ_SYNCED_AT".to_string(),
            "_DBMAZZ_CDC_VERSION".to_string(),
        ];
        if self.soft_delete {
            cols.insert(1, "_DBMAZZ_IS_DELETED".to_string());
        }
        cols
    }

    async fn list_tables(&self) -> anyhow::Result<Vec<String>> {
        todo!("Snowflake REST API: list_tables")
    }

    async fn table_exists(&self, _table: &str) -> anyhow::Result<bool> {
        todo!("Snowflake REST API: table_exists")
    }

    async fn get_columns(&self, _table: &str) -> anyhow::Result<Vec<ColumnInfo>> {
        todo!("Snowflake REST API: get_columns")
    }

    async fn metadata_row_count(&self) -> anyhow::Result<i64> {
        todo!("Snowflake REST API: metadata_row_count")
    }

    async fn count_rows(&self, _table: &str, _exclude_deleted: bool) -> anyhow::Result<i64> {
        todo!("Snowflake REST API: count_rows")
    }

    async fn count_duplicates_by_pk(
        &self,
        _table: &str,
        _pk_column: &str,
    ) -> anyhow::Result<i64> {
        todo!("Snowflake REST API: count_duplicates_by_pk")
    }

    async fn list_primary_keys(
        &self,
        _table: &str,
        _pk_column: &str,
    ) -> anyhow::Result<Vec<SqlValue>> {
        todo!("Snowflake REST API: list_primary_keys")
    }

    async fn row_exists(
        &self,
        _table: &str,
        _pk_column: &str,
        _pk_value: &SqlValue,
    ) -> anyhow::Result<bool> {
        todo!("Snowflake REST API: row_exists")
    }

    async fn row_is_live(
        &self,
        _table: &str,
        _pk_column: &str,
        _pk_value: &SqlValue,
    ) -> anyhow::Result<bool> {
        todo!("Snowflake REST API: row_is_live")
    }

    async fn fetch_value(
        &self,
        _table: &str,
        _pk_column: &str,
        _pk_value: &SqlValue,
        _column: &str,
    ) -> anyhow::Result<Option<SqlValue>> {
        todo!("Snowflake REST API: fetch_value")
    }

    async fn fetch_row(
        &self,
        _table: &str,
        _pk_column: &str,
        _pk_value: &SqlValue,
    ) -> anyhow::Result<Option<HashMap<String, SqlValue>>> {
        todo!("Snowflake REST API: fetch_row")
    }

    async fn clean(&self, _tables: &[String]) -> anyhow::Result<Vec<String>> {
        todo!("Snowflake REST API: clean")
    }

    async fn hash_table(
        &self,
        _table: &str,
        _pk_column: &str,
        _columns: &[String],
    ) -> anyhow::Result<String> {
        todo!("Snowflake REST API: hash_table")
    }

    async fn fetch_all_rows(
        &self,
        _table: &str,
        _columns: &[String],
        _order_by: &str,
    ) -> anyhow::Result<Vec<Vec<SqlValue>>> {
        todo!("Snowflake REST API: fetch_all_rows")
    }

    async fn missing_rows_for_pks(
        &self,
        _table: &str,
        _pk_column: &str,
        _expected_pks: &[SqlValue],
    ) -> anyhow::Result<Vec<SqlValue>> {
        todo!("Snowflake REST API: missing_rows_for_pks")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_target_capabilities() {
        let target = SnowflakeTarget::new(
            "xy12345.us-east-1",
            "user",
            "db",
            "PUBLIC",
            "wh",
            None,
            true,
        );
        let caps = target.capabilities();
        assert!(!caps.supports_hard_delete); // soft_delete=true
        assert!(caps.has_metadata_table);
        assert!((caps.post_cdc_settle_seconds - 35.0).abs() < f64::EPSILON);
    }

    #[test]
    fn snowflake_target_capabilities_hard_delete() {
        let target = SnowflakeTarget::new(
            "xy12345.us-east-1",
            "user",
            "db",
            "PUBLIC",
            "wh",
            None,
            false,
        );
        let caps = target.capabilities();
        assert!(caps.supports_hard_delete); // soft_delete=false
    }

    #[test]
    fn snowflake_audit_columns_soft_delete() {
        let target = SnowflakeTarget::new(
            "xy12345.us-east-1",
            "user",
            "db",
            "PUBLIC",
            "wh",
            None,
            true,
        );
        let cols = target.expected_audit_columns();
        assert_eq!(cols.len(), 4);
        assert!(cols.contains(&"_DBMAZZ_IS_DELETED".to_string()));
    }

    #[test]
    fn snowflake_audit_columns_no_soft_delete() {
        let target = SnowflakeTarget::new(
            "xy12345.us-east-1",
            "user",
            "db",
            "PUBLIC",
            "wh",
            None,
            false,
        );
        let cols = target.expected_audit_columns();
        assert_eq!(cols.len(), 3);
        assert!(!cols.contains(&"_DBMAZZ_IS_DELETED".to_string()));
    }

    #[test]
    fn snowflake_name() {
        let target = SnowflakeTarget::new(
            "xy12345.us-east-1",
            "user",
            "db",
            "PUBLIC",
            "wh",
            None,
            true,
        );
        assert_eq!(target.name(), "snowflake");
    }
}
