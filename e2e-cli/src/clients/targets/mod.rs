//! Target backend trait and implementations.
//!
//! Each sink (PostgreSQL, StarRocks, Snowflake) implements the `TargetBackend`
//! trait. The verify runner and load runner only call methods on this trait --
//! they never reach into sink-specific internals.
//!
//! Sinks differ in:
//!   - audit column naming (StarRocks `dbmazz_*`, Postgres `_dbmazz_*`, Snowflake `_DBMAZZ_*`)
//!   - metadata table presence (Postgres/Snowflake have `_dbmazz._metadata`, StarRocks does not)
//!   - delete semantics (Postgres hard-deletes, StarRocks/Snowflake soft-delete)
//!   - normalizer settle time (Snowflake ~30s, StarRocks ~2s, Postgres synchronous)

pub mod postgres;
pub mod snowflake;
pub mod starrocks;

use std::collections::HashMap;

use async_trait::async_trait;

// ── Data model ──────────────────────────────────────────────────────────────

/// Normalized column metadata.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name as stored in the target.
    pub name: String,
    /// Backend-normalized type name (INTEGER, BIGINT, TEXT, etc.).
    #[allow(dead_code)]
    pub sql_type: String,
    /// Whether the column allows NULLs.
    #[allow(dead_code)]
    pub nullable: bool,
}

/// What a target sink can and cannot do.
///
/// Used by the verify runner to skip validations that don't apply to this sink.
#[derive(Debug, Clone)]
pub struct BackendCapabilities {
    /// True: DELETE removes the row physically. False: soft delete via is_deleted.
    pub supports_hard_delete: bool,
    /// True: ALTER TABLE ADD COLUMN is propagated automatically.
    #[allow(dead_code)]
    pub supports_schema_evolution: bool,
    /// True: array types are supported.
    #[allow(dead_code)]
    pub supports_arrays: bool,
    /// True: enum types are supported.
    #[allow(dead_code)]
    pub supports_enum: bool,
    /// True: the sink maintains a `_dbmazz._metadata` table.
    pub has_metadata_table: bool,
    /// True: `hash_table()` uses native SQL aggregation.
    #[allow(dead_code)]
    pub supports_hash_compare_sql: bool,
    /// Seconds to wait after a CDC operation before reading the target.
    pub post_cdc_settle_seconds: f64,
    /// Seconds to wait after snapshot completes before reading the target.
    pub post_snapshot_settle_seconds: f64,
}

/// A value that can be returned from a target query.
///
/// Wraps the common SQL types used in e2e tests so that `fetch_value`
/// and `fetch_row` can return data without the caller knowing the driver.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl SqlValue {
    /// Return the inner value as an i64, or None if not an integer.
    #[allow(dead_code)]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            SqlValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// Return the inner value as a string, or None if NULL.
    #[allow(dead_code)]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            SqlValue::Text(v) => Some(v),
            _ => None,
        }
    }
}

// ── Trait ────────────────────────────────────────────────────────────────────

/// Contract for target sinks used by the e2e verify and load runners.
#[async_trait]
#[allow(dead_code)]
pub trait TargetBackend: Send + Sync {
    // ── lifecycle ────────────────────────────────────────────────────────

    /// Open the connection. Must be called before any other method.
    async fn connect(&mut self) -> anyhow::Result<()>;

    /// Close the connection. Safe to call multiple times.
    async fn close(&mut self) -> anyhow::Result<()>;

    // ── identity ────────────────────────────────────────────────────────

    /// Human-readable name: "postgres", "starrocks", "snowflake".
    fn name(&self) -> &str;

    /// Capabilities of this backend.
    fn capabilities(&self) -> BackendCapabilities;

    /// Audit column names this sink adds to replicated tables.
    fn expected_audit_columns(&self) -> Vec<String>;

    // ── schema inspection ───────────────────────────────────────────────

    /// Return user tables in the target (excluding metadata/system tables).
    async fn list_tables(&self) -> anyhow::Result<Vec<String>>;

    /// Return True if the named table exists.
    async fn table_exists(&self, table: &str) -> anyhow::Result<bool>;

    /// Return all columns of a table, including audit columns.
    async fn get_columns(&self, table: &str) -> anyhow::Result<Vec<ColumnInfo>>;

    /// Return the number of rows in the sink's metadata table.
    async fn metadata_row_count(&self) -> anyhow::Result<i64>;

    // ── row counting ────────────────────────────────────────────────────

    /// Count rows in a table. If `exclude_deleted` is true and the sink uses
    /// soft delete, the count excludes rows where is_deleted is true.
    async fn count_rows(&self, table: &str, exclude_deleted: bool) -> anyhow::Result<i64>;

    /// Count rows that share a PK value with another row (should return 0).
    async fn count_duplicates_by_pk(&self, table: &str, pk_column: &str) -> anyhow::Result<i64>;

    /// Return all PK values in the target, sorted.
    async fn list_primary_keys(&self, table: &str, pk_column: &str) -> anyhow::Result<Vec<SqlValue>>;

    // ── row queries ─────────────────────────────────────────────────────

    /// Return true if a row with the given PK exists (even if soft-deleted).
    async fn row_exists(&self, table: &str, pk_column: &str, pk_value: &SqlValue) -> anyhow::Result<bool>;

    /// Return true if a row exists and is not marked as deleted.
    async fn row_is_live(&self, table: &str, pk_column: &str, pk_value: &SqlValue) -> anyhow::Result<bool>;

    /// Return a single column value for a row, or None if not found.
    async fn fetch_value(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
        column: &str,
    ) -> anyhow::Result<Option<SqlValue>>;

    /// Return an entire row as column_name -> value, or None if not found.
    async fn fetch_row(
        &self,
        table: &str,
        pk_column: &str,
        pk_value: &SqlValue,
    ) -> anyhow::Result<Option<HashMap<String, SqlValue>>>;

    // ── cleanup ─────────────────────────────────────────────────────────

    /// Remove all dbmazz artifacts from the target. Returns descriptions of actions taken.
    async fn clean(&self, tables: &[String]) -> anyhow::Result<Vec<String>>;

    // ── tier 2 helpers ──────────────────────────────────────────────────

    /// Aggregate hash over the table, ordered by PK. Returns a hex string.
    async fn hash_table(
        &self,
        table: &str,
        pk_column: &str,
        columns: &[String],
    ) -> anyhow::Result<String>;

    /// Return all rows of the table as vectors of SqlValue, ordered.
    async fn fetch_all_rows(
        &self,
        table: &str,
        columns: &[String],
        order_by: &str,
    ) -> anyhow::Result<Vec<Vec<SqlValue>>>;

    /// Return the PKs in `expected_pks` that don't exist in the target.
    async fn missing_rows_for_pks(
        &self,
        table: &str,
        pk_column: &str,
        expected_pks: &[SqlValue],
    ) -> anyhow::Result<Vec<SqlValue>>;
}
