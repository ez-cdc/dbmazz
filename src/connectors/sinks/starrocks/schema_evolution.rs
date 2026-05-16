// Copyright 2025
// Licensed under the Elastic License v2.0

//! StarRocks-specific schema evolution: ALTER TABLE ADD COLUMN application,
//! `fast_schema_evolution` per-table detection, server version validation,
//! and target-side schema cache.
//!
//! Consumes the sink-agnostic `SchemaDiff` produced by
//! `crate::connectors::sinks::schema_evolution::compute_schema_evolution_plan`
//! and emits StarRocks-dialect DDL via the MySQL protocol pool.
//!
//! # Hard requirements (validated at setup, fail loud if missing)
//!
//! - **Server version ≥ 3.2.0**. Older versions don't support
//!   `fast_schema_evolution` and would require a poll loop on
//!   `SHOW ALTER TABLE` that adds complexity for no demonstrated demand.
//!
//! # Per-table degraded mode
//!
//! - **Table has `fast_schema_evolution=true`** → ADD COLUMN applies in
//!   metadata-only milliseconds. Stream Load following the ALTER sees the
//!   new column immediately.
//! - **Table missing the property** → schema evolution is **disabled** for
//!   that table. The daemon arranca anyway with a loud `WARN` at setup
//!   plus per-event `WARN` and a `schema_evolution_skipped_total` counter
//!   when SchemaChange events arrive. The table behaves as it did before
//!   this change (data loads succeed, but column additions are silently
//!   filtered by Stream Load via `max_filter_ratio`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use mysql_async::{prelude::Queryable, Conn, OptsBuilder, Pool};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::connectors::sinks::schema_evolution::SchemaDiff;
use crate::connectors::sinks::starrocks::config::StarRocksSinkConfig;
use crate::connectors::sinks::starrocks::types::TypeMapper;
use crate::utils::validate_sql_identifier;

/// Minimum supported StarRocks server version.
///
/// Older versions don't support `fast_schema_evolution` table property —
/// `ALTER TABLE` is asynchronous and slow on big tables, which would force
/// a poll loop that we explicitly chose not to implement.
const MIN_SERVER_VERSION_MAJOR: u32 = 3;
const MIN_SERVER_VERSION_MINOR: u32 = 2;

/// Cached metadata about a column in a target StarRocks table. Built from
/// `INFORMATION_SCHEMA.COLUMNS` on demand.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TargetColumn {
    pub name: String,
    /// Raw `COLUMN_TYPE` value from `INFORMATION_SCHEMA.COLUMNS`. Useful
    /// for diagnostic logging; not used for type-equality checks (the diff
    /// logic compares by NAME only).
    pub data_type_str: String,
    pub nullable: bool,
}

/// StarRocks-specific schema evolution machinery. Owned by `StarRocksSink`,
/// shared between `setup()` and `write_batch()`.
pub struct StarRocksSchemaEvolution {
    pool: Pool,
    database: String,
    type_mapper: TypeMapper,
    /// Tables (by short name, no `db.` prefix) that have
    /// `fast_schema_evolution=true` → schema evolution is **enabled**.
    /// Populated once at setup; not mutated at runtime.
    schema_evolution_enabled: Arc<RwLock<HashSet<String>>>,
    /// Cache: target table name → its current column list. Refreshed after
    /// each successful ALTER and on every `refresh_target_schema_cache` call.
    target_columns: Arc<RwLock<HashMap<String, Vec<TargetColumn>>>>,
}

impl StarRocksSchemaEvolution {
    pub fn new(config: &StarRocksSinkConfig) -> Result<Self> {
        let opts = OptsBuilder::default()
            .ip_or_hostname(config.hostname())
            .tcp_port(config.mysql_port)
            .user(Some(config.user.clone()))
            .pass(Some(config.password.clone()))
            .db_name(Some(config.database.clone()))
            .prefer_socket(false); // StarRocks doesn't support Unix socket
        Ok(Self {
            pool: Pool::new(opts),
            database: config.database.clone(),
            type_mapper: TypeMapper::new(),
            schema_evolution_enabled: Arc::new(RwLock::new(HashSet::new())),
            target_columns: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    // ---------------------------------------------------------------------
    // Setup-time validation
    // ---------------------------------------------------------------------

    /// Validate that the connected StarRocks server is at least version 3.2.0.
    /// Fails loud if older — `fast_schema_evolution` does not exist before 3.2.
    pub async fn validate_server_version(&self) -> Result<()> {
        let mut conn = self.get_conn().await?;

        // `@@version` on the MySQL-protocol layer returns a synthetic value
        // with a MySQL compat prefix on some builds; `current_version()`
        // returns the actual StarRocks version.
        let raw_version: Option<String> = conn
            .query_first("SELECT current_version()")
            .await
            .map_err(|e| anyhow!("Failed to query StarRocks server version: {}", e))?;

        let version_str = raw_version
            .ok_or_else(|| anyhow!("StarRocks `SELECT current_version()` returned no row"))?;

        let (major, minor) = parse_server_version(&version_str).ok_or_else(|| {
            anyhow!(
                "Could not parse StarRocks version string: {:?}",
                version_str
            )
        })?;

        if major < MIN_SERVER_VERSION_MAJOR
            || (major == MIN_SERVER_VERSION_MAJOR && minor < MIN_SERVER_VERSION_MINOR)
        {
            return Err(anyhow!(
                "StarRocks server version {}.{} is too old (got {:?}). \
                 Minimum required: {}.{}.0. \
                 Older versions do not support fast_schema_evolution and would \
                 require synchronous polling on `SHOW ALTER TABLE`. \
                 Upgrade your StarRocks cluster to 3.2 or later.",
                major,
                minor,
                version_str,
                MIN_SERVER_VERSION_MAJOR,
                MIN_SERVER_VERSION_MINOR
            ));
        }

        info!(
            "  [OK] StarRocks server version {}.{}+ detected (raw: {})",
            major, minor, version_str
        );
        Ok(())
    }

    /// Populate `schema_evolution_enabled` from `INFORMATION_SCHEMA.TABLES_CONFIG`.
    /// Per-table query failures degrade the table to disabled (loud WARN);
    /// tables present without the property log a WARN with remediation.
    pub async fn detect_fast_schema_evolution_per_table(&self, tables: &[String]) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let mut enabled = self.schema_evolution_enabled.write().await;

        for table_qualified in tables {
            let table_name = table_qualified
                .split('.')
                .next_back()
                .unwrap_or(table_qualified)
                .to_string();

            let has_flag = self
                .table_has_fast_schema_evolution(&mut conn, &table_name)
                .await
                .unwrap_or_else(|e| {
                    warn!(
                        table = %table_name,
                        error = %e,
                        "starrocks_sink: failed to detect fast_schema_evolution; treating as disabled"
                    );
                    false
                });

            if has_flag {
                info!(
                    "  [OK] Table {} has fast_schema_evolution=true (schema evolution enabled)",
                    table_name
                );
                enabled.insert(table_name);
            } else {
                warn!(
                    table = %table_name,
                    "starrocks_sink: schema evolution DISABLED for table — \
                     fast_schema_evolution=true not set on target. \
                     Schema additions in source will NOT be propagated to this table. \
                     To enable, recreate the table with \
                     PROPERTIES('fast_schema_evolution'='true') (StarRocks 3.2+; \
                     this property is creation-only and cannot be added via ALTER)."
                );
            }
        }

        Ok(())
    }

    /// Lightweight reconciliation pass on startup. For tables whose source
    /// schema has columns missing from the target schema, emit `ALTER TABLE
    /// ADD COLUMN` to bring the target up to date.
    ///
    /// Skipped for tables without `fast_schema_evolution=true` — for those
    /// the daemon emits a WARN at setup time (see
    /// `detect_fast_schema_evolution_per_table`) and proceeds.
    ///
    /// Fails loud on any individual ALTER failure: setup error surfaces in
    /// `/healthz` and the daemon does not begin CDC.
    pub async fn reconcile_target_schema(
        &self,
        source_schemas: &[crate::core::traits::SourceTableSchema],
    ) -> Result<()> {
        let enabled = self.schema_evolution_enabled.read().await.clone();

        for source in source_schemas {
            if !enabled.contains(&source.name) {
                continue;
            }

            self.refresh_target_schema_cache(&source.name).await?;

            let target_names: HashSet<String> = self
                .target_columns
                .read()
                .await
                .get(&source.name)
                .map(|cols| cols.iter().map(|c| c.name.to_lowercase()).collect())
                .unwrap_or_default();

            let mut diff = SchemaDiff::default();
            for (i, src_col) in source.columns.iter().enumerate() {
                if !target_names.contains(&src_col.name.to_lowercase()) {
                    diff.added
                        .push(crate::connectors::sinks::schema_evolution::AddedColumn {
                            name: src_col.name.clone(),
                            data_type: src_col.data_type.clone(),
                            nullable: true,
                            #[allow(clippy::cast_possible_wrap)]
                            ordinal: i as i32,
                            pg_type_id: src_col.pg_type_id,
                        });
                }
            }

            if diff.added.is_empty() {
                continue;
            }

            info!(
                table = %source.name,
                added_count = diff.added.len(),
                "starrocks_sink: reconcile_on_startup applying ALTERs"
            );
            self.apply_diff(&source.name, &diff)
                .await
                .with_context(|| {
                    format!(
                        "starrocks_sink: reconcile_on_startup failed for table {}",
                        source.name
                    )
                })?;
        }

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Runtime DDL
    // ---------------------------------------------------------------------

    /// Apply a `SchemaDiff` to the target table: emit `ALTER TABLE ADD COLUMN`
    /// for each added column, verify each via `INFORMATION_SCHEMA`, and
    /// refresh the target_columns cache once at the end.
    ///
    /// # Idempotency
    ///
    /// StarRocks does **not** support `IF NOT EXISTS` in `ALTER TABLE ADD COLUMN`
    /// (parser rejects it: `No viable statement for input 'ADD COLUMN IF'.`).
    /// Idempotency is therefore implemented as a pre-check against
    /// `INFORMATION_SCHEMA.COLUMNS`: if the column already exists on the
    /// target, the ALTER is skipped with an info log. This makes
    /// `apply_diff` safe to retry after a partial failure or to re-run
    /// across daemon restarts when the in-memory cache is stale.
    ///
    /// Caller MUST check `is_schema_evolution_enabled(table)` first; this
    /// function unconditionally applies the diff. Tables without the flag
    /// should be filtered out at the call site.
    pub async fn apply_diff(&self, table_name: &str, diff: &SchemaDiff) -> Result<()> {
        validate_sql_identifier(table_name)
            .map_err(|e| anyhow!("Invalid table name '{}': {}", table_name, e))?;
        validate_sql_identifier(&self.database)
            .map_err(|e| anyhow!("Invalid database name '{}': {}", self.database, e))?;

        if diff.added.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_conn().await?;

        for added in &diff.added {
            validate_sql_identifier(&added.name)
                .map_err(|e| anyhow!("Invalid column name '{}': {}", added.name, e))?;

            // Idempotency pre-check: StarRocks rejects IF NOT EXISTS on
            // ADD COLUMN, so we ask INFORMATION_SCHEMA before issuing DDL.
            let column_exists: Option<i32> = conn
                .exec_first(
                    "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                    (&self.database, table_name, &added.name),
                )
                .await
                .with_context(|| {
                    format!(
                        "starrocks_sink: failed to pre-check column existence for {}.{}.{}",
                        self.database, table_name, added.name
                    )
                })?;

            if column_exists.is_some() {
                info!(
                    table = %table_name,
                    column = %added.name,
                    "starrocks_sink: ADD COLUMN skipped — column already exists in target"
                );
                continue;
            }

            let starrocks_type = self.type_mapper.to_starrocks_type(&added.data_type);
            let sql = format!(
                "ALTER TABLE `{}`.`{}` ADD COLUMN `{}` {}",
                self.database, table_name, added.name, starrocks_type
            );
            conn.query_drop(&sql).await.with_context(|| {
                format!(
                    "starrocks_sink: ALTER TABLE failed for {}.{} column {}",
                    self.database, table_name, added.name
                )
            })?;

            self.verify_alter_completed(&mut conn, table_name, &added.name)
                .await?;

            info!(
                table = %table_name,
                column = %added.name,
                starrocks_type = %starrocks_type,
                "starrocks_sink: ADD COLUMN applied"
            );
        }

        // Drop the connection so refresh_target_schema_cache can take its own.
        drop(conn);
        self.refresh_target_schema_cache(table_name).await?;
        Ok(())
    }

    pub async fn is_schema_evolution_enabled(&self, table_name: &str) -> bool {
        self.schema_evolution_enabled
            .read()
            .await
            .contains(table_name)
    }

    /// Reload the column list for `table_name` from `INFORMATION_SCHEMA.COLUMNS`.
    pub async fn refresh_target_schema_cache(&self, table_name: &str) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let rows: Vec<(String, String, String)> = conn
            .exec(
                "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
                 FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                 ORDER BY ORDINAL_POSITION",
                (&self.database, table_name),
            )
            .await
            .with_context(|| {
                format!(
                    "starrocks_sink: failed to refresh target schema for {}.{}",
                    self.database, table_name
                )
            })?;

        let columns: Vec<TargetColumn> = rows
            .into_iter()
            .map(|(name, data_type_str, is_nullable)| TargetColumn {
                name,
                data_type_str,
                nullable: is_nullable.eq_ignore_ascii_case("YES"),
            })
            .collect();

        let mut cache = self.target_columns.write().await;
        cache.insert(table_name.to_string(), columns);
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------

    async fn get_conn(&self) -> Result<Conn> {
        self.pool
            .get_conn()
            .await
            .map_err(|e| anyhow!("starrocks_sink: failed to get MySQL connection: {}", e))
    }

    /// Defensive post-ALTER sanity check. With `fast_schema_evolution=true`
    /// the column is visible immediately; without it this would never
    /// succeed (and we wouldn't have reached this path anyway).
    async fn verify_alter_completed(
        &self,
        conn: &mut Conn,
        table_name: &str,
        column_name: &str,
    ) -> Result<()> {
        let exists: Option<i32> = conn
            .exec_first(
                "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                (&self.database, table_name, column_name),
            )
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to verify ALTER for {}.{}: {}",
                    table_name,
                    column_name,
                    e
                )
            })?;

        if exists.is_none() {
            return Err(anyhow!(
                "starrocks_sink: ALTER TABLE {}.{} ADD COLUMN {} reported success \
                 but the column did not appear in INFORMATION_SCHEMA. \
                 Check fast_schema_evolution=true is set on the table.",
                self.database,
                table_name,
                column_name
            ));
        }
        Ok(())
    }

    async fn table_has_fast_schema_evolution(
        &self,
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<bool> {
        let row: Option<(String,)> = conn
            .exec_first(
                "SELECT PROPERTIES FROM INFORMATION_SCHEMA.TABLES_CONFIG
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (&self.database, table_name),
            )
            .await
            .with_context(|| {
                format!(
                    "starrocks_sink: failed to query TABLES_CONFIG for {}.{}",
                    self.database, table_name
                )
            })?;

        let Some((properties,)) = row else {
            warn!(
                table = %table_name,
                "starrocks_sink: table not present in INFORMATION_SCHEMA.TABLES_CONFIG"
            );
            return Ok(false);
        };

        Ok(properties_contain_fast_schema_evolution(&properties))
    }
}

// ---------------------------------------------------------------------------
// Free functions (unit-testable)
// ---------------------------------------------------------------------------

/// Parse a StarRocks version string like `"3.2.4"`, `"3.3.0-rc01"`, or
/// `"3.4.1 8b4e8f9"` into `(major, minor)`. Returns `None` if unparseable.
fn parse_server_version(s: &str) -> Option<(u32, u32)> {
    let head = s
        .split(|c: char| c.is_whitespace() || c == '-')
        .next()?
        .trim();
    let mut parts = head.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Accept both the `SHOW CREATE TABLE` form (`"key" = "value"`) and the
/// `INFORMATION_SCHEMA.TABLES_CONFIG` JSON form (`"key":"value"`).
fn properties_contain_fast_schema_evolution(properties: &str) -> bool {
    let lower = properties.to_ascii_lowercase();
    let stripped: String = lower.chars().filter(|c| !c.is_whitespace()).collect();
    stripped.contains("\"fast_schema_evolution\"=\"true\"")
        || stripped.contains("\"fast_schema_evolution\":\"true\"")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::sinks::schema_evolution::AddedColumn;
    use crate::core::record::DataType;

    fn test_config() -> StarRocksSinkConfig {
        StarRocksSinkConfig {
            http_url: "http://localhost:9999".to_string(),
            mysql_port: 9999,
            database: "testdb".to_string(),
            user: "root".to_string(),
            password: String::new(),
            timeout_secs: 30,
            max_filter_ratio: 0.2,
        }
    }

    #[tokio::test]
    async fn apply_diff_empty_returns_ok_without_connecting() {
        // Pool::new doesn't establish a connection; only the first
        // get_conn() does. apply_diff with an empty diff short-circuits
        // before that, so this test verifies the fast path is genuinely
        // pure (no network).
        let se = StarRocksSchemaEvolution::new(&test_config()).unwrap();
        let diff = SchemaDiff::default();
        let result = se.apply_diff("orders", &diff).await;
        assert!(
            result.is_ok(),
            "empty diff must return Ok without contacting the server"
        );
    }

    #[tokio::test]
    async fn is_schema_evolution_enabled_defaults_to_false() {
        let se = StarRocksSchemaEvolution::new(&test_config()).unwrap();
        assert!(
            !se.is_schema_evolution_enabled("orders").await,
            "tables not added to the enabled set must default to false (degraded mode)"
        );
    }

    #[tokio::test]
    async fn is_schema_evolution_enabled_true_after_manual_insert() {
        // Manually populate the set to verify the read-side logic without
        // running the catalog query.
        let se = StarRocksSchemaEvolution::new(&test_config()).unwrap();
        se.schema_evolution_enabled
            .write()
            .await
            .insert("orders".to_string());
        assert!(se.is_schema_evolution_enabled("orders").await);
        assert!(!se.is_schema_evolution_enabled("other").await);
    }

    #[tokio::test]
    async fn apply_diff_validates_table_name() {
        let se = StarRocksSchemaEvolution::new(&test_config()).unwrap();
        let mut diff = SchemaDiff::default();
        diff.added.push(AddedColumn {
            name: "tax".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            ordinal: 1,
            pg_type_id: Some(23),
        });
        // Table name with semicolon — must be rejected before any DB call.
        let result = se.apply_diff("orders; DROP TABLE", &diff).await;
        assert!(
            result.is_err(),
            "invalid table name must be rejected at validation"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Invalid table name") || err.contains("SQL injection"),
            "error must reference table-name validation; got: {err}"
        );
    }

    #[test]
    fn parse_version_simple() {
        assert_eq!(parse_server_version("3.2.4"), Some((3, 2)));
        assert_eq!(parse_server_version("3.3.0"), Some((3, 3)));
        assert_eq!(parse_server_version("4.0.0"), Some((4, 0)));
    }

    #[test]
    fn parse_version_with_rc_suffix() {
        assert_eq!(parse_server_version("3.3.0-rc01"), Some((3, 3)));
        assert_eq!(parse_server_version("3.2.0-alpha"), Some((3, 2)));
    }

    #[test]
    fn parse_version_with_build_hash() {
        assert_eq!(parse_server_version("3.4.1 8b4e8f9"), Some((3, 4)));
        assert_eq!(parse_server_version("3.2.0  abc123"), Some((3, 2)));
    }

    #[test]
    fn parse_version_invalid() {
        assert_eq!(parse_server_version(""), None);
        assert_eq!(parse_server_version("not-a-version"), None);
        assert_eq!(parse_server_version("3"), None);
        assert_eq!(parse_server_version("3."), None);
        assert_eq!(parse_server_version("3.x"), None);
    }

    #[test]
    fn fast_schema_evolution_property_detection() {
        // Canonical SHOW CREATE TABLE rendering with spaces.
        assert!(properties_contain_fast_schema_evolution(
            r#""replication_num" = "3", "fast_schema_evolution" = "true""#
        ));
        // No-space variant.
        assert!(properties_contain_fast_schema_evolution(
            r#""fast_schema_evolution"="true""#
        ));
        // Case variations.
        assert!(properties_contain_fast_schema_evolution(
            r#""Fast_Schema_Evolution" = "TRUE""#
        ));
        // Embedded among other properties.
        assert!(properties_contain_fast_schema_evolution(
            r#""bucket_size" = "10", "fast_schema_evolution" = "true", "replication_num" = "3""#
        ));
    }

    #[test]
    fn fast_schema_evolution_property_detection_json_format() {
        // INFORMATION_SCHEMA.TABLES_CONFIG.PROPERTIES returns JSON, not the
        // SHOW CREATE TABLE rendering — this is the format actually used in
        // production. Regression test for a real bug found running against
        // StarRocks 3.3.22 where `INFORMATION_SCHEMA.TABLES_CONFIG` returned
        // `"fast_schema_evolution":"true"` (JSON, colon) but the matcher
        // only accepted the `=` form.
        assert!(properties_contain_fast_schema_evolution(
            r#"{"compression":"LZ4","fast_schema_evolution":"true","replication_num":"1"}"#
        ));
        // Bare key-value JSON without surrounding object.
        assert!(properties_contain_fast_schema_evolution(
            r#""fast_schema_evolution":"true""#
        ));
        // JSON form set to false → must NOT detect.
        assert!(!properties_contain_fast_schema_evolution(
            r#"{"compression":"LZ4","fast_schema_evolution":"false"}"#
        ));
    }

    #[test]
    fn fast_schema_evolution_property_absent() {
        assert!(!properties_contain_fast_schema_evolution(""));
        assert!(!properties_contain_fast_schema_evolution(
            r#""replication_num" = "3""#
        ));
        // Property is set to false.
        assert!(!properties_contain_fast_schema_evolution(
            r#""fast_schema_evolution" = "false""#
        ));
        // Different key that contains the substring (not actually our key).
        assert!(!properties_contain_fast_schema_evolution(
            r#""fast_schema_evolution_disabled" = "true""#
        ));
    }
}
