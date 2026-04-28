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
    /// Creates a new instance with its own MySQL pool. Independent from
    /// `StarRocksSetup`'s pool — both can coexist; `mysql_async::Pool` is
    /// `Clone` and internally Arc-counted, but we keep the construction
    /// site here for self-containment.
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
    /// Fails loud if older — does not arrancar in degraded mode for old
    /// versions because `fast_schema_evolution` does not exist there at all.
    ///
    /// Parses the result of `SELECT current_version()` (StarRocks) which
    /// returns a string like `"3.2.4"`, `"3.3.0-rc01"`, or `"3.4.1 8b4e8f9"`.
    pub async fn validate_server_version(&self) -> Result<()> {
        let mut conn = self.get_conn().await?;

        // StarRocks exposes its version via `current_version()`; MySQL-protocol
        // layer also accepts `SELECT @@version` but that returns a synthetic
        // value that includes a MySQL compat prefix on some builds. Prefer
        // `current_version()` for accuracy.
        let raw_version: Option<String> = conn
            .query_first("SELECT current_version()")
            .await
            .map_err(|e| anyhow!("Failed to query StarRocks server version: {}", e))?;

        let version_str = raw_version.ok_or_else(|| {
            anyhow!("StarRocks `SELECT current_version()` returned no row")
        })?;

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

    /// For each configured table, detect whether `fast_schema_evolution` is
    /// enabled. Tables with the property are added to the
    /// `schema_evolution_enabled` set; tables without are logged once with
    /// an actionable WARN.
    ///
    /// This is opportunistic — failure to query `INFORMATION_SCHEMA.TABLES_CONFIG`
    /// for a single table degrades that table to disabled and continues
    /// (rather than failing setup entirely). The catalog query itself
    /// failing for a known reason (permissions, missing table) bubbles up.
    pub async fn detect_fast_schema_evolution_per_table(
        &self,
        tables: &[String],
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;
        let mut enabled = self.schema_evolution_enabled.write().await;

        for table_qualified in tables {
            // Strip any schema prefix; StarRocks uses unqualified table names.
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
                // Not inserted into `enabled` — skip path activates at runtime.
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
                // Skip tables without the flag — degraded mode covers them.
                continue;
            }

            // Refresh the target column cache for this table.
            self.refresh_target_schema_cache(&source.name).await?;

            // Build a name set of the current target columns.
            let target_names: HashSet<String> = self
                .target_columns
                .read()
                .await
                .get(&source.name)
                .map(|cols| cols.iter().map(|c| c.name.to_lowercase()).collect())
                .unwrap_or_default();

            // Build a synthetic SchemaDiff with the missing source columns.
            let mut diff = SchemaDiff::default();
            for (i, src_col) in source.columns.iter().enumerate() {
                if !target_names.contains(&src_col.name.to_lowercase()) {
                    diff.added.push(crate::connectors::sinks::schema_evolution::AddedColumn {
                        name: src_col.name.clone(),
                        data_type: src_col.data_type.clone(),
                        nullable: true, // always nullable on reconcile
                        #[allow(clippy::cast_possible_wrap)]
                        ordinal: i as i32,
                        pg_type_id: Some(src_col.pg_type_id),
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
            self.apply_diff(&source.name, &diff).await.with_context(|| {
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

            let starrocks_type = self.type_mapper.to_starrocks_type(&added.data_type);
            // Backtick-quoted column name to handle reserved words; database
            // and table identifiers are validated above so direct interpolation
            // is safe (no user-controlled SQL injection vector).
            let sql = format!(
                "ALTER TABLE `{}`.`{}` ADD COLUMN IF NOT EXISTS `{}` {}",
                self.database, table_name, added.name, starrocks_type
            );
            conn.query_drop(&sql).await.with_context(|| {
                format!(
                    "starrocks_sink: ALTER TABLE failed for {}.{} column {}",
                    self.database, table_name, added.name
                )
            })?;

            // Verify the column actually appeared in INFORMATION_SCHEMA.
            // With fast_schema_evolution=true this is metadata-only and
            // returns within milliseconds; the verify is a defensive sanity
            // check rather than a real poll.
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

    /// Returns `true` if the table is in the schema-evolution-enabled set.
    pub async fn is_schema_evolution_enabled(&self, table_name: &str) -> bool {
        self.schema_evolution_enabled
            .read()
            .await
            .contains(table_name)
    }

    /// Reload the column list for `table_name` from `INFORMATION_SCHEMA.COLUMNS`.
    /// Stores the result in the in-memory cache. Used both at setup and after
    /// runtime ALTERs.
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

    /// Confirm a column appears in `INFORMATION_SCHEMA.COLUMNS` after an ALTER.
    /// With fast_schema_evolution this is metadata-only and instant; the
    /// check is a defensive sanity validation rather than a real poll.
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
            .map_err(|e| anyhow!("Failed to verify ALTER for {}.{}: {}", table_name, column_name, e))?;

        if exists.is_none() {
            return Err(anyhow!(
                "starrocks_sink: ALTER TABLE {}.{} ADD COLUMN {} reported success \
                 but the column did not appear in INFORMATION_SCHEMA. \
                 Check fast_schema_evolution=true is set on the table.",
                self.database, table_name, column_name
            ));
        }
        Ok(())
    }

    /// Query `INFORMATION_SCHEMA.TABLES_CONFIG` for the table's
    /// `PROPERTIES` string and check whether `"fast_schema_evolution" = "true"`
    /// appears in it.
    async fn table_has_fast_schema_evolution(
        &self,
        conn: &mut Conn,
        table_name: &str,
    ) -> Result<bool> {
        // `INFORMATION_SCHEMA.TABLES_CONFIG` is StarRocks-specific (3.2+).
        // PROPERTIES is a string column; format includes `"key" = "value"` pairs.
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
            // Table not in TABLES_CONFIG — should not happen if setup
            // verified existence already, but be defensive.
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
/// `"3.4.1 8b4e8f9"` into `(major, minor)`. Returns `None` if the format
/// is not recognised.
fn parse_server_version(s: &str) -> Option<(u32, u32)> {
    // Take everything before the first whitespace (build-hash suffix) and
    // the first dash (rc/alpha suffix).
    let head = s
        .split(|c: char| c.is_whitespace() || c == '-')
        .next()?
        .trim();
    let mut parts = head.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Check whether a `PROPERTIES` string from `INFORMATION_SCHEMA.TABLES_CONFIG`
/// contains `"fast_schema_evolution" = "true"`. Tolerates whitespace and
/// case variations.
fn properties_contain_fast_schema_evolution(properties: &str) -> bool {
    // Lowercase the whole string and look for the canonical key=true pattern.
    // Tolerates `"fast_schema_evolution"="true"` (no spaces) and the
    // canonical SHOW CREATE TABLE rendering with spaces.
    let lower = properties.to_ascii_lowercase();
    // Strip whitespace so `"fast_schema_evolution" = "true"` and
    // `"fast_schema_evolution"="true"` match the same needle.
    let stripped: String = lower.chars().filter(|c| !c.is_whitespace()).collect();
    stripped.contains("\"fast_schema_evolution\"=\"true\"")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
