// Copyright 2025
// Licensed under the Elastic License v2.0

//! Snowflake-specific schema evolution: ALTER TABLE ADD COLUMN application
//! inside `BEGIN/COMMIT`, target-side schema cache, and reconcile-on-startup.
//!
//! Consumes the sink-agnostic `SchemaDiff` produced by
//! `crate::connectors::sinks::schema_evolution::compute_schema_evolution_plan`
//! and emits Snowflake-dialect DDL via the HTTP client.
//!
//! # Conservative policy
//!
//! - **ADD COLUMN** → auto-applied via `ALTER TABLE ADD COLUMN IF NOT EXISTS`
//!   inside a transaction.
//! - **DROP / MODIFY / RENAME** → handled by the shared `compute_schema_evolution_plan`
//!   which logs the events and excludes them from the pending diffs. Target
//!   keeps the dead column / old type.
//!
//! # Privilege validation
//!
//! Snowflake's existing `setup::run_setup` already issues `ALTER TABLE ADD
//! COLUMN` for the four audit columns (`_DBMAZZ_OP_TYPE`, etc.) — so the
//! runtime privilege requirement is implicitly validated at setup-time.
//! `validate_alter_privilege` here logs that fact and returns `Ok` rather
//! than re-running an explicit probe, to avoid emitting catalog mutations
//! that would clutter audit logs in production accounts.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::client::SnowflakeClient;
use super::types::TypeMapper;
use crate::connectors::sinks::schema_evolution::SchemaDiff;
use crate::core::traits::SourceTableSchema;
use crate::utils::validate_sql_identifier;

/// Cached metadata about a column in a target Snowflake table. Built from
/// `INFORMATION_SCHEMA.COLUMNS` on demand. Used purely for diff computation
/// and observability — not for type checks (those are coarse, name-based).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TargetColumn {
    pub name: String,
    pub data_type_str: String,
    pub nullable: bool,
}

/// Snowflake schema evolution machinery.
///
/// Owned by `SnowflakeSink`, shared between `setup()` and `write_batch()`
/// via reference — the underlying client is already an `Arc` so cloning is
/// cheap.
pub struct SnowflakeSchemaEvolution {
    client: Arc<SnowflakeClient>,
    database: String,
    schema: String,
    type_mapper: TypeMapper,
    /// Cache: target table name (UPPERCASE Snowflake convention) → its
    /// current column list. Refreshed after each successful ALTER and on
    /// every `refresh_target_schema_cache` call.
    target_columns: Arc<RwLock<HashMap<String, Vec<TargetColumn>>>>,
}

impl SnowflakeSchemaEvolution {
    pub fn new(client: Arc<SnowflakeClient>, database: String, schema: String) -> Self {
        Self {
            client,
            database,
            schema,
            type_mapper: TypeMapper::new(),
            target_columns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ---------------------------------------------------------------------
    // Setup-time validation
    // ---------------------------------------------------------------------

    /// Logs that ALTER TABLE privilege will be implicitly validated by the
    /// existing audit-column setup path. Always returns `Ok` — actual
    /// privilege failures surface loud at the next ALTER call site.
    pub fn validate_alter_privilege(&self) {
        info!(
            "  [OK] Snowflake ALTER TABLE privilege validates implicitly at setup \
             via existing audit-column ALTER. Runtime privilege failures halt the \
             pipeline loud (CdcState::Stopped) with the Snowflake error verbatim."
        );
    }

    /// Reconcile target schema with source schema on startup. For each table,
    /// compute the columns present in source but missing from target and
    /// emit `ALTER TABLE ADD COLUMN` inside one transaction per table. Halts
    /// loud on any DDL failure.
    pub async fn reconcile_target_schema(
        &self,
        source_schemas: &[SourceTableSchema],
    ) -> Result<()> {
        for source in source_schemas {
            // Snowflake unquoted identifiers normalise to UPPERCASE in
            // INFORMATION_SCHEMA, so the cache key is uppercase here.
            let target_name = source.name.to_uppercase();
            self.refresh_target_schema_cache(&target_name).await?;

            let target_names: HashSet<String> = self
                .target_columns
                .read()
                .await
                .get(&target_name)
                .map(|cols| cols.iter().map(|c| c.name.to_uppercase()).collect())
                .unwrap_or_default();

            let mut diff = SchemaDiff::default();
            for (i, src_col) in source.columns.iter().enumerate() {
                if !target_names.contains(&src_col.name.to_uppercase()) {
                    diff.added
                        .push(crate::connectors::sinks::schema_evolution::AddedColumn {
                            name: src_col.name.clone(),
                            data_type: src_col.data_type.clone(),
                            nullable: true,
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
                "snowflake_sink: reconcile_on_startup applying ALTERs"
            );
            self.apply_diff(&source.name, &diff)
                .await
                .with_context(|| {
                    format!(
                        "snowflake_sink: reconcile_on_startup failed for table {}",
                        source.name
                    )
                })?;
        }

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Runtime DDL
    // ---------------------------------------------------------------------

    /// Apply a `SchemaDiff` to the target table inside a single transaction:
    /// `BEGIN; ALTER ...; ALTER ...; COMMIT`. On any DDL failure, ROLLBACK
    /// is attempted and the error is bubbled up.
    ///
    /// Snowflake rejects DEFAULT expressions on `ALTER TABLE ADD COLUMN` —
    /// added columns are nullable with no default. Backfill semantics: NULL
    /// for pre-existing rows, populated for any post-DDL writes.
    pub async fn apply_diff(&self, table_name: &str, diff: &SchemaDiff) -> Result<()> {
        validate_sql_identifier(table_name)
            .map_err(|e| anyhow!("Invalid table name '{}': {}", table_name, e))?;
        validate_sql_identifier(&self.database)
            .map_err(|e| anyhow!("Invalid database name '{}': {}", self.database, e))?;
        validate_sql_identifier(&self.schema)
            .map_err(|e| anyhow!("Invalid target schema name '{}': {}", self.schema, e))?;

        if diff.added.is_empty() {
            return Ok(());
        }

        self.client
            .execute("BEGIN TRANSACTION")
            .await
            .context("snowflake_sink: failed to BEGIN TRANSACTION for schema evolution")?;

        // Inner async block so we always issue COMMIT or ROLLBACK before returning.
        let result: Result<()> = async {
            for added in &diff.added {
                validate_sql_identifier(&added.name)
                    .map_err(|e| anyhow!("Invalid column name '{}': {}", added.name, e))?;

                let sf_type = self.type_mapper.to_snowflake_type(&added.data_type);
                let sql = format!(
                    r#"ALTER TABLE "{}"."{}"."{}" ADD COLUMN IF NOT EXISTS "{}" {}"#,
                    self.database.to_uppercase(),
                    self.schema.to_uppercase(),
                    table_name.to_uppercase(),
                    added.name.to_uppercase(),
                    sf_type
                );
                self.client.execute(&sql).await.with_context(|| {
                    format!(
                        "snowflake_sink: ALTER TABLE failed for {}.{}.{} column {}",
                        self.database, self.schema, table_name, added.name
                    )
                })?;
                info!(
                    table = %table_name,
                    column = %added.name,
                    snowflake_type = %sf_type,
                    "snowflake_sink: ADD COLUMN applied"
                );
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                self.client
                    .execute("COMMIT")
                    .await
                    .context("snowflake_sink: COMMIT failed after schema evolution ALTERs")?;
                self.refresh_target_schema_cache(&table_name.to_uppercase())
                    .await?;
                Ok(())
            }
            Err(err) => {
                if let Err(rb_err) = self.client.execute("ROLLBACK").await {
                    warn!(
                        rollback_error = %rb_err,
                        "snowflake_sink: ROLLBACK failed after ALTER error; the transaction may be left open until the connection closes"
                    );
                }
                Err(err)
            }
        }
    }

    pub async fn refresh_target_schema_cache(&self, table_name: &str) -> Result<()> {
        let database_upper = self.database.to_uppercase();
        let schema_upper = self.schema.to_uppercase();
        let table_upper = table_name.to_uppercase();
        let sql = format!(
            r#"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
               FROM "{}"."INFORMATION_SCHEMA"."COLUMNS"
               WHERE TABLE_SCHEMA = '{}'
                 AND TABLE_NAME = '{}'
               ORDER BY ORDINAL_POSITION"#,
            database_upper, schema_upper, table_upper
        );

        let result = self.client.execute(&sql).await.with_context(|| {
            format!(
                "snowflake_sink: failed to refresh target schema for {}.{}.{}",
                database_upper, schema_upper, table_upper
            )
        })?;

        let columns = parse_information_schema_rows(&result);

        let mut cache = self.target_columns.write().await;
        cache.insert(table_upper, columns);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Free functions (unit-testable)
// ---------------------------------------------------------------------------

/// Parse a Snowflake `INFORMATION_SCHEMA.COLUMNS` query result into a list
/// of `TargetColumn`s. The `data` field of `QueryResult` is JSON; rows are
/// arrays of stringified values.
fn parse_information_schema_rows(result: &super::client::QueryResult) -> Vec<TargetColumn> {
    let mut cols = Vec::new();
    let Some(ref data) = result.data else {
        return cols;
    };
    let Some(rows) = data.as_array() else {
        return cols;
    };
    for row in rows {
        let Some(arr) = row.as_array() else {
            continue;
        };
        let name = arr
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let data_type_str = arr
            .get(1)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let nullable = arr
            .get(2)
            .and_then(|v| v.as_str())
            .map(|s| s.eq_ignore_ascii_case("YES"))
            .unwrap_or(false);
        if !name.is_empty() {
            cols.push(TargetColumn {
                name,
                data_type_str,
                nullable,
            });
        }
    }
    cols
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::sinks::snowflake::client::QueryResult;

    fn make_result(rows: serde_json::Value) -> QueryResult {
        QueryResult {
            success: true,
            data: Some(rows),
            query_id: "test".to_string(),
            message: String::new(),
        }
    }

    // Note: full `SnowflakeSchemaEvolution::apply_diff` tests require a
    // live `SnowflakeClient` (which only constructs via a real `connect`
    // call). The verify suite in ez-cdc-cli covers those integration tests
    // end-to-end. Here we cover only the pure helpers that don't require
    // a client.

    #[test]
    fn parse_rows_empty_data() {
        let r = QueryResult {
            success: true,
            data: None,
            query_id: "test".to_string(),
            message: String::new(),
        };
        assert!(parse_information_schema_rows(&r).is_empty());
    }

    #[test]
    fn parse_rows_typical() {
        let r = make_result(serde_json::json!([
            ["ID", "NUMBER", "NO"],
            ["NAME", "VARCHAR", "YES"],
            ["CREATED_AT", "TIMESTAMP_TZ", "NO"]
        ]));
        let cols = parse_information_schema_rows(&r);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "ID");
        assert_eq!(cols[0].data_type_str, "NUMBER");
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "NAME");
        assert!(cols[1].nullable);
        assert_eq!(cols[2].name, "CREATED_AT");
        assert!(!cols[2].nullable);
    }

    #[test]
    fn parse_rows_skips_empty_names() {
        let r = make_result(serde_json::json!([
            ["", "NUMBER", "NO"],
            ["VALID", "VARCHAR", "YES"]
        ]));
        let cols = parse_information_schema_rows(&r);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "VALID");
    }

    #[test]
    fn parse_rows_handles_partial_rows() {
        // Row missing nullable column → defaults to NOT NULL.
        let r = make_result(serde_json::json!([["ID", "NUMBER"]]));
        let cols = parse_information_schema_rows(&r);
        assert_eq!(cols.len(), 1);
        assert!(!cols[0].nullable);
    }
}
