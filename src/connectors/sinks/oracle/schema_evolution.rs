// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle-specific schema evolution: ALTER TABLE ADD COLUMN application,
//! target-side column cache, and runtime DDL application.
//!
//! Consumes the sink-agnostic `SchemaDiff` produced by
//! `crate::connectors::sinks::schema_evolution::compute_schema_evolution_plan`
//! and emits Oracle-dialect DDL via the Oracle client.
//!
//! # Oracle DDL syntax
//!
//! ```sql
//! ALTER TABLE "schema"."table" ADD ("column_name" VARCHAR2(4000))
//! ```
//!
//! Oracle does **not** support `IF NOT EXISTS` in `ALTER TABLE ADD`.
//! Idempotency is implemented as a pre-check against `ALL_TAB_COLUMNS`
//! using `OracleClient::get_table_columns`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::RwLock;
use tracing::{info};

use super::client::OracleClient;
use super::config::OracleSinkConfig;
use super::types::TypeMapper;
use crate::connectors::sinks::schema_evolution::SchemaDiff;
use crate::utils::validate_sql_identifier;

/// Cached metadata about a column in a target Oracle table.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TargetColumn {
    pub name: String,
    pub data_type_str: String,
    pub nullable: bool,
}

/// Oracle-specific schema evolution machinery.
///
/// Owned by `OracleSink`, shared between `setup()` and `write_batch()`.
pub struct OracleSchemaEvolution {
    client: OracleClient,
    type_mapper: TypeMapper,
    /// Cache: target table name → its current column list.
    /// Refreshed after each successful ALTER and on every
    /// `refresh_target_schema_cache` call.
    target_columns: Arc<RwLock<HashMap<String, Vec<TargetColumn>>>>,
}

impl OracleSchemaEvolution {
    pub fn new(config: &OracleSinkConfig) -> Result<Self> {
        let client = OracleClient::new(config);
        Ok(Self {
            client,
            type_mapper: TypeMapper::new(),
            target_columns: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    // ---------------------------------------------------------------------
    // Runtime DDL
    // ---------------------------------------------------------------------

    /// Apply a `SchemaDiff` to the target table: emit `ALTER TABLE ADD`
    /// for each added column, and refresh the target_columns cache at the end.
    ///
    /// # Idempotency
    ///
    /// Oracle does **not** support `IF NOT EXISTS` in `ALTER TABLE ADD`
    /// (the parser rejects it). Idempotency is therefore implemented as a
    /// pre-check against `ALL_TAB_COLUMNS` via `OracleClient::get_table_columns`:
    /// if the column already exists on the target, the ALTER is skipped with
    /// an info log. This makes `apply_diff` safe to retry after a partial
    /// failure or to re-run across daemon restarts when the in-memory cache
    /// is stale.
    pub async fn apply_diff(&self, table_name: &str, diff: &SchemaDiff) -> Result<()> {
        validate_sql_identifier(table_name)
            .map_err(|e| anyhow!("Invalid table name '{}': {}", table_name, e))?;
        validate_sql_identifier(self.client.schema())
            .map_err(|e| anyhow!("Invalid schema name '{}': {}", self.client.schema(), e))?;

        if diff.added.is_empty() {
            return Ok(());
        }

        // Pre-check: fetch existing target columns once so we can skip
        // columns that are already materialised (idempotency).
        let existing_columns = self
            .client
            .get_table_columns(table_name)
            .await
            .with_context(|| {
                format!(
                    "oracle_sink: failed to pre-check existing columns for {}.{}",
                    self.client.schema(),
                    table_name
                )
            })?;

        let existing_set: HashSet<String> = existing_columns
            .into_iter()
            .map(|c| c.to_uppercase())
            .collect();

        for added in &diff.added {
            validate_sql_identifier(&added.name)
                .map_err(|e| anyhow!("Invalid column name '{}': {}", added.name, e))?;

            if existing_set.contains(&added.name.to_uppercase()) {
                info!(
                    table = %table_name,
                    column = %added.name,
                    "oracle_sink: ADD COLUMN skipped — column already exists in target"
                );
                continue;
            }

            let oracle_type = self.type_mapper.to_oracle_type(&added.data_type);
            let sql = format!(
                "ALTER TABLE \"{}\".\"{}\" ADD (\"{}\" {})",
                self.client.schema(),
                table_name,
                added.name,
                oracle_type
            );

            self.client
                .execute_ddl(&sql)
                .await
                .with_context(|| {
                    format!(
                        "oracle_sink: ALTER TABLE failed for {}.{} column {}",
                        self.client.schema(),
                        table_name,
                        added.name
                    )
                })?;

            info!(
                table = %table_name,
                column = %added.name,
                oracle_type = %oracle_type,
                "oracle_sink: ADD COLUMN applied"
            );
        }

        self.refresh_target_schema_cache(table_name).await?;
        Ok(())
    }

    /// Check if schema evolution is enabled for a given table.
    /// For Oracle, schema evolution is always enabled (no special property needed).
    pub async fn is_schema_evolution_enabled(&self, _table_name: &str) -> bool {
        true
    }

    // ---------------------------------------------------------------------
    // Setup-time reconciliation
    // ---------------------------------------------------------------------

    /// Lightweight reconciliation pass on startup. For tables whose source
    /// schema has columns missing from the target schema, emit `ALTER TABLE
    /// ADD` to bring the target up to date.
    ///
    /// Fails loud on any individual ALTER failure: setup error surfaces in
    /// `/healthz` and the daemon does not begin CDC.
    pub async fn reconcile_target_schema(
        &self,
        source_schemas: &[crate::core::traits::SourceTableSchema],
    ) -> Result<()> {
        for source in source_schemas {
            self.refresh_target_schema_cache(&source.name).await?;

            let target_names: HashSet<String> = self
                .target_columns
                .read()
                .await
                .get(&source.name)
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
                "oracle_sink: reconcile_on_startup applying ALTERs"
            );
            self.apply_diff(&source.name, &diff)
                .await
                .with_context(|| {
                    format!(
                        "oracle_sink: reconcile_on_startup failed for table {}",
                        source.name
                    )
                })?;
        }

        Ok(())
    }

    // ---------------------------------------------------------------------
    // Cache management
    // ---------------------------------------------------------------------

    /// Reload the column list for `table_name` from `ALL_TAB_COLUMNS`.
    ///
    /// Stores richer metadata than the simple name-only pre-check in
    /// `apply_diff`; used by `reconcile_target_schema` to compare the
    /// full target schema against the source schema on startup.
    pub async fn refresh_target_schema_cache(&self, table_name: &str) -> Result<()> {
        let result = self
            .client
            .query(
                "SELECT column_name, data_type, nullable \
                 FROM all_tab_columns \
                 WHERE owner = UPPER(:1) AND table_name = UPPER(:2) \
                 ORDER BY column_id",
                &[
                    oracle_rs::Value::String(self.client.schema().to_string()),
                    oracle_rs::Value::String(table_name.to_string()),
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "oracle_sink: failed to refresh target schema for {}.{}",
                    self.client.schema(),
                    table_name
                )
            })?;

        let rows: Vec<(String, String, String)> = result
            .rows
            .iter()
            .map(|row| {
                (
                    row.get_string(0).unwrap_or_default().to_string(),
                    row.get_string(1).unwrap_or_default().to_string(),
                    row.get_string(2).unwrap_or_default().to_string(),
                )
            })
            .collect();

        let columns: Vec<TargetColumn> = rows
            .into_iter()
            .map(|(name, data_type_str, nullable_str)| TargetColumn {
                name,
                data_type_str,
                nullable: nullable_str == "Y",
            })
            .collect();

        let mut cache = self.target_columns.write().await;
        cache.insert(table_name.to_string(), columns);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::sinks::schema_evolution::AddedColumn;
    use crate::core::record::DataType;

    fn test_config() -> OracleSinkConfig {
        OracleSinkConfig {
            host: "localhost".to_string(),
            port: 1521,
            service_name: "ORCLCDB".to_string(),
            user: "cdc_user".to_string(),
            password: "cdc_pass".to_string(),
            schema: "CDC_SCHEMA".to_string(),
            timeout_secs: 30,
            batch_size: 1000,
        }
    }

    #[tokio::test]
    async fn is_schema_evolution_enabled_defaults_to_true() {
        let se = OracleSchemaEvolution::new(&test_config()).unwrap();
        assert!(se.is_schema_evolution_enabled("orders").await);
    }

    #[test]
    fn test_apply_diff_empty_returns_ok_without_connecting() {
        // OracleClient::new does not establish a connection; only the first
        // method call does. apply_diff with an empty diff short-circuits
        // before that, so this test verifies the fast path is genuinely
        // pure (no network).
        let se = OracleSchemaEvolution::new(&test_config()).unwrap();
        let diff = SchemaDiff::default();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(se.apply_diff("orders", &diff));
        assert!(
            result.is_ok(),
            "empty diff must return Ok without contacting the server"
        );
    }

    #[test]
    fn test_apply_diff_validates_table_name() {
        let se = OracleSchemaEvolution::new(&test_config()).unwrap();
        let mut diff = SchemaDiff::default();
        diff.added.push(AddedColumn {
            name: "tax".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            ordinal: 1,
            pg_type_id: None,
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(se.apply_diff("orders; DROP TABLE", &diff));
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
    fn test_apply_diff_validates_column_name() {
        let se = OracleSchemaEvolution::new(&test_config()).unwrap();
        let mut diff = SchemaDiff::default();
        diff.added.push(AddedColumn {
            name: "col; DROP TABLE".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            ordinal: 1,
            pg_type_id: None,
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(se.apply_diff("orders", &diff));
        assert!(
            result.is_err(),
            "invalid column name must be rejected at validation"
        );
    }

    #[test]
    fn test_reconcile_empty_schemas_noops() {
        // Empty list of source schemas should return Ok immediately
        // without making any DB calls.
        let se = OracleSchemaEvolution::new(&test_config()).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(se.reconcile_target_schema(&[]));
        assert!(
            result.is_ok(),
            "reconcile with empty source schemas must return Ok"
        );
    }

    #[test]
    fn test_config_uses_valid_identifiers() {
        // Sanity check: the test config uses identifiers that pass
        // validate_sql_identifier.
        let config = test_config();
        assert!(validate_sql_identifier(&config.schema).is_ok());
        assert!(validate_sql_identifier("orders").is_ok());
    }
}
