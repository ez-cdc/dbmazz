// Copyright 2025
// Licensed under the Elastic License v2.0

//! Authoritative target-side schema tracking for the PG sink.
//!
//! Owns the `_dbmazz._schema_tracking` metadata table and the in-memory
//! `SchemaState` (`Arc<HashMap<qualified_name, SourceTableSchema>>`). The
//! tracking table is the source of truth; the in-memory cache is a
//! performance optimization for the normalizer's MERGE builder.
//!
//! Key invariants:
//! - The tracking table is always updated transactionally with the DDL that
//!   materializes the column on the target. No partial state.
//! - `diff_against_cache` compares by column NAME, never by position
//!   (pgoutput Relation messages carry all columns in source order, which
//!   may change independently of the column set).
//! - All DDL identifiers are double-quoted to prevent SQL injection and to
//!   handle reserved words.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_postgres::{Client, Transaction};
use tracing::{error, info, warn};

use crate::connectors::sinks::postgres::types::pg_oid_to_target_type;
use crate::connectors::sinks::schema_evolution::AddedColumn;
use crate::core::record::TableRef;
use crate::core::traits::{SourceColumn, SourceTableSchema};
use crate::source::converter::pg_type_to_data_type;

// Re-export the shared diff function so any future external caller of
// `schema_tracking::diff_against_cache` keeps compiling. Internal PG-sink
// callers (raw_table.rs) consume the function directly from the shared
// module via `crate::connectors::sinks::schema_evolution`.
#[allow(unused_imports)]
pub use crate::connectors::sinks::schema_evolution::diff_against_cache;

/// In-memory schema cache: qualified table name → `SourceTableSchema`.
///
/// Key format: `"<src_schema>.<src_table>"` — matches the normalizer's
/// existing `format!("{}.{}", schema, name)` convention.
pub type SchemaState = Arc<HashMap<String, SourceTableSchema>>;

/// Fully-qualified name of the tracking table.
pub const TRACKING_TABLE: &str = "_dbmazz._schema_tracking";

// ---------------------------------------------------------------------------
// Public types — moved to `crate::connectors::sinks::schema_evolution`.
// `SchemaDiff`, `AddedColumn`, `TypeChange`, and `diff_against_cache` are
// imported above and re-exported for backward-compatible call sites.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// DDL helpers
// ---------------------------------------------------------------------------

/// Create `_dbmazz._schema_tracking` if it does not exist.
///
/// Pre: the `_dbmazz` schema already exists (created by `setup::run_setup`).
/// Post: the table exists with the schema described in solution doc §5.1.
/// Errors: bubbles up any DDL error. Not transactional — standalone `batch_execute`.
pub async fn create_tracking_table(client: &Client) -> Result<()> {
    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {} (
            job_name     TEXT        NOT NULL,
            src_schema   TEXT        NOT NULL,
            src_table    TEXT        NOT NULL,
            column_name  TEXT        NOT NULL,
            pg_type_id   OID         NOT NULL,
            ordinal      INT         NOT NULL,
            nullable     BOOLEAN     NOT NULL,
            added_at_lsn BIGINT      NOT NULL DEFAULT 0,
            added_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (job_name, src_schema, src_table, column_name)
        )"#,
        TRACKING_TABLE
    );

    client
        .batch_execute(&ddl)
        .await
        .context("Failed to create _dbmazz._schema_tracking table")?;

    info!("  [OK] Schema tracking table {} ready", TRACKING_TABLE);
    Ok(())
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

/// Read every row of `_schema_tracking` for `job_name` and return a
/// `HashMap<qualified_name, SourceTableSchema>`. Columns are ordered by
/// `ordinal ASC`. Primary keys are NOT stored in the tracking table — they
/// are carried over from the caller-supplied `source_pk_lookup`.
///
/// Pre: table exists. May return an empty map (first-ever run → caller must
/// call `seed_initial_state`).
/// Post: returned map is a point-in-time snapshot. Caller owns it.
/// Errors: query failure.
pub async fn load_tracking_state(
    client: &Client,
    job_name: &str,
    source_pk_lookup: &HashMap<String, Vec<String>>,
) -> Result<HashMap<String, SourceTableSchema>> {
    let rows = client
        .query(
            &format!(
                "SELECT src_schema, src_table, column_name, pg_type_id::int8, ordinal, nullable
                 FROM {}
                 WHERE job_name = $1
                 ORDER BY src_schema, src_table, ordinal ASC",
                TRACKING_TABLE
            ),
            &[&job_name],
        )
        .await
        .context("Failed to query _dbmazz._schema_tracking")?;

    // Group rows by qualified name, then build SourceTableSchema per group.
    // We accumulate columns in a Vec (already ordered by `ordinal ASC`).
    let mut table_columns: HashMap<String, (String, String, Vec<SourceColumn>)> = HashMap::new();

    for row in &rows {
        let src_schema: &str = row.get(0);
        let src_table: &str = row.get(1);
        let column_name: &str = row.get(2);
        // pg_type_id is stored as OID (which tokio-postgres maps to u32), but
        // we cast it to int8 in the query to avoid OID-as-u32 codec issues.
        let pg_type_id_i64: i64 = row.get(3);
        #[allow(clippy::cast_sign_loss)]
        let pg_type_id = pg_type_id_i64 as u32;
        let _ordinal: i32 = row.get(4);
        let nullable: bool = row.get(5);

        let qn = format!("{}.{}", src_schema, src_table);
        let entry = table_columns
            .entry(qn)
            .or_insert_with(|| (src_schema.to_owned(), src_table.to_owned(), Vec::new()));

        entry.2.push(SourceColumn {
            name: column_name.to_owned(),
            data_type: pg_type_to_data_type(pg_type_id),
            nullable,
            pg_type_id,
        });
    }

    let mut result: HashMap<String, SourceTableSchema> =
        HashMap::with_capacity(table_columns.len());

    for (qn, (src_schema, src_table, columns)) in table_columns {
        let primary_keys = source_pk_lookup.get(&qn).cloned().unwrap_or_default();

        result.insert(
            qn,
            SourceTableSchema {
                schema: src_schema,
                name: src_table,
                columns,
                primary_keys,
            },
        );
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Write paths
// ---------------------------------------------------------------------------

/// First-ever run: the tracking table has zero rows for this `job_name`.
/// Insert one row per column per replicated table with `added_at_lsn = 0`.
/// Wrapped in a single transaction — either the whole baseline lands or
/// nothing does.
///
/// Pre: `_schema_tracking` exists and has no rows for `job_name`.
/// Post: one row per column per table, PK-deduplicated via ON CONFLICT.
/// Errors: any INSERT failure → caller retries setup.
pub async fn seed_initial_state(
    client: &mut Client,
    job_name: &str,
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    let tx = client
        .transaction()
        .await
        .context("Failed to begin seed_initial_state transaction")?;

    for schema in source_schemas {
        for (ordinal, col) in schema.columns.iter().enumerate() {
            #[allow(clippy::cast_possible_wrap)]
            let ordinal_i32 = ordinal as i32;
            tx.execute(
                &format!(
                    "INSERT INTO {} (job_name, src_schema, src_table, column_name, pg_type_id, ordinal, nullable, added_at_lsn)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, 0)
                     ON CONFLICT DO NOTHING",
                    TRACKING_TABLE
                ),
                // pg_type_id is bound as &u32 — tokio-postgres maps u32 → OID
                // natively. Do NOT bind as i64 (would send INT8 and PG won't
                // implicitly cast BIGINT → OID).
                &[
                    &job_name,
                    &schema.schema,
                    &schema.name,
                    &col.name,
                    &col.pg_type_id,
                    &ordinal_i32,
                    &col.nullable,
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to seed tracking row for {}.{}.{}",
                    schema.schema, schema.name, col.name
                )
            })?;
        }
    }

    tx.commit()
        .await
        .context("Failed to commit seed_initial_state transaction")?;

    info!(
        "  [OK] Seeded {} tracking rows across {} tables",
        source_schemas
            .iter()
            .map(|s| s.columns.len())
            .sum::<usize>(),
        source_schemas.len()
    );
    Ok(())
}

/// Runtime path: insert one row per newly-added column inside an existing
/// batch transaction. Called from `raw_table::write_batch_to_raw` after
/// `ALTER TABLE` succeeds for that column. `ON CONFLICT DO NOTHING` makes
/// the whole operation idempotent under batch retry.
///
/// Pre: `tx` is active, caller has already run `ALTER TABLE` for each col.
/// Post: tracking table has one row per column (or the row already existed).
/// Errors: DB failure rolls back the whole tx.
pub async fn insert_tracked_columns(
    tx: &Transaction<'_>,
    job_name: &str,
    table: &TableRef,
    added: &[AddedColumn],
    lsn: u64,
) -> Result<()> {
    // PG source's pgoutput Relation always carries a non-empty namespace, so
    // TableRef.schema should always be Some(...) here. Fail loudly rather than
    // silently defaulting to "public" — a "public" row would never match
    // tracking rows written by `seed_initial_state` / `reconcile_on_startup`
    // for tables that actually live in another schema.
    let src_schema = table.schema.as_deref().with_context(|| {
        format!(
            "insert_tracked_columns: TableRef.schema is None for table {} — incompatible with PG source",
            table.name
        )
    })?;
    // LSN stored as BIGINT; reinterpret the u64 bit pattern as i64 (same
    // approach as lsn_offset in _metadata).
    #[allow(clippy::cast_possible_wrap)]
    let lsn_i64 = lsn as i64;

    for col in added {
        // pg_type_id is `Option<u32>` on the shared `AddedColumn` (sink-agnostic).
        // The PG source always populates it; `.expect()` makes the contract loud
        // for any future non-PG source that wires SchemaChange events into
        // this PG sink path (which would be a misconfiguration).
        let pg_type_id = col
            .pg_type_id
            .expect("PG sink: AddedColumn must carry pg_type_id (PG source contract)");
        tx.execute(
            &format!(
                "INSERT INTO {} (job_name, src_schema, src_table, column_name, pg_type_id, ordinal, nullable, added_at_lsn)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT DO NOTHING",
                TRACKING_TABLE
            ),
            // pg_type_id bound as &u32 → OID. See note in seed_initial_state.
            &[
                &job_name,
                &src_schema,
                &table.name,
                &col.name,
                &pg_type_id,
                &col.ordinal,
                &col.nullable,
                &lsn_i64,
            ],
        )
        .await
        .with_context(|| {
            format!(
                "Failed to insert tracking row for {}.{}.{}",
                src_schema, table.name, col.name
            )
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Diff logic — moved to `crate::connectors::sinks::schema_evolution`.
// `diff_against_cache` is re-exported at the top of this module for
// backward compatibility with existing call sites.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Startup reconciliation
// ---------------------------------------------------------------------------

/// Restart path: the tracking table has rows for this `job_name` from a
/// previous run, and the source may have evolved while the daemon was down.
/// Compare the tracking state to `source_schemas` (just fetched from
/// `information_schema` by the engine) and bring the target + tracking into
/// agreement.
///
/// For each `(schema, table)`:
/// 1. Column in `source_schemas` but not in `_schema_tracking` →
///    `ALTER TABLE ADD COLUMN IF NOT EXISTS` on target + insert tracking row.
/// 2. Column in `_schema_tracking` but not in `source_schemas` →
///    `warn!` (dropped on source while down). Leave target + tracking alone.
/// 3. Both sides agree by name but `pg_type_id` differs → `error!`, leave
///    tracking as-is. MERGE keeps the old cast. Operator intervention required.
///
/// Returns the final reconciled `HashMap<String, SourceTableSchema>`
/// reflecting the tracking state AFTER step 1's inserts.
///
/// Transaction boundary: ONE atomic tx for the entire reconcile loop.
pub async fn reconcile_on_startup(
    client: &mut Client,
    target_schema: &str,
    job_name: &str,
    source_schemas: &[SourceTableSchema],
    mut cache: HashMap<String, SourceTableSchema>,
) -> Result<HashMap<String, SourceTableSchema>> {
    let tx = client
        .transaction()
        .await
        .context("Failed to begin reconcile_on_startup transaction")?;

    for source in source_schemas {
        let qn = format!("{}.{}", source.schema, source.name);

        // Build a name→SourceColumn lookup for the cached state of this table.
        let cached_by_name: HashMap<&str, &SourceColumn> = cache
            .get(&qn)
            .map(|s| s.columns.iter().map(|c| (c.name.as_str(), c)).collect())
            .unwrap_or_default();

        // Next ordinal starts after however many columns are already tracked.
        let base_ordinal: i32 = cached_by_name.len() as i32;
        let mut next_ordinal = base_ordinal;

        let mut newly_added: Vec<AddedColumn> = Vec::new();

        for source_col in &source.columns {
            match cached_by_name.get(source_col.name.as_str()) {
                None => {
                    // Column in source but not in tracking → ADD COLUMN.
                    let added = AddedColumn {
                        name: source_col.name.clone(),
                        data_type: source_col.data_type.clone(),
                        nullable: true, // always nullable on reconcile (Risk §5.7)
                        ordinal: next_ordinal,
                        pg_type_id: Some(source_col.pg_type_id),
                    };

                    let sql = alter_add_column_sql(target_schema, &source.name, &added);
                    tx.batch_execute(&sql).await.with_context(|| {
                        format!(
                            "reconcile_on_startup: ALTER TABLE failed for {}.{} column {}",
                            target_schema, source.name, source_col.name
                        )
                    })?;

                    tx.execute(
                        &format!(
                            "INSERT INTO {} (job_name, src_schema, src_table, column_name, pg_type_id, ordinal, nullable, added_at_lsn)
                             VALUES ($1, $2, $3, $4, $5, $6, $7, 0)
                             ON CONFLICT DO NOTHING",
                            TRACKING_TABLE
                        ),
                        // pg_type_id bound as &u32 → OID. See note in seed_initial_state.
                        &[
                            &job_name,
                            &source.schema,
                            &source.name,
                            &source_col.name,
                            &source_col.pg_type_id,
                            &added.ordinal,
                            &added.nullable,
                        ],
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "reconcile_on_startup: INSERT tracking row failed for {}.{}.{}",
                            source.schema, source.name, source_col.name
                        )
                    })?;

                    info!(
                        table = %qn,
                        column = %source_col.name,
                        "reconcile_on_startup: added column to target"
                    );
                    newly_added.push(added);
                    next_ordinal += 1;
                }
                Some(cached_col) => {
                    // Column present on both sides — check for type mismatch.
                    if cached_col.pg_type_id != source_col.pg_type_id {
                        error!(
                            table = %qn,
                            column = %source_col.name,
                            old_oid = cached_col.pg_type_id,
                            new_oid = source_col.pg_type_id,
                            "reconcile_on_startup: type change not supported, MERGE keeps old cast; operator intervention required"
                        );
                    }
                }
            }
        }

        // Warn about columns that were dropped on the source while down.
        let source_col_names: HashMap<&str, ()> = source
            .columns
            .iter()
            .map(|c| (c.name.as_str(), ()))
            .collect();
        for cached_col in cached_by_name.values() {
            if !source_col_names.contains_key(cached_col.name.as_str()) {
                warn!(
                    table = %qn,
                    column = %cached_col.name,
                    "reconcile_on_startup: column dropped on source while daemon was down; target keeps dead column"
                );
            }
        }

        // Advance the in-memory cache with any newly-added columns so the
        // returned state is consistent with the tracking table post-commit.
        if !newly_added.is_empty() {
            let entry = cache
                .entry(qn.clone())
                .or_insert_with(|| SourceTableSchema {
                    schema: source.schema.clone(),
                    name: source.name.clone(),
                    columns: Vec::new(),
                    primary_keys: source.primary_keys.clone(),
                });
            for added in newly_added {
                let pg_type_id = added
                    .pg_type_id
                    .expect("PG sink: AddedColumn must carry pg_type_id (PG source contract)");
                entry.columns.push(SourceColumn {
                    name: added.name,
                    data_type: pg_type_to_data_type(pg_type_id),
                    nullable: added.nullable,
                    pg_type_id,
                });
            }
        }
    }

    tx.commit()
        .await
        .context("Failed to commit reconcile_on_startup transaction")?;

    info!("  [OK] reconcile_on_startup complete");
    Ok(cache)
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Build `ALTER TABLE "<target_schema>"."<table_name>" ADD COLUMN IF NOT EXISTS
/// "<col_name>" <type>`. Always nullable with no default — matches backfill
/// semantics (NULL for pre-existing rows). All identifiers are double-quoted.
fn alter_add_column_sql(target_schema: &str, table_name: &str, column: &AddedColumn) -> String {
    let pg_type_id = column
        .pg_type_id
        .expect("PG sink: AddedColumn must carry pg_type_id (PG source contract)");
    let col_type = pg_oid_to_target_type(pg_type_id);
    format!(
        r#"ALTER TABLE "{}"."{}" ADD COLUMN IF NOT EXISTS "{}" {}"#,
        target_schema, table_name, column.name, col_type
    )
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;

    // Diff-logic tests live in `crate::connectors::sinks::schema_evolution`
    // (the shared module). Tests here cover the PG-sink-specific helpers
    // (`alter_add_column_sql`).

    // ------------------------------------------------------------------
    // alter_add_column_sql tests
    // ------------------------------------------------------------------

    #[test]
    fn test_alter_add_column_sql_basic() {
        let col = AddedColumn {
            name: "description".to_owned(),
            data_type: DataType::String,
            nullable: true,
            ordinal: 3,
            pg_type_id: Some(25), // text
        };
        let sql = alter_add_column_sql("public", "orders", &col);
        assert_eq!(
            sql,
            r#"ALTER TABLE "public"."orders" ADD COLUMN IF NOT EXISTS "description" text"#
        );
    }

    #[test]
    fn test_alter_add_column_sql_quotes_identifier() {
        // Table name that is a reserved word must be quoted correctly.
        let col = AddedColumn {
            name: "amount".to_owned(),
            data_type: DataType::Decimal {
                precision: 38,
                scale: 9,
            },
            nullable: true,
            ordinal: 1,
            pg_type_id: Some(1700), // numeric
        };
        let sql = alter_add_column_sql("public", "order", &col);
        assert!(
            sql.contains(r#""public"."order""#),
            "reserved-word table name must be double-quoted; got: {sql}"
        );
        assert!(
            sql.contains(r#""amount""#),
            "column name must be double-quoted; got: {sql}"
        );
    }
}
