// Copyright 2025
// Licensed under the Elastic License v2.0

//! Shared schema-evolution diff logic across sinks.
//!
//! Hosts the pure (no DB, no I/O, no locking) computation that turns a
//! batch of `CdcRecord` events into a list of `(TableRef, SchemaDiff)`
//! pairs. Each sink consumes the result and applies dialect-specific DDL
//! to its target via its own per-sink module.
//!
//! This file is sink-agnostic. PG-specific concerns (OID preservation,
//! `_dbmazz._schema_tracking` table) live in `postgres::schema_tracking`.
//!
//! # Conservative policy
//!
//! All sinks share the same policy (codified in the spec at
//! `openspec/changes/schema-evolution-starrocks-snowflake/specs/`):
//!
//! - **ADD COLUMN** → auto-applied via `ALTER TABLE ADD COLUMN IF NOT EXISTS`
//! - **DROP COLUMN** → logged as `warn!`, target keeps the dead column
//! - **MODIFY COLUMN type** → logged as `error!`, target keeps the old cast
//! - **RENAME COLUMN** → seen as drop+add, both above apply

use std::collections::HashMap;

use tracing::{error, warn};

use crate::core::record::{CdcRecord, ColumnDef, DataType, TableRef};
use crate::core::traits::{SourceColumn, SourceTableSchema};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A new column that needs to be materialized on the target.
///
/// `pg_type_id` is `Some(_)` when the source is Postgres (the only source
/// today). Sinks that don't need OID-level fidelity can ignore it and use
/// `data_type` instead.
#[derive(Debug, Clone)]
pub struct AddedColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// Zero-based ordinal relative to the in-memory snapshot passed to
    /// `compute_schema_evolution_plan`. Useful for sinks that maintain a
    /// per-job tracking table (Postgres). Other sinks can ignore.
    pub ordinal: i32,
    /// Source PG OID. Only populated when source is Postgres.
    pub pg_type_id: Option<u32>,
}

/// A column whose type changed between cache and the incoming schema.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TypeChange {
    pub name: String,
    pub old_data_type: DataType,
    pub new_data_type: DataType,
    /// Source-side OID before the change. Only populated when source is Postgres.
    pub old_pg_type_id: Option<u32>,
    /// Source-side OID after the change. Only populated when source is Postgres.
    pub new_pg_type_id: Option<u32>,
}

/// Result of diffing a `SchemaChange` event against the current target cache.
/// Empty diffs are no-ops.
#[derive(Debug, Default)]
pub struct SchemaDiff {
    /// Columns present in the new schema that are missing from the cache.
    pub added: Vec<AddedColumn>,
    /// Columns whose type disagrees between cache and the new schema.
    /// Handled by `error!` log; no DDL.
    pub type_changed: Vec<TypeChange>,
    /// Columns present in the cache but absent from the new schema (source
    /// dropped the column while the daemon was running).
    /// Handled by `warn!` log; no DDL. Target keeps the column as dead data.
    pub dropped: Vec<String>,
}

impl SchemaDiff {
    /// Returns `true` when there is nothing to do (no DDL, no logs).
    #[allow(dead_code)]
    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.type_changed.is_empty() && self.dropped.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Pure diff functions
// ---------------------------------------------------------------------------

/// Compute a `SchemaDiff` between the cached `SourceTableSchema` for `table`
/// and the `new_cols` coming from a `CdcRecord::SchemaChange`. Pure
/// function — no DB, no locking. Safe to call without a tx.
///
/// If the table is not in the cache at all, every `new_col` becomes an
/// `Added` entry (cold-start case — should normally not occur after setup).
///
/// MUST diff by column NAME, not position — pgoutput Relation messages
/// carry all columns in source order, which can change independently of
/// the column set.
///
/// # Type-change detection
///
/// Compares both the generic `DataType` (always populated) AND the source
/// PG OID (when both sides have one and the cached side is non-zero). The
/// OID check catches NUMERIC precision/scale changes that map to the same
/// coarse `DataType::Decimal` bucket.
///
/// # Ordinal contract
///
/// `added[i].ordinal` is `cached_schema.columns.len() + (i-th additive column)`
/// — absolute against the cache passed in, not against any persistent
/// tracking table. Callers that diff multiple `SchemaChange` events for
/// the same table within a single batch MUST fold each diff back into the
/// working cache BEFORE calling this for the next event, so ordinals stay
/// monotonic. `compute_schema_evolution_plan` does this automatically.
pub fn diff_against_cache(
    cache: &HashMap<String, SourceTableSchema>,
    table: &TableRef,
    new_cols: &[ColumnDef],
) -> SchemaDiff {
    let src_schema = table.schema.as_deref().unwrap_or("public");
    let qn = format!("{}.{}", src_schema, table.name);

    let mut diff = SchemaDiff::default();

    let mut new_by_name: HashMap<&str, &ColumnDef> = HashMap::with_capacity(new_cols.len());
    for col in new_cols {
        new_by_name.insert(col.name.as_str(), col);
    }

    match cache.get(&qn) {
        None => {
            for (i, col) in new_cols.iter().enumerate() {
                diff.added.push(AddedColumn {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    nullable: col.nullable,
                    #[allow(clippy::cast_possible_wrap)]
                    ordinal: i as i32,
                    pg_type_id: col.pg_type_id,
                });
            }
        }
        Some(cached_schema) => {
            let cached_by_name: HashMap<&str, &SourceColumn> = cached_schema
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c))
                .collect();

            #[allow(clippy::cast_possible_wrap)]
            let mut next_ordinal: i32 = cached_schema.columns.len() as i32;

            for col in new_cols {
                match cached_by_name.get(col.name.as_str()) {
                    None => {
                        diff.added.push(AddedColumn {
                            name: col.name.clone(),
                            data_type: col.data_type.clone(),
                            nullable: col.nullable,
                            ordinal: next_ordinal,
                            pg_type_id: col.pg_type_id,
                        });
                        next_ordinal += 1;
                    }
                    Some(cached_col) => {
                        // OID check catches PG NUMERIC precision changes that
                        // map to the same coarse DataType::Decimal bucket.
                        let dt_changed = cached_col.data_type != col.data_type;
                        let oid_changed = match (cached_col.pg_type_id, col.pg_type_id) {
                            (cached, Some(new)) if cached != 0 => cached != new,
                            _ => false,
                        };
                        if dt_changed || oid_changed {
                            diff.type_changed.push(TypeChange {
                                name: col.name.clone(),
                                old_data_type: cached_col.data_type.clone(),
                                new_data_type: col.data_type.clone(),
                                old_pg_type_id: if cached_col.pg_type_id == 0 {
                                    None
                                } else {
                                    Some(cached_col.pg_type_id)
                                },
                                new_pg_type_id: col.pg_type_id,
                            });
                        }
                    }
                }
            }

            for cached_col in &cached_schema.columns {
                if !new_by_name.contains_key(cached_col.name.as_str()) {
                    diff.dropped.push(cached_col.name.clone());
                }
            }
        }
    }

    diff
}

/// Pure pre-pass: clone the current schema snapshot and walk the batch's
/// `SchemaChange` records to compute the per-table schema diffs that need
/// DDL applied during the batch.
///
/// Returns `(working, pending_diffs)` where:
/// - `working` is the post-batch schema state — the snapshot with every
///   added column folded in. Sinks broadcast or persist this after a
///   successful commit (PG via `watch::send_replace`, others via in-memory
///   write-lock on `Arc<RwLock<...>>`).
/// - `pending_diffs` is the list of `(TableRef, SchemaDiff)` pairs that
///   actually have new columns and require ALTER TABLE during the batch.
///   Type-change-only and drop-only diffs are logged here and excluded
///   from this list (they need no DDL).
///
/// Pure function — no DB, no I/O, no locking. Safe to unit-test.
pub fn compute_schema_evolution_plan(
    snapshot: &HashMap<String, SourceTableSchema>,
    records: &[CdcRecord],
) -> (
    HashMap<String, SourceTableSchema>,
    Vec<(TableRef, SchemaDiff)>,
) {
    let mut working: HashMap<String, SourceTableSchema> = snapshot.clone();
    let mut pending_diffs: Vec<(TableRef, SchemaDiff)> = Vec::new();

    for record in records {
        if let CdcRecord::SchemaChange { table, columns, .. } = record {
            let diff = diff_against_cache(&working, table, columns);

            for tc in &diff.type_changed {
                error!(
                    table = %table.qualified_name(),
                    column = %tc.name,
                    old_type = ?tc.old_data_type,
                    new_type = ?tc.new_data_type,
                    "schema evolution: type change not supported, MERGE keeps old cast"
                );
            }

            for dropped in &diff.dropped {
                warn!(
                    table = %table.qualified_name(),
                    column = %dropped,
                    "schema evolution: column drop ignored, target keeps dead column"
                );
            }

            if !diff.added.is_empty() {
                let src_schema = table.schema.clone().unwrap_or_else(|| "public".to_string());
                let qn = table.qualified_name();

                let entry = working.entry(qn).or_insert_with(|| SourceTableSchema {
                    schema: src_schema,
                    name: table.name.clone(),
                    columns: Vec::new(),
                    primary_keys: Vec::new(),
                });

                // Fold added columns into `working` so a second SchemaChange
                // for the same table in this batch doesn't re-emit them.
                for added in &diff.added {
                    entry.columns.push(SourceColumn {
                        name: added.name.clone(),
                        data_type: added.data_type.clone(),
                        nullable: added.nullable,
                        pg_type_id: added.pg_type_id.unwrap_or(0),
                    });
                }

                pending_diffs.push((table.clone(), diff));
            }
        }
    }

    (working, pending_diffs)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::position::SourcePosition;
    use crate::core::record::{ColumnDef, TableRef};

    fn cache_with_orders_id() -> HashMap<String, SourceTableSchema> {
        let mut snapshot: HashMap<String, SourceTableSchema> = HashMap::new();
        snapshot.insert(
            "public.orders".to_string(),
            SourceTableSchema {
                schema: "public".to_string(),
                name: "orders".to_string(),
                columns: vec![SourceColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    pg_type_id: 23,
                }],
                primary_keys: vec!["id".to_string()],
            },
        );
        snapshot
    }

    #[test]
    fn no_schema_change_returns_empty_diffs() {
        let snapshot = cache_with_orders_id();

        let records = vec![CdcRecord::Insert {
            table: TableRef::new(Some("public".into()), "orders".into()),
            columns: vec![],
            position: SourcePosition::Lsn(0),
        }];

        let (working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);

        assert!(
            pending_diffs.is_empty(),
            "Insert-only batch must produce no pending diffs"
        );
        assert_eq!(
            working
                .get("public.orders")
                .expect("table must exist in working")
                .columns
                .len(),
            1,
            "working must equal input snapshot when no SchemaChange present"
        );
    }

    #[test]
    fn cold_start_added_column() {
        let snapshot: HashMap<String, SourceTableSchema> = HashMap::new();

        let records = vec![CdcRecord::SchemaChange {
            table: TableRef::new(Some("public".into()), "orders".into()),
            columns: vec![
                ColumnDef::with_pg_oid("id".into(), DataType::Int32, false, 23),
                ColumnDef::with_pg_oid("description".into(), DataType::String, true, 25),
            ],
            position: SourcePosition::Lsn(100),
        }];

        let (working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);

        assert_eq!(
            pending_diffs.len(),
            1,
            "one SchemaChange for a new table must produce exactly one pending diff"
        );
        assert_eq!(
            pending_diffs[0].1.added.len(),
            2,
            "both columns are new (cold-start) — both must appear in added"
        );
        assert_eq!(
            working
                .get("public.orders")
                .expect("public.orders must be in working after cold-start SchemaChange")
                .columns
                .len(),
            2,
            "working must contain both columns after cold-start SchemaChange"
        );
    }

    #[test]
    fn intra_batch_dedup() {
        // Snapshot has public.orders with one column — id.
        let snapshot = cache_with_orders_id();

        // Two SchemaChange records for the same table in the same batch,
        // both declaring the same `description` column. The second must see
        // `description` already in the working map and not re-emit.
        let schema_change = CdcRecord::SchemaChange {
            table: TableRef::new(Some("public".into()), "orders".into()),
            columns: vec![
                ColumnDef::with_pg_oid("id".into(), DataType::Int32, false, 23),
                ColumnDef::with_pg_oid("description".into(), DataType::String, true, 25),
            ],
            position: SourcePosition::Lsn(200),
        };
        let records = vec![schema_change.clone(), schema_change];

        let (_working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);

        assert_eq!(
            pending_diffs.len(),
            1,
            "two identical SchemaChange events in one batch must produce only one pending diff"
        );
        assert_eq!(
            pending_diffs[0].1.added.len(),
            1,
            "only `description` should be added — `id` was already in the snapshot"
        );
    }

    #[test]
    fn type_change_logged_no_ddl() {
        // Snapshot has id as Int32. Incoming claims id is Int64 — type changed.
        let snapshot = cache_with_orders_id();

        let records = vec![CdcRecord::SchemaChange {
            table: TableRef::new(Some("public".into()), "orders".into()),
            columns: vec![ColumnDef::with_pg_oid(
                "id".into(),
                DataType::Int64,
                false,
                20,
            )],
            position: SourcePosition::Lsn(300),
        }];

        let (_working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);

        assert!(
            pending_diffs.is_empty(),
            "type-change-only diff must NOT produce a pending diff (no DDL)"
        );
    }

    #[test]
    fn dropped_column_logged_no_ddl() {
        // Snapshot has orders(id, name). Incoming has only orders(id).
        // → name was dropped.
        let mut snapshot: HashMap<String, SourceTableSchema> = HashMap::new();
        snapshot.insert(
            "public.orders".to_string(),
            SourceTableSchema {
                schema: "public".to_string(),
                name: "orders".to_string(),
                columns: vec![
                    SourceColumn {
                        name: "id".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                        pg_type_id: 23,
                    },
                    SourceColumn {
                        name: "name".into(),
                        data_type: DataType::String,
                        nullable: true,
                        pg_type_id: 25,
                    },
                ],
                primary_keys: vec!["id".into()],
            },
        );

        let records = vec![CdcRecord::SchemaChange {
            table: TableRef::new(Some("public".into()), "orders".into()),
            columns: vec![ColumnDef::with_pg_oid(
                "id".into(),
                DataType::Int32,
                false,
                23,
            )],
            position: SourcePosition::Lsn(400),
        }];

        let (_working, pending_diffs) = compute_schema_evolution_plan(&snapshot, &records);

        assert!(
            pending_diffs.is_empty(),
            "drop-only diff must NOT produce a pending diff (no DDL)"
        );
    }

    #[test]
    fn diff_against_cache_cold_start() {
        let cache: HashMap<String, SourceTableSchema> = HashMap::new();
        let table = TableRef::new(Some("public".into()), "orders".into());
        let cols = vec![
            ColumnDef::with_pg_oid("id".into(), DataType::Int32, false, 23),
            ColumnDef::with_pg_oid("name".into(), DataType::String, true, 25),
        ];

        let diff = diff_against_cache(&cache, &table, &cols);

        assert_eq!(diff.added.len(), 2);
        assert!(diff.type_changed.is_empty());
        assert!(diff.dropped.is_empty());
        assert_eq!(diff.added[0].ordinal, 0);
        assert_eq!(diff.added[1].ordinal, 1);
    }

    #[test]
    fn diff_against_cache_no_pg_type_id_still_produces_added() {
        // Non-PG source: ColumnDef has pg_type_id = None.
        let cache: HashMap<String, SourceTableSchema> = HashMap::new();
        let table = TableRef::new(Some("public".into()), "orders".into());
        let cols = vec![ColumnDef {
            name: "id".into(),
            data_type: DataType::Int32,
            nullable: false,
            pg_type_id: None,
        }];

        let diff = diff_against_cache(&cache, &table, &cols);

        assert_eq!(
            diff.added.len(),
            1,
            "added entries are produced even without pg_type_id"
        );
        assert!(diff.added[0].pg_type_id.is_none());
    }
}
