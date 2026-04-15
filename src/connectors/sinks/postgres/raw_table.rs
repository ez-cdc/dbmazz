// Copyright 2025
// Licensed under the Elastic License v2.0

//! Raw table writer: serializes CdcRecords and writes to the staging table via COPY.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use futures_util::SinkExt;
use tokio::sync::watch;
use tokio_postgres::Client;
use tracing::{error, info, warn};

use super::schema_tracking::{self, SchemaState};
use super::types::pg_oid_to_target_type;
use crate::core::record::{CdcRecord, ColumnValue, TableRef, Value};
use crate::core::traits::{SourceColumn, SourceTableSchema};
use crate::source::converter::pg_type_to_data_type;

/// Metadata schema name
const METADATA_SCHEMA: &str = "_dbmazz";

/// Write a batch of CdcRecords to the raw table via COPY and update metadata.
/// Everything runs in a single transaction (atomic).
/// The batch_id is read from metadata and incremented atomically within the transaction
/// (safe for multiple sink instances writing concurrently, e.g., snapshot workers).
///
/// Schema evolution (ADD COLUMN) is handled before the COPY stream opens:
/// `CdcRecord::SchemaChange` events are pre-scanned in Phase 1 to compute diffs,
/// then Phase 2 runs DDL + COPY + metadata update atomically in one transaction.
/// After a successful commit, Phase 3 broadcasts the new schema state to the
/// normalizer via `schema_tx`.
///
/// Returns (records_written, bytes_written).
pub async fn write_batch_to_raw(
    client: &mut Client,
    raw_table: &str,
    target_schema: &str,
    job_name: &str,
    lsn: u64,
    records: &[CdcRecord],
    schema_tx: &watch::Sender<SchemaState>,
) -> Result<(usize, u64)> {
    // ─── Phase 1: pre-pass (pure diff, no DB) ─────────────────────────────────
    // schema_tx.borrow() yields the Arc<HashMap>; deref once to get &HashMap,
    // then clone the HashMap out of the Arc so compute_schema_evolution_plan
    // takes a plain &HashMap with no Arc lifetime dependency.
    let snapshot: HashMap<String, SourceTableSchema> = (**schema_tx.borrow()).clone();
    let (working, pending_diffs) = compute_schema_evolution_plan(&snapshot, records);

    // ─── Phase 2: single transaction (DDL → batch_id → COPY → lsn → COMMIT) ──

    let tx = client
        .transaction()
        .await
        .context("Failed to begin transaction")?;

    // 2a. DDL first — COPY stream must NOT open until all DDL for this batch
    //     is complete (Risk #6). A COPY failure rolls back the DDL too.
    for (table, diff) in &pending_diffs {
        for added in &diff.added {
            let sql = format!(
                r#"ALTER TABLE "{}"."{}" ADD COLUMN IF NOT EXISTS "{}" {}"#,
                target_schema,
                table.name,
                added.name,
                pg_oid_to_target_type(added.pg_type_id)
            );
            tx.batch_execute(&sql).await.with_context(|| {
                format!(
                    "ALTER TABLE failed for {}.{} column {}",
                    target_schema, table.name, added.name
                )
            })?;
        }
        schema_tracking::insert_tracked_columns(&tx, job_name, table, &diff.added, lsn)
            .await
            .with_context(|| {
                format!(
                    "Failed to insert tracking rows for {}.{}",
                    target_schema, table.name
                )
            })?;
        info!(
            table = %table.qualified_name(),
            added_count = diff.added.len(),
            "pg_sink: schema evolution applied"
        );
    }

    // 2b. Atomically read and increment sync_batch_id within the transaction.
    let row = tx
        .query_one(
            "UPDATE _dbmazz.\"_metadata\" SET sync_batch_id = sync_batch_id + 1 WHERE job_name = $1 RETURNING sync_batch_id",
            &[&job_name],
        )
        .await
        .context("Failed to increment sync_batch_id")?;
    let batch_id: i64 = row.get(0);

    // 2c. COPY INTO raw table.
    let copy_stmt = format!(
        "COPY {} (_timestamp, _dst_table, _data, _record_type, _match_data, _batch_id, _toast_columns) FROM STDIN",
        raw_table
    );
    let copy_sink = tx
        .copy_in(&copy_stmt)
        .await
        .with_context(|| format!("Failed to start COPY into {}", raw_table))?;

    let mut buf = BytesMut::with_capacity(records.len() * 256);
    let mut rows_written = 0usize;
    // Batch base timestamp. Each written record uses `batch_base + rows_written`
    // so the MERGE tiebreaker `ORDER BY _timestamp DESC` is deterministic for
    // multiple operations on the same PK inside a single batch (e.g. two
    // sequential UPDATEs on the same row in one transaction). Without the
    // per-row offset every record in the batch shared the same `_timestamp`
    // and ROW_NUMBER() picked an arbitrary survivor, losing the final write.
    let batch_base_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    for record in records {
        match record {
            CdcRecord::Insert { table, columns, .. } => {
                let data_json = columns_to_json(columns);
                write_copy_row(
                    &mut buf,
                    &RawRow {
                        timestamp: batch_base_nanos + rows_written as i64,
                        dst_table: &table.qualified_name(),
                        data_json: &data_json,
                        record_type: 0,
                        match_data: None,
                        batch_id,
                        toast_columns: "",
                    },
                );
                rows_written += 1;
            }
            CdcRecord::Update {
                table,
                old_columns,
                new_columns,
                ..
            } => {
                let toast_cols = extract_toast_columns(new_columns);
                let data_json = columns_to_json_skip_unchanged(new_columns);
                let match_json = old_columns.as_ref().map(|cols| columns_to_json(cols));
                write_copy_row(
                    &mut buf,
                    &RawRow {
                        timestamp: batch_base_nanos + rows_written as i64,
                        dst_table: &table.qualified_name(),
                        data_json: &data_json,
                        record_type: 1,
                        match_data: match_json.as_deref(),
                        batch_id,
                        toast_columns: &toast_cols,
                    },
                );
                rows_written += 1;
            }
            CdcRecord::Delete { table, columns, .. } => {
                let data_json = columns_to_json(columns);
                let match_json = columns_to_json(columns);
                write_copy_row(
                    &mut buf,
                    &RawRow {
                        timestamp: batch_base_nanos + rows_written as i64,
                        dst_table: &table.qualified_name(),
                        data_json: &data_json,
                        record_type: 2,
                        match_data: Some(&match_json),
                        batch_id,
                        toast_columns: "",
                    },
                );
                rows_written += 1;
            }
            // SchemaChange DDL was already applied in Phase 2a above.
            // Begin, Commit, Heartbeat are control records — not data rows.
            _ => continue,
        }
    }

    let bytes_written = buf.len() as u64;

    // Send COPY data.
    let mut copy_sink = std::pin::pin!(copy_sink);
    copy_sink
        .send(buf.freeze())
        .await
        .context("Failed to send COPY data")?;
    let _rows = copy_sink.finish().await.context("Failed to finish COPY")?;

    // 2d. Update lsn_offset (within same transaction, batch_id already incremented above).
    tx.execute(
        &format!(
            "UPDATE {}.\"_metadata\" SET lsn_offset = GREATEST(lsn_offset, $1) WHERE job_name = $2",
            METADATA_SCHEMA
        ),
        &[&(lsn as i64), &job_name],
    )
    .await
    .context("Failed to update lsn_offset")?;

    // 2e. COMMIT — atomic: DDL + raw data + metadata.
    tx.commit().await.context("Failed to commit transaction")?;

    // ─── Phase 3: post-commit schema broadcast ─────────────────────────────────
    //
    // `working` already reflects every successfully-applied SchemaChange in this
    // batch (built incrementally during Phase 1). Wrap it in Arc and publish so
    // the normalizer rebuilds its MERGE with the new column set.
    //
    // `send_replace` is infallible regardless of receiver count — the stored
    // value is always updated even if no receivers are listening (snapshot-worker
    // mode). The returned previous value is intentionally discarded.
    if !pending_diffs.is_empty() {
        let _ = schema_tx.send_replace(Arc::new(working));
    }

    Ok((rows_written, bytes_written))
}

/// Pure pre-pass: clone the current schema snapshot and walk the batch's
/// `SchemaChange` records to compute the per-table schema diffs that need
/// DDL applied during the transaction.
///
/// Returns `(working, pending_diffs)` where:
/// - `working` is the post-batch schema state — the snapshot with every
///   added column folded in. This is the value that Phase 3 broadcasts on
///   the watch channel after a successful commit.
/// - `pending_diffs` is the list of `(TableRef, SchemaDiff)` pairs that
///   actually have new columns and require ALTER TABLE during Phase 2a.
///   Type-change-only and drop-only diffs are logged here and excluded
///   from this list (they need no DDL).
///
/// Pure function — no DB, no I/O, no locking. Safe to unit-test.
fn compute_schema_evolution_plan(
    snapshot: &HashMap<String, SourceTableSchema>,
    records: &[CdcRecord],
) -> (
    HashMap<String, SourceTableSchema>,
    Vec<(TableRef, schema_tracking::SchemaDiff)>,
) {
    // Clone the snapshot into a mutable working map.
    // Each SchemaChange in the batch is diffed against `working` (not the
    // original snapshot), and `working` is mutated in-place after each diff so
    // that a second SchemaChange for the same table in the same batch sees the
    // already-added columns and does not re-emit them.
    //
    // After this loop, `working` reflects the schema state that will exist AFTER
    // the entire batch is committed. It is broadcast in Phase 3.
    let mut working: HashMap<String, SourceTableSchema> = snapshot.clone();

    // Only diffs with non-empty `added` need DDL work. Type-change-only and
    // drop-only diffs are logged here and are no-ops for the transaction.
    let mut pending_diffs: Vec<(TableRef, schema_tracking::SchemaDiff)> = Vec::new();

    for record in records {
        if let CdcRecord::SchemaChange { table, columns, .. } = record {
            let diff = schema_tracking::diff_against_cache(&working, table, columns);

            // Log type changes (no DDL — operator intervention required).
            for tc in &diff.type_changed {
                error!(
                    table = %table.qualified_name(),
                    column = %tc.name,
                    old_oid = tc.old_pg_type_id,
                    new_oid = tc.new_pg_type_id,
                    "pg_sink: type change not supported, MERGE keeps old cast"
                );
            }

            // Log drops (no DDL — target keeps dead column).
            for dropped in &diff.dropped {
                warn!(
                    table = %table.qualified_name(),
                    column = %dropped,
                    "pg_sink: column drop ignored, target keeps dead column"
                );
            }

            if !diff.added.is_empty() {
                let src_schema = table.schema.clone().unwrap_or_else(|| "public".to_string());
                let qn = table.qualified_name();

                // Ensure the table exists in `working` before pushing columns.
                let entry = working.entry(qn).or_insert_with(|| SourceTableSchema {
                    schema: src_schema,
                    name: table.name.clone(),
                    columns: Vec::new(),
                    primary_keys: Vec::new(),
                });

                // Advance working map so subsequent SchemaChange events for the
                // same table see these columns and don't re-emit them (Risk #7).
                for added in &diff.added {
                    entry.columns.push(SourceColumn {
                        name: added.name.clone(),
                        data_type: pg_type_to_data_type(added.pg_type_id),
                        nullable: added.nullable,
                        pg_type_id: added.pg_type_id,
                    });
                }

                pending_diffs.push((table.clone(), diff));
            }
        }
    }

    (working, pending_diffs)
}

/// Fields for a single raw table row.
struct RawRow<'a> {
    timestamp: i64,
    dst_table: &'a str,
    data_json: &'a str,
    record_type: i16,
    match_data: Option<&'a str>,
    batch_id: i64,
    toast_columns: &'a str,
}

/// Write one row to the COPY buffer in text format.
/// Columns are tab-separated, rows are newline-terminated.
fn write_copy_row(buf: &mut BytesMut, row: &RawRow<'_>) {
    // _timestamp
    buf.put_slice(row.timestamp.to_string().as_bytes());
    buf.put_u8(b'\t');

    // _dst_table
    buf.put_slice(row.dst_table.as_bytes());
    buf.put_u8(b'\t');

    // _data (JSONB) — escape backslashes for COPY text format
    write_copy_escaped(buf, row.data_json);
    buf.put_u8(b'\t');

    // _record_type
    buf.put_slice(row.record_type.to_string().as_bytes());
    buf.put_u8(b'\t');

    // _match_data (JSONB or NULL)
    match row.match_data {
        Some(json) => write_copy_escaped(buf, json),
        None => buf.put_slice(b"\\N"),
    }
    buf.put_u8(b'\t');

    // _batch_id
    buf.put_slice(row.batch_id.to_string().as_bytes());
    buf.put_u8(b'\t');

    // _toast_columns
    buf.put_slice(row.toast_columns.as_bytes());
    buf.put_u8(b'\n');
}

/// Write a string to the COPY buffer, escaping backslashes for COPY text format.
/// COPY interprets `\` as escape character, so all `\` in the data must be doubled.
#[inline]
fn write_copy_escaped(buf: &mut BytesMut, s: &str) {
    for byte in s.as_bytes() {
        if *byte == b'\\' {
            buf.put_slice(b"\\\\");
        } else {
            buf.put_u8(*byte);
        }
    }
}

/// Serialize column values to a JSON object string.
fn columns_to_json(columns: &[ColumnValue]) -> String {
    let mut obj = serde_json::Map::new();
    for col in columns {
        let json_val = value_to_json(&col.value);
        obj.insert(col.name.clone(), json_val);
    }
    serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_else(|_| "{}".to_string())
}

/// Serialize column values to JSON, skipping Unchanged (TOAST) columns.
fn columns_to_json_skip_unchanged(columns: &[ColumnValue]) -> String {
    let mut obj = serde_json::Map::new();
    for col in columns {
        if col.value.is_unchanged() {
            continue;
        }
        let json_val = value_to_json(&col.value);
        obj.insert(col.name.clone(), json_val);
    }
    serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_else(|_| "{}".to_string())
}

/// Extract names of Unchanged (TOAST) columns as comma-separated string.
fn extract_toast_columns(columns: &[ColumnValue]) -> String {
    columns
        .iter()
        .filter(|c| c.value.is_unchanged())
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

/// Convert a Value to a serde_json::Value.
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::json!(*b),
        Value::Int64(n) => serde_json::json!(*n),
        Value::Float64(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::json!(s),
        Value::Bytes(b) => {
            // Hex-encode bytes (PostgreSQL can decode with decode(val, 'hex'))
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            serde_json::json!(hex)
        }
        Value::Json(s) => {
            // Parse the JSON string to embed it as a JSON value (not a string)
            serde_json::from_str(s).unwrap_or_else(|_| serde_json::json!(s))
        }
        Value::Timestamp(ts) => serde_json::json!(*ts),
        Value::Decimal(s) => serde_json::json!(s),
        Value::Uuid(s) => serde_json::json!(s),
        Value::Unchanged => serde_json::Value::Null, // should not reach here
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::position::SourcePosition;
    use crate::core::record::{ColumnDef, ColumnValue, DataType};

    #[test]
    fn test_columns_to_json() {
        let cols = vec![
            ColumnValue::new("id".to_string(), Value::Int64(42)),
            ColumnValue::new("name".to_string(), Value::String("test".to_string())),
            ColumnValue::new("active".to_string(), Value::Bool(true)),
        ];
        let json = columns_to_json(&cols);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], 42);
        assert_eq!(parsed["name"], "test");
        assert_eq!(parsed["active"], true);
    }

    #[test]
    fn test_columns_to_json_with_null() {
        let cols = vec![
            ColumnValue::new("id".to_string(), Value::Int64(1)),
            ColumnValue::new("deleted_at".to_string(), Value::Null),
        ];
        let json = columns_to_json(&cols);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], 1);
        assert!(parsed["deleted_at"].is_null());
    }

    #[test]
    fn test_columns_to_json_skip_unchanged() {
        let cols = vec![
            ColumnValue::new("id".to_string(), Value::Int64(1)),
            ColumnValue::new("name".to_string(), Value::String("new".to_string())),
            ColumnValue::new("big_text".to_string(), Value::Unchanged),
        ];
        let json = columns_to_json_skip_unchanged(&cols);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["name"], "new");
        assert!(parsed.get("big_text").is_none());
    }

    #[test]
    fn test_extract_toast_columns() {
        let cols = vec![
            ColumnValue::new("id".to_string(), Value::Int64(1)),
            ColumnValue::new("big_text".to_string(), Value::Unchanged),
            ColumnValue::new("another_big".to_string(), Value::Unchanged),
        ];
        assert_eq!(extract_toast_columns(&cols), "big_text,another_big");
    }

    #[test]
    fn test_extract_toast_columns_none() {
        let cols = vec![
            ColumnValue::new("id".to_string(), Value::Int64(1)),
            ColumnValue::new("name".to_string(), Value::String("test".to_string())),
        ];
        assert_eq!(extract_toast_columns(&cols), "");
    }

    #[test]
    fn test_write_copy_escaped() {
        let mut buf = BytesMut::new();
        write_copy_escaped(&mut buf, r#"{"path":"C:\\Users"}"#);
        // Backslashes should be doubled
        assert_eq!(
            std::str::from_utf8(&buf).unwrap(),
            r#"{"path":"C:\\\\Users"}"#
        );
    }

    #[test]
    fn test_write_copy_escaped_no_backslash() {
        let mut buf = BytesMut::new();
        write_copy_escaped(&mut buf, r#"{"key":"value"}"#);
        assert_eq!(std::str::from_utf8(&buf).unwrap(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_value_to_json_types() {
        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(value_to_json(&Value::Bool(true)), serde_json::json!(true));
        assert_eq!(value_to_json(&Value::Int64(42)), serde_json::json!(42));
        assert_eq!(
            value_to_json(&Value::String("hi".into())),
            serde_json::json!("hi")
        );
        assert_eq!(
            value_to_json(&Value::Decimal("123.45".into())),
            serde_json::json!("123.45")
        );
        assert_eq!(
            value_to_json(&Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into())),
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn test_value_to_json_nested_json() {
        let json_str = r#"{"nested": [1, 2, 3]}"#;
        let result = value_to_json(&Value::Json(json_str.to_string()));
        // Should be embedded as JSON object, not as a string
        assert!(result.is_object());
        assert!(result["nested"].is_array());
    }

    // ------------------------------------------------------------------
    // compute_schema_evolution_plan tests
    // ------------------------------------------------------------------

    #[test]
    fn test_compute_schema_evolution_plan_no_schema_change() {
        // Snapshot contains one table.
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

        // Batch contains only an Insert — no SchemaChange.
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
        // working must be identical to the input snapshot.
        assert_eq!(
            working
                .get("public.orders")
                .expect("table must exist in working")
                .columns
                .len(),
            1,
            "working schema must equal the input snapshot when no SchemaChange is present"
        );
    }

    #[test]
    fn test_compute_schema_evolution_plan_cold_start_added_column() {
        // Cold start: empty snapshot — table has never been seen.
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
    fn test_compute_schema_evolution_plan_intra_batch_dedup() {
        // Snapshot has public.orders with one column — id.
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

        // Two SchemaChange records for the same table in the same batch,
        // both declaring the same `description` column.
        // The second SchemaChange must see `description` already in the
        // working map and produce an empty diff (Risk #7 invariant).
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
            "two identical SchemaChange events in the same batch must produce only one pending diff (Risk #7)"
        );
        assert_eq!(
            pending_diffs[0].1.added.len(),
            1,
            "only `description` should be in the added set — `id` was already in the snapshot"
        );
    }
}
