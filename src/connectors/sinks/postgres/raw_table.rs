// Copyright 2025
// Licensed under the Elastic License v2.0

//! Raw table writer: serializes CdcRecords and writes to the staging table via COPY.

use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use futures::SinkExt;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::Client;

use crate::core::record::{CdcRecord, ColumnValue, Value};

/// Metadata schema name
const METADATA_SCHEMA: &str = "_dbmazz";

/// Write a batch of CdcRecords to the raw table via COPY and update metadata.
/// Everything runs in a single transaction (atomic).
/// The batch_id is read from metadata and incremented atomically within the transaction
/// (safe for multiple sink instances writing concurrently, e.g., snapshot workers).
///
/// Returns (records_written, bytes_written).
pub async fn write_batch_to_raw(
    client: &mut Client,
    raw_table: &str,
    job_name: &str,
    lsn: u64,
    records: &[CdcRecord],
) -> Result<(usize, u64)> {
    let tx = client
        .transaction()
        .await
        .context("Failed to begin transaction")?;

    // Atomically read and increment sync_batch_id within the transaction
    let row = tx
        .query_one(
            "UPDATE _dbmazz.\"_metadata\" SET sync_batch_id = sync_batch_id + 1 WHERE job_name = $1 RETURNING sync_batch_id",
            &[&job_name],
        )
        .await
        .context("Failed to increment sync_batch_id")?;
    let batch_id: i64 = row.get(0);

    // COPY INTO raw table
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
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;

    for record in records {
        match record {
            CdcRecord::Insert {
                table, columns, ..
            } => {
                let data_json = columns_to_json(columns);
                write_copy_row(&mut buf, &RawRow {
                    timestamp: now_nanos,
                    dst_table: &table.qualified_name(),
                    data_json: &data_json,
                    record_type: 0,
                    match_data: None,
                    batch_id,
                    toast_columns: "",
                });
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
                write_copy_row(&mut buf, &RawRow {
                    timestamp: now_nanos,
                    dst_table: &table.qualified_name(),
                    data_json: &data_json,
                    record_type: 1,
                    match_data: match_json.as_deref(),
                    batch_id,
                    toast_columns: &toast_cols,
                });
                rows_written += 1;
            }
            CdcRecord::Delete {
                table, columns, ..
            } => {
                let data_json = columns_to_json(columns);
                let match_json = columns_to_json(columns);
                write_copy_row(&mut buf, &RawRow {
                    timestamp: now_nanos,
                    dst_table: &table.qualified_name(),
                    data_json: &data_json,
                    record_type: 2,
                    match_data: Some(&match_json),
                    batch_id,
                    toast_columns: "",
                });
                rows_written += 1;
            }
            // Skip Begin, Commit, Heartbeat, SchemaChange — not data records
            _ => continue,
        }
    }

    let bytes_written = buf.len() as u64;

    // Send COPY data
    let mut copy_sink = std::pin::pin!(copy_sink);
    copy_sink
        .send(buf.freeze())
        .await
        .context("Failed to send COPY data")?;
    let _rows = copy_sink
        .finish()
        .await
        .context("Failed to finish COPY")?;

    // Update lsn_offset (within same transaction, batch_id already incremented above)
    tx.execute(
        &format!(
            "UPDATE {}.\"_metadata\" SET lsn_offset = GREATEST(lsn_offset, $1) WHERE job_name = $2",
            METADATA_SCHEMA
        ),
        &[&(lsn as i64), &job_name],
    )
    .await
    .context("Failed to update lsn_offset")?;

    // COMMIT — atomic: raw data + metadata
    tx.commit().await.context("Failed to commit transaction")?;

    Ok((rows_written, bytes_written))
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
    use crate::core::record::ColumnValue;

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
        assert_eq!(
            std::str::from_utf8(&buf).unwrap(),
            r#"{"key":"value"}"#
        );
    }

    #[test]
    fn test_value_to_json_types() {
        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(value_to_json(&Value::Bool(true)), serde_json::json!(true));
        assert_eq!(value_to_json(&Value::Int64(42)), serde_json::json!(42));
        assert_eq!(value_to_json(&Value::String("hi".into())), serde_json::json!("hi"));
        assert_eq!(value_to_json(&Value::Decimal("123.45".into())), serde_json::json!("123.45"));
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
}
