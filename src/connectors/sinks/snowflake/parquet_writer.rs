// Copyright 2025
// Licensed under the Elastic License v2.0

//! Parquet writer for Snowflake.

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Int32Array, Int64Array, StringBuilder, StringDictionaryBuilder};
use arrow::datatypes::{DataType as ArrowDataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

use super::types::TypeMapper;
use crate::core::CdcRecord;

/// Parquet schema for the Snowflake raw table.
fn raw_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("_timestamp", ArrowDataType::Int64, false),
        Field::new(
            "_dst_table",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8),
            ),
            false,
        ),
        Field::new("_data", ArrowDataType::Utf8, false),
        Field::new("_record_type", ArrowDataType::Int32, false),
        Field::new("_match_data", ArrowDataType::Utf8, true),
        Field::new("_batch_id", ArrowDataType::Int64, false),
        Field::new("_toast_columns", ArrowDataType::Utf8, true),
    ])
}

/// Converts CDC records to Parquet bytes for the raw table.
///
/// Filters out non-data records (Begin, Commit, Heartbeat, SchemaChange).
/// Returns `(parquet_bytes, data_record_count)`.
pub fn records_to_parquet(records: &[CdcRecord], batch_id: i64) -> Result<(Vec<u8>, usize)> {
    let type_mapper = TypeMapper::new();

    // Pre-allocate builders
    let capacity = records.len();
    let mut timestamps = Vec::with_capacity(capacity);
    let mut dst_tables: StringDictionaryBuilder<Int32Type> =
        StringDictionaryBuilder::with_capacity(capacity, 32, capacity * 20);
    let mut data_jsons = StringBuilder::with_capacity(capacity, capacity * 256);
    let mut record_types = Vec::with_capacity(capacity);
    let mut match_data_jsons = StringBuilder::with_capacity(capacity, capacity * 64);
    let mut batch_ids = Vec::with_capacity(capacity);
    let mut toast_columns = StringBuilder::with_capacity(capacity, capacity * 32);

    let mut data_count = 0usize;

    for record in records {
        match record {
            CdcRecord::Insert {
                table,
                columns,
                position,
            } => {
                let ts = position_to_timestamp(position);
                let data_json = columns_to_json_string(columns, &type_mapper);

                timestamps.push(ts);
                dst_tables.append_value(table.qualified_name());
                data_jsons.append_value(&data_json);
                record_types.push(0i32);
                match_data_jsons.append_null();
                batch_ids.push(batch_id);
                toast_columns.append_null();
                data_count += 1;
            }
            CdcRecord::Update {
                table,
                old_columns,
                new_columns,
                position,
            } => {
                let ts = position_to_timestamp(position);

                // Track TOAST (unchanged) columns
                let mut toast_cols: Vec<&str> = Vec::new();
                let data_json =
                    columns_to_json_string_with_toast(new_columns, &type_mapper, &mut toast_cols);

                let match_json = old_columns
                    .as_ref()
                    .map(|cols| columns_to_json_string(cols, &type_mapper));

                timestamps.push(ts);
                dst_tables.append_value(table.qualified_name());
                data_jsons.append_value(&data_json);
                record_types.push(1i32);
                match match_json {
                    Some(ref mj) => match_data_jsons.append_value(mj),
                    None => match_data_jsons.append_null(),
                }
                batch_ids.push(batch_id);
                if toast_cols.is_empty() {
                    toast_columns.append_value("");
                } else {
                    toast_columns.append_value(toast_cols.join(","));
                }
                data_count += 1;
            }
            CdcRecord::Delete {
                table,
                columns,
                position,
            } => {
                let ts = position_to_timestamp(position);
                let data_json = columns_to_json_string(columns, &type_mapper);

                timestamps.push(ts);
                dst_tables.append_value(table.qualified_name());
                data_jsons.append_value(&data_json);
                record_types.push(2i32);
                match_data_jsons.append_value(&data_json); // match_data = same columns for delete
                batch_ids.push(batch_id);
                toast_columns.append_value("");
                data_count += 1;
            }
            // Non-data records: skip
            CdcRecord::SchemaChange { .. }
            | CdcRecord::Begin { .. }
            | CdcRecord::Commit { .. }
            | CdcRecord::Heartbeat { .. } => {}
        }
    }

    if data_count == 0 {
        return Ok((Vec::new(), 0));
    }

    // Build Arrow arrays
    let schema = Arc::new(raw_table_schema());

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(timestamps)),
        Arc::new(dst_tables.finish()),
        Arc::new(data_jsons.finish()),
        Arc::new(Int32Array::from(record_types)),
        Arc::new(match_data_jsons.finish()),
        Arc::new(Int64Array::from(batch_ids)),
        Arc::new(toast_columns.finish()),
    ];

    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .context("Failed to create Arrow RecordBatch")?;

    // Write to Parquet in memory
    let mut buf = Vec::with_capacity(data_count * 256);
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema, None).context("Failed to create Parquet writer")?;

    writer
        .write(&batch)
        .context("Failed to write RecordBatch to Parquet")?;

    writer.close().context("Failed to close Parquet writer")?;

    Ok((buf, data_count))
}

/// Extracts a timestamp (nanos) from a SourcePosition.
fn position_to_timestamp(position: &crate::core::SourcePosition) -> i64 {
    match position {
        crate::core::SourcePosition::Lsn(lsn) => *lsn as i64,
        crate::core::SourcePosition::Offset(offset) => *offset,
        _ => chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
    }
}

/// Serializes columns to a JSON string.
fn columns_to_json_string(
    columns: &[crate::core::ColumnValue],
    type_mapper: &TypeMapper,
) -> String {
    let mut obj = serde_json::Map::with_capacity(columns.len());
    for col in columns {
        if col.value.is_unchanged() {
            continue; // Skip TOAST unchanged in data JSON
        }
        let json_value = type_mapper.value_to_json(&col.value);
        obj.insert(col.name.clone(), json_value);
    }
    serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_else(|_| "{}".to_string())
}

/// Serializes columns to JSON, tracking TOAST (unchanged) column names.
fn columns_to_json_string_with_toast<'a>(
    columns: &'a [crate::core::ColumnValue],
    type_mapper: &TypeMapper,
    toast_cols: &mut Vec<&'a str>,
) -> String {
    let mut obj = serde_json::Map::with_capacity(columns.len());
    for col in columns {
        if col.value.is_unchanged() {
            toast_cols.push(&col.name);
            continue;
        }
        let json_value = type_mapper.value_to_json(&col.value);
        obj.insert(col.name.clone(), json_value);
    }
    serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::TableRef;
    use crate::core::{ColumnValue, SourcePosition, Value};

    fn make_insert(table: &str, id: i64, name: &str, lsn: u64) -> CdcRecord {
        CdcRecord::Insert {
            table: TableRef {
                schema: Some("public".to_string()),
                name: table.to_string(),
            },
            columns: vec![
                ColumnValue {
                    name: "id".to_string(),
                    value: Value::Int64(id),
                },
                ColumnValue {
                    name: "name".to_string(),
                    value: Value::String(name.to_string()),
                },
            ],
            position: SourcePosition::Lsn(lsn),
        }
    }

    fn make_update_with_toast(table: &str, id: i64, lsn: u64) -> CdcRecord {
        CdcRecord::Update {
            table: TableRef {
                schema: Some("public".to_string()),
                name: table.to_string(),
            },
            old_columns: Some(vec![ColumnValue {
                name: "id".to_string(),
                value: Value::Int64(id),
            }]),
            new_columns: vec![
                ColumnValue {
                    name: "id".to_string(),
                    value: Value::Int64(id),
                },
                ColumnValue {
                    name: "name".to_string(),
                    value: Value::String("updated".to_string()),
                },
                ColumnValue {
                    name: "big_data".to_string(),
                    value: Value::Unchanged,
                },
            ],
            position: SourcePosition::Lsn(lsn),
        }
    }

    fn make_delete(table: &str, id: i64, lsn: u64) -> CdcRecord {
        CdcRecord::Delete {
            table: TableRef {
                schema: Some("public".to_string()),
                name: table.to_string(),
            },
            columns: vec![ColumnValue {
                name: "id".to_string(),
                value: Value::Int64(id),
            }],
            position: SourcePosition::Lsn(lsn),
        }
    }

    #[test]
    fn test_empty_records() {
        let records = vec![
            CdcRecord::Begin { xid: 1 },
            CdcRecord::Commit {
                xid: 1,
                position: SourcePosition::Lsn(100),
                commit_timestamp_us: 0,
            },
        ];
        let (bytes, count) = records_to_parquet(&records, 1).unwrap();
        assert_eq!(count, 0);
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_insert_records() {
        let records = vec![
            make_insert("orders", 1, "order_a", 100),
            make_insert("orders", 2, "order_b", 101),
            make_insert("items", 10, "item_x", 102),
        ];

        let (bytes, count) = records_to_parquet(&records, 42).unwrap();
        assert_eq!(count, 3);
        assert!(!bytes.is_empty());

        // Verify it's valid Parquet by reading it back
        let cursor = bytes::Bytes::from(bytes);
        use parquet::file::reader::FileReader;
        let reader = parquet::file::reader::SerializedFileReader::new(cursor).unwrap();
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.row_group(0).num_rows(), 3);
    }

    #[test]
    fn test_mixed_operations() {
        let records = vec![
            CdcRecord::Begin { xid: 1 },
            make_insert("orders", 1, "a", 100),
            make_update_with_toast("orders", 1, 101),
            make_delete("orders", 2, 102),
            CdcRecord::Commit {
                xid: 1,
                position: SourcePosition::Lsn(103),
                commit_timestamp_us: 0,
            },
        ];

        let (bytes, count) = records_to_parquet(&records, 5).unwrap();
        assert_eq!(count, 3); // 1 insert + 1 update + 1 delete
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_toast_tracking() {
        let type_mapper = TypeMapper::new();
        let columns = vec![
            ColumnValue {
                name: "id".to_string(),
                value: Value::Int64(1),
            },
            ColumnValue {
                name: "big_col".to_string(),
                value: Value::Unchanged,
            },
            ColumnValue {
                name: "another_big".to_string(),
                value: Value::Unchanged,
            },
        ];

        let mut toast = Vec::new();
        let json = columns_to_json_string_with_toast(&columns, &type_mapper, &mut toast);

        assert_eq!(toast, vec!["big_col", "another_big"]);
        // JSON should only have "id"
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("id").is_some());
        assert!(parsed.get("big_col").is_none());
        assert!(parsed.get("another_big").is_none());
    }
}
