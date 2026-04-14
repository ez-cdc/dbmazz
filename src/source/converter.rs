// Copyright 2025
// Licensed under the Elastic License v2.0

//! Converts pgoutput CdcMessage types into generic CdcRecord types.
//!
//! This module is the boundary between PostgreSQL-specific replication data
//! and the generic CDC pipeline. Everything downstream of this converter
//! (Pipeline, Sink) only sees CdcRecord — never CdcMessage.

use crate::core::position::SourcePosition;
use crate::core::record::{CdcRecord, ColumnDef, ColumnValue, DataType, TableRef, Value};
use crate::pipeline::schema_cache::{SchemaCache, TableSchema};
use crate::source::parser::{CdcMessage, TupleData};
use crate::utils::{normalize_timestamptz, parse_pg_array, strip_money_symbol};

/// Convert a CdcMessage (pgoutput) to a CdcRecord (generic).
/// Returns None for messages that don't produce records (Unknown, LogicalMessage).
pub fn convert_message(
    msg: &CdcMessage,
    schema_cache: &SchemaCache,
    lsn: u64,
) -> Option<CdcRecord> {
    let position = SourcePosition::Lsn(lsn);

    match msg {
        CdcMessage::Begin { xid, .. } => Some(CdcRecord::Begin { xid: *xid as u64 }),

        CdcMessage::Commit {
            end_lsn, timestamp, ..
        } => Some(CdcRecord::Commit {
            xid: 0,
            position: SourcePosition::Lsn(*end_lsn),
            commit_timestamp_us: *timestamp,
        }),

        CdcMessage::Relation {
            namespace,
            name,
            columns,
            ..
        } => {
            let column_defs = columns
                .iter()
                .map(|c| ColumnDef::new(c.name.clone(), pg_type_to_data_type(c.type_id), true))
                .collect();

            Some(CdcRecord::SchemaChange {
                table: TableRef::new(Some(namespace.clone()), name.clone()),
                columns: column_defs,
                position,
            })
        }

        CdcMessage::Insert { relation_id, tuple } => {
            let schema = schema_cache.get(*relation_id)?;
            let columns = tuple_to_column_values(tuple, schema);

            Some(CdcRecord::Insert {
                table: TableRef::new(Some(schema.namespace.clone()), schema.name.clone()),
                columns,
                position,
            })
        }

        CdcMessage::Update {
            relation_id,
            old_tuple,
            new_tuple,
        } => {
            let schema = schema_cache.get(*relation_id)?;
            let new_columns = tuple_to_column_values(new_tuple, schema);
            let old_columns = old_tuple
                .as_ref()
                .map(|t| tuple_to_column_values(t, schema));

            Some(CdcRecord::Update {
                table: TableRef::new(Some(schema.namespace.clone()), schema.name.clone()),
                old_columns,
                new_columns,
                position,
            })
        }

        CdcMessage::Delete {
            relation_id,
            old_tuple,
        } => {
            let schema = schema_cache.get(*relation_id)?;
            let old = old_tuple.as_ref()?;
            let columns = tuple_to_column_values(old, schema);

            Some(CdcRecord::Delete {
                table: TableRef::new(Some(schema.namespace.clone()), schema.name.clone()),
                columns,
                position,
            })
        }

        CdcMessage::KeepAlive { wal_end, .. } => Some(CdcRecord::Heartbeat {
            position: SourcePosition::Lsn(*wal_end),
        }),

        CdcMessage::Unknown | CdcMessage::LogicalMessage { .. } => None,
    }
}

/// Convert a Tuple to Vec<ColumnValue> using schema info
fn tuple_to_column_values(
    tuple: &crate::source::parser::Tuple,
    schema: &TableSchema,
) -> Vec<ColumnValue> {
    schema
        .columns
        .iter()
        .zip(tuple.cols.iter())
        .map(|(col, data)| {
            let value = match data {
                TupleData::Null => Value::Null,
                TupleData::Toast => Value::Unchanged,
                TupleData::Text(bytes) => {
                    let text = String::from_utf8_lossy(bytes);
                    convert_pg_value(&text, col.type_id)
                }
            };
            ColumnValue::new(col.name.clone(), value)
        })
        .collect()
}

/// Convert a PostgreSQL text value to a generic Value based on type OID
fn convert_pg_value(text: &str, pg_type_id: u32) -> Value {
    match pg_type_id {
        // Boolean
        16 => {
            let is_true = matches!(text.to_lowercase().as_str(), "t" | "true" | "1");
            Value::Bool(is_true)
        }
        // Integer types (INT2, INT4, INT8)
        21 | 23 | 20 => text
            .parse::<i64>()
            .map(Value::Int64)
            .unwrap_or_else(|_| Value::String(text.to_string())),
        // Float types (FLOAT4, FLOAT8)
        700 | 701 => text
            .parse::<f64>()
            .map(Value::Float64)
            .unwrap_or_else(|_| Value::String(text.to_string())),
        // Money - strip currency symbol
        790 => Value::Decimal(strip_money_symbol(text)),
        // NUMERIC/DECIMAL - keep as string for precision
        1700 => Value::Decimal(text.to_string()),
        // Timestamp (no TZ) - keep as-is
        1114 => Value::String(text.to_string()),
        // TimestampTZ - normalize to UTC
        1184 => Value::String(normalize_timestamptz(text)),
        // JSON/JSONB
        114 | 3802 => Value::Json(text.to_string()),
        // UUID
        2950 => Value::Uuid(text.to_string()),
        // Integer arrays
        1005 | 1007 | 1016 => Value::Json(parse_pg_array(text, "int")),
        // Float arrays
        1021 | 1022 => Value::Json(parse_pg_array(text, "float")),
        // Text/varchar arrays
        1009 | 1015 => Value::Json(parse_pg_array(text, "text")),
        // Default: string
        _ => Value::String(text.to_string()),
    }
}

/// Convert PostgreSQL type OID to generic DataType
pub fn pg_type_to_data_type(pg_type_id: u32) -> DataType {
    match pg_type_id {
        16 => DataType::Boolean,
        21 => DataType::Int16,
        23 => DataType::Int32,
        20 => DataType::Int64,
        700 => DataType::Float32,
        701 => DataType::Float64,
        1700 => DataType::Decimal {
            precision: 38,
            scale: 9,
        },
        1114 => DataType::Timestamp,
        1184 => DataType::TimestampTz,
        25 | 1043 | 1042 => DataType::String,
        114 | 3802 => DataType::Jsonb,
        2950 => DataType::Uuid,
        17 => DataType::Bytes,
        _ => DataType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_to_data_type() {
        assert_eq!(pg_type_to_data_type(16), DataType::Boolean);
        assert_eq!(pg_type_to_data_type(21), DataType::Int16);
        assert_eq!(pg_type_to_data_type(23), DataType::Int32);
        assert_eq!(pg_type_to_data_type(20), DataType::Int64);
        assert_eq!(pg_type_to_data_type(700), DataType::Float32);
        assert_eq!(pg_type_to_data_type(701), DataType::Float64);
        assert_eq!(pg_type_to_data_type(1114), DataType::Timestamp);
        assert_eq!(pg_type_to_data_type(25), DataType::String);
        assert_eq!(pg_type_to_data_type(3802), DataType::Jsonb);
        assert_eq!(pg_type_to_data_type(2950), DataType::Uuid);
        // Unknown type defaults to String
        assert_eq!(pg_type_to_data_type(99999), DataType::String);
    }

    #[test]
    fn test_convert_pg_value_bool() {
        assert!(matches!(convert_pg_value("t", 16), Value::Bool(true)));
        assert!(matches!(convert_pg_value("true", 16), Value::Bool(true)));
        assert!(matches!(convert_pg_value("1", 16), Value::Bool(true)));
        assert!(matches!(convert_pg_value("f", 16), Value::Bool(false)));
        assert!(matches!(convert_pg_value("false", 16), Value::Bool(false)));
    }

    #[test]
    fn test_convert_pg_value_int() {
        assert!(matches!(convert_pg_value("42", 23), Value::Int64(42)));
        assert!(matches!(convert_pg_value("-100", 20), Value::Int64(-100)));
        // Invalid int falls back to string
        assert!(matches!(
            convert_pg_value("not_a_number", 23),
            Value::String(_)
        ));
    }

    #[test]
    fn test_convert_pg_value_float() {
        if let Value::Float64(f) = convert_pg_value("3.5", 701) {
            assert!((f - 3.5).abs() < 0.001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_convert_pg_value_json() {
        if let Value::Json(s) = convert_pg_value(r#"{"key": "value"}"#, 3802) {
            assert_eq!(s, r#"{"key": "value"}"#);
        } else {
            panic!("Expected Json");
        }
    }

    #[test]
    fn test_convert_pg_value_uuid() {
        if let Value::Uuid(s) = convert_pg_value("550e8400-e29b-41d4-a716-446655440000", 2950) {
            assert_eq!(s, "550e8400-e29b-41d4-a716-446655440000");
        } else {
            panic!("Expected Uuid");
        }
    }

    #[test]
    fn test_convert_pg_value_decimal() {
        if let Value::Decimal(s) = convert_pg_value("123.456789", 1700) {
            assert_eq!(s, "123.456789");
        } else {
            panic!("Expected Decimal");
        }
    }

    #[test]
    fn test_convert_pg_value_default_string() {
        // Unknown type OID → String
        if let Value::String(s) = convert_pg_value("hello", 99999) {
            assert_eq!(s, "hello");
        } else {
            panic!("Expected String");
        }
    }
}
