// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle sink type mapping: core::DataType → Oracle SQL types.

use crate::core::record::{DataType, Value};

/// Map a source-agnostic DataType to Oracle column type DDL string.
pub fn data_type_to_oracle(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "NUMBER(1)",
        DataType::Int16 => "NUMBER(5)",
        DataType::Int32 => "NUMBER(10)",
        DataType::Int64 => "NUMBER(19)",
        DataType::Float32 => "BINARY_FLOAT",
        DataType::Float64 => "BINARY_DOUBLE",
        DataType::Decimal { .. } => "NUMBER",
        DataType::String | DataType::Text => "CLOB",
        DataType::Bytes => "BLOB",
        DataType::Json | DataType::Jsonb => "CLOB",
        DataType::Date => "DATE",
        DataType::Time => "INTERVAL DAY TO SECOND(0)",
        DataType::Timestamp => "TIMESTAMP(6)",
        DataType::TimestampTz => "TIMESTAMP(6) WITH TIME ZONE",
        DataType::Uuid => "VARCHAR2(36)",
    }
}

/// Maps a SourceColumn's type info to the target Oracle DDL type.
/// Accepts the DataType and optional pg_type_id for PG-source refinement.
pub fn column_type(data_type: DataType, pg_type_id: Option<u32>) -> &'static str {
    // For PG sources, String/Text refinement via OID
    if matches!(data_type, DataType::String | DataType::Text) {
        if let Some(oid) = pg_type_id {
            return pg_oid_to_oracle_type(oid);
        }
    }
    data_type_to_oracle(&data_type)
}

/// Map a PG type OID to Oracle column type.
/// Used as a refinement when the PG source provides more specific type info.
fn pg_oid_to_oracle_type(pg_type_id: u32) -> &'static str {
    match pg_type_id {
        // Character types
        25 | 1043 | 1042 => "CLOB",   // text, varchar, bpchar
        18 => "CHAR(1)",               // "char"
        // Network types
        869 => "VARCHAR2(45)",         // inet
        650 => "VARCHAR2(45)",         // cidr
        // UUID
        2950 => "VARCHAR2(36)",        // uuid
        // Default
        _ => "CLOB",
    }
}

/// Convert a core::Value to its Oracle SQL string representation for
/// use in INSERT/UPDATE/MERGE statements.
pub fn value_to_oracle_expr(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b { "1" } else { "0" }.to_string()
        }
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::String(s) => {
            // Escape single quotes for Oracle SQL
            let escaped = s.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::Bytes(b) => {
            format!("'{}'", hex::encode(b))
        }
        Value::Json(j) => {
            let s = serde_json::to_string(j).unwrap_or_default();
            let escaped = s.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::Timestamp(ts) => {
            // ts is epoch nanos as i64 — convert to Oracle TIMESTAMP literal
            let secs = ts / 1_000_000_000;
            let _subsec_nanos = (ts % 1_000_000_000).abs();
            // Use chrono for formatting if useful, but simple approach:
            format!("TO_TIMESTAMP('{}', 'FF9')", secs)
        }
        Value::Decimal(d) => d.to_string(),
        Value::Uuid(u) => {
            format!("'{}'", u)
        }
        Value::Unchanged => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_types() {
        assert_eq!(data_type_to_oracle(&DataType::Boolean), "NUMBER(1)");
        assert_eq!(data_type_to_oracle(&DataType::Int32), "NUMBER(10)");
        assert_eq!(data_type_to_oracle(&DataType::Int64), "NUMBER(19)");
        assert_eq!(data_type_to_oracle(&DataType::Float64), "BINARY_DOUBLE");
        assert_eq!(data_type_to_oracle(&DataType::String), "CLOB");
        assert_eq!(data_type_to_oracle(&DataType::Timestamp), "TIMESTAMP(6)");
    }

    #[test]
    fn test_value_conversion() {
        assert_eq!(value_to_oracle_expr(&Value::Null), "NULL");
        assert_eq!(value_to_oracle_expr(&Value::Bool(true)), "1");
        assert_eq!(value_to_oracle_expr(&Value::Int64(42)), "42");
        assert_eq!(value_to_oracle_expr(&Value::String("hello".into())), "'hello'");
        assert_eq!(value_to_oracle_expr(&Value::String("it's".into())), "'it''s'");
    }
}
