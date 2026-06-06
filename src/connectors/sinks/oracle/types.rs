// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle Type Mappings
//!
//! This module provides mappings between:
//! - `core::DataType` (database-agnostic) -> Oracle column types
//! - `core::Value` -> Oracle SQL values for prepared statements
//!
//! ## Oracle Type System
//!
//! Oracle supports the following data types:
//!
//! | Category | Types |
//! |----------|-------|
//! | Numeric | NUMBER(p,s), BINARY_FLOAT, BINARY_DOUBLE |
//! | Character | CHAR(n), VARCHAR2(n), NCHAR(n), NVARCHAR2(n), CLOB, NCLOB |
//! | Date/Time | DATE, TIMESTAMP(p), TIMESTAMP(p) WITH TIME ZONE, TIMESTAMP(p) WITH LOCAL TIME ZONE |
//! | Binary | RAW(n), BLOB, BFILE |
//! | Large Objects | CLOB, NCLOB, BLOB |
//! | XML | XMLTYPE |
//! | JSON | JSON (21c+), CLOB |
//! | Rowid | ROWID, UROWID |

use crate::core::{DataType, Value};

#[cfg(feature = "sink-oracle")]
use oracle_rs::Value as OracleValue;

/// Type mapper for converting CDC types to Oracle types and values.
#[derive(Debug, Clone)]
pub struct TypeMapper {
    /// Default string length for VARCHAR2 when no length is specified
    default_string_length: u16,
}

impl TypeMapper {
    /// Creates a new type mapper with default settings.
    pub fn new() -> Self {
        Self {
            default_string_length: 4000,
        }
    }

    /// Converts a CDC `DataType` to an Oracle SQL type string for DDL.
    pub fn to_oracle_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Boolean => "NUMBER(1)".to_string(),
            DataType::Int16 => "NUMBER(5)".to_string(),
            DataType::Int32 => "NUMBER(10)".to_string(),
            DataType::Int64 => "NUMBER(19)".to_string(),
            DataType::Float32 => "BINARY_FLOAT".to_string(),
            DataType::Float64 => "BINARY_DOUBLE".to_string(),
            DataType::Decimal { precision, scale } => {
                let p = (*precision).min(38).max(1);
                let s = (*scale).min(p);
                format!("NUMBER({},{})", p, s)
            }
            DataType::String => format!("VARCHAR2({})", self.default_string_length),
            DataType::Text => "CLOB".to_string(),
            DataType::Bytes => "BLOB".to_string(),
            DataType::Json | DataType::Jsonb => "CLOB".to_string(),
            DataType::Uuid => "VARCHAR2(36)".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time => "INTERVAL DAY TO SECOND".to_string(),
            DataType::Timestamp => "TIMESTAMP(6)".to_string(),
            DataType::TimestampTz => "TIMESTAMP(6) WITH TIME ZONE".to_string(),
        }
    }

    /// Converts a CDC `Value` to an `oracle_rs::Value` for use as a bind parameter.
    ///
    /// This is the primary conversion for parameterized queries.
    /// Returns `oracle_rs::Value::Null` for both `Null` and `Unchanged` variants.
    pub fn value_to_oracle(&self, value: &Value) -> OracleValue {
        match value {
            Value::Null => OracleValue::Null,
            Value::Bool(b) => OracleValue::Boolean(*b),
            Value::Int64(i) => OracleValue::Integer(*i),
            Value::Float64(f) => OracleValue::Float(*f),
            Value::String(s) => OracleValue::String(s.clone()),
            Value::Bytes(b) => OracleValue::Bytes(b.clone()),
            Value::Json(s) => OracleValue::String(s.clone()),
            Value::Timestamp(ts) => {
                // Store as epoch microseconds in a NUMBER column
                OracleValue::Integer(*ts)
            }
            Value::Decimal(s) => OracleValue::String(s.clone()),
            Value::Uuid(s) => OracleValue::String(s.clone()),
            Value::Unchanged => OracleValue::Null,
        }
    }

    /// Converts a CDC `Value` to a string representation for use in MERGE SQL.
    /// Returns "NULL" for null values.
    pub fn value_to_sql_literal(&self, value: &Value) -> String {
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
                // BLOB as hex
                format!("'{}'", hex::encode(b))
            }
            Value::Json(s) => {
                let escaped = s.replace('\'', "''");
                format!("'{}'", escaped)
            }
            Value::Timestamp(ts) => {
                let secs = ts / 1_000_000;
                let nanos = ((ts % 1_000_000) * 1000) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                    format!(
                        "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                        dt.format("%Y-%m-%d %H:%M:%S%.6f")
                    )
                } else {
                    "NULL".to_string()
                }
            }
            Value::Decimal(s) => s.clone(),
            Value::Uuid(s) => format!("'{}'", s),
            Value::Unchanged => "NULL".to_string(),
        }
    }
}

impl Default for TypeMapper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datatype_to_oracle() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.to_oracle_type(&DataType::Boolean), "NUMBER(1)");
        assert_eq!(mapper.to_oracle_type(&DataType::Int16), "NUMBER(5)");
        assert_eq!(mapper.to_oracle_type(&DataType::Int32), "NUMBER(10)");
        assert_eq!(mapper.to_oracle_type(&DataType::Int64), "NUMBER(19)");
        assert_eq!(mapper.to_oracle_type(&DataType::Float32), "BINARY_FLOAT");
        assert_eq!(mapper.to_oracle_type(&DataType::Float64), "BINARY_DOUBLE");
        assert_eq!(mapper.to_oracle_type(&DataType::String), "VARCHAR2(4000)");
        assert_eq!(mapper.to_oracle_type(&DataType::Text), "CLOB");
        assert_eq!(mapper.to_oracle_type(&DataType::Bytes), "BLOB");
        assert_eq!(mapper.to_oracle_type(&DataType::Json), "CLOB");
        assert_eq!(mapper.to_oracle_type(&DataType::Jsonb), "CLOB");
        assert_eq!(mapper.to_oracle_type(&DataType::Uuid), "VARCHAR2(36)");
        assert_eq!(mapper.to_oracle_type(&DataType::Date), "DATE");
        assert_eq!(mapper.to_oracle_type(&DataType::Time), "INTERVAL DAY TO SECOND");
        assert_eq!(mapper.to_oracle_type(&DataType::Timestamp), "TIMESTAMP(6)");
        assert_eq!(
            mapper.to_oracle_type(&DataType::TimestampTz),
            "TIMESTAMP(6) WITH TIME ZONE"
        );
    }

    #[test]
    fn test_decimal_mapping() {
        let mapper = TypeMapper::new();
        assert_eq!(
            mapper.to_oracle_type(&DataType::Decimal {
                precision: 10,
                scale: 2
            }),
            "NUMBER(10,2)"
        );
        // Test precision capping
        assert_eq!(
            mapper.to_oracle_type(&DataType::Decimal {
                precision: 50,
                scale: 10
            }),
            "NUMBER(38,10)"
        );
        // Test scale clamping
        assert_eq!(
            mapper.to_oracle_type(&DataType::Decimal {
                precision: 10,
                scale: 15
            }),
            "NUMBER(10,10)"
        );
    }

    #[test]
    fn test_value_to_sql_literal() {
        let mapper = TypeMapper::new();
        assert_eq!(mapper.value_to_sql_literal(&Value::Null), "NULL");
        assert_eq!(mapper.value_to_sql_literal(&Value::Bool(true)), "1");
        assert_eq!(mapper.value_to_sql_literal(&Value::Bool(false)), "0");
        assert_eq!(mapper.value_to_sql_literal(&Value::Int64(42)), "42");
        assert_eq!(mapper.value_to_sql_literal(&Value::Float64(3.5)), "3.5");
        assert_eq!(
            mapper.value_to_sql_literal(&Value::String("hello".to_string())),
            "'hello'"
        );
        assert_eq!(
            mapper.value_to_sql_literal(&Value::String("it's".to_string())),
            "'it''s'"
        );
        assert_eq!(
            mapper.value_to_sql_literal(&Value::Decimal("123.45".to_string())),
            "123.45"
        );
        assert_eq!(
            mapper.value_to_sql_literal(&Value::Uuid(
                "550e8400-e29b-41d4-a716-446655440000".to_string()
            )),
            "'550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    #[test]
    fn test_timestamp_literal() {
        let mapper = TypeMapper::new();
        // 2024-01-15 10:30:00 UTC in microseconds
        let ts = 1_705_315_800_000_000_i64;
        let result = mapper.value_to_sql_literal(&Value::Timestamp(ts));
        assert!(result.starts_with("TO_TIMESTAMP("));
        assert!(result.contains("2024-01-15"));
    }

    #[test]
    fn test_bytes_literal() {
        let mapper = TypeMapper::new();
        let bytes = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let result = mapper.value_to_sql_literal(&Value::Bytes(bytes));
        assert_eq!(result, "'48656c6c6f'");
    }

    #[test]
    fn test_value_to_oracle() {
        let mapper = TypeMapper::new();

        // Null
        assert!(matches!(mapper.value_to_oracle(&Value::Null), OracleValue::Null));

        // Bool
        assert!(matches!(mapper.value_to_oracle(&Value::Bool(true)), OracleValue::Boolean(true)));
        assert!(matches!(mapper.value_to_oracle(&Value::Bool(false)), OracleValue::Boolean(false)));

        // Int64
        assert!(matches!(mapper.value_to_oracle(&Value::Int64(42)), OracleValue::Integer(42)));

        // Float64
        assert!(matches!(mapper.value_to_oracle(&Value::Float64(3.14)), OracleValue::Float(v) if (v - 3.14).abs() < 1e-10));

        // String
        assert!(matches!(
            mapper.value_to_oracle(&Value::String("hello".to_string())),
            OracleValue::String(ref s) if s == "hello"
        ));

        // Bytes
        let bytes = vec![0x00, 0x01, 0x02];
        assert!(matches!(
            mapper.value_to_oracle(&Value::Bytes(bytes.clone())),
            OracleValue::Bytes(ref b) if b == &bytes
        ));

        // Json stored as string
        let json = r#"{"key": "value"}"#.to_string();
        assert!(matches!(
            mapper.value_to_oracle(&Value::Json(json.clone())),
            OracleValue::String(ref s) if s == &json
        ));

        // Timestamp stored as epoch micros integer
        assert!(matches!(
            mapper.value_to_oracle(&Value::Timestamp(1_705_315_800_000_000)),
            OracleValue::Integer(1_705_315_800_000_000)
        ));

        // Decimal stored as string
        assert!(matches!(
            mapper.value_to_oracle(&Value::Decimal("123.45".to_string())),
            OracleValue::String(ref s) if s == "123.45"
        ));

        // Uuid stored as string
        let uuid = "550e8400-e29b-41d4-a716-446655440000".to_string();
        assert!(matches!(
            mapper.value_to_oracle(&Value::Uuid(uuid.clone())),
            OracleValue::String(ref s) if s == &uuid
        ));

        // Unchanged maps to Null
        assert!(matches!(mapper.value_to_oracle(&Value::Unchanged), OracleValue::Null));
    }

    #[test]
    fn test_default() {
        let mapper = TypeMapper::default();
        assert_eq!(mapper.to_oracle_type(&DataType::String), "VARCHAR2(4000)");
    }
}
