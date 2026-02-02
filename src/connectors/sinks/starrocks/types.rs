// Copyright 2025
// Licensed under the Elastic License v2.0

#![allow(dead_code)]
//! StarRocks Type Mappings
//!
//! This module provides mappings between:
//! - `core::DataType` (database-agnostic) -> StarRocks column types
//! - `core::Value` -> JSON values for Stream Load
//! - PostgreSQL type OIDs -> StarRocks types (legacy support)
//!
//! ## StarRocks Type System
//!
//! StarRocks supports the following data types:
//!
//! | Category | Types |
//! |----------|-------|
//! | Numeric | TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL |
//! | String | CHAR, VARCHAR, STRING |
//! | Date/Time | DATE, DATETIME |
//! | Boolean | BOOLEAN |
//! | Semi-structured | JSON, ARRAY, MAP, STRUCT |
//! | Binary | BINARY, VARBINARY |
//!
//! ## Mapping Strategy
//!
//! - Prefer larger types to avoid overflow (e.g., INT64 -> BIGINT)
//! - Use STRING for unbounded text to avoid VARCHAR length issues
//! - DECIMAL preserves precision for financial data
//! - JSON for semi-structured data

use crate::core::{DataType, Value};

/// Type mapper for converting CDC types to StarRocks types.
#[derive(Debug, Clone)]
pub struct TypeMapper {
    /// Default DECIMAL precision
    default_decimal_precision: u8,
    /// Default DECIMAL scale
    default_decimal_scale: u8,
}

impl TypeMapper {
    /// Creates a new type mapper with default settings.
    pub fn new() -> Self {
        Self {
            default_decimal_precision: 38,
            default_decimal_scale: 9,
        }
    }

    /// Converts a CDC `DataType` to a StarRocks type string.
    ///
    /// # Arguments
    ///
    /// * `data_type` - The CDC data type to convert
    ///
    /// # Returns
    ///
    /// StarRocks SQL type string
    pub fn to_starrocks_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int16 => "SMALLINT".to_string(),
            DataType::Int32 => "INT".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::Float32 => "FLOAT".to_string(),
            DataType::Float64 => "DOUBLE".to_string(),
            DataType::Decimal { precision, scale } => {
                // StarRocks DECIMAL max precision is 38
                let p = (*precision).min(38);
                let s = (*scale).min(p);
                format!("DECIMAL({},{})", p, s)
            }
            DataType::String => "STRING".to_string(),
            DataType::Text => "STRING".to_string(),
            DataType::Bytes => "VARBINARY".to_string(),
            DataType::Json | DataType::Jsonb => "JSON".to_string(),
            DataType::Uuid => "STRING".to_string(), // StarRocks doesn't have native UUID
            DataType::Date => "DATE".to_string(),
            DataType::Time => "STRING".to_string(), // StarRocks doesn't have TIME type
            DataType::Timestamp => "DATETIME".to_string(),
            DataType::TimestampTz => "DATETIME".to_string(), // StarRocks DATETIME doesn't store TZ
        }
    }

    /// Converts a PostgreSQL type OID to a StarRocks type string.
    ///
    /// This is provided for backward compatibility with the legacy sink.
    ///
    /// # Arguments
    ///
    /// * `pg_type_id` - PostgreSQL type OID
    ///
    /// # Returns
    ///
    /// StarRocks SQL type string
    pub fn pg_type_to_starrocks(&self, pg_type_id: u32) -> &'static str {
        match pg_type_id {
            16 => "BOOLEAN",           // bool
            21 => "SMALLINT",          // int2
            23 => "INT",               // int4
            20 => "BIGINT",            // int8
            700 => "FLOAT",            // float4
            701 => "DOUBLE",           // float8
            1700 => "DECIMAL(38,9)",   // numeric
            1114 => "DATETIME",        // timestamp
            1184 => "DATETIME",        // timestamptz
            25 => "STRING",            // text
            1043 => "STRING",          // varchar
            1042 => "STRING",          // char/bpchar
            3802 => "JSON",            // jsonb
            114 => "JSON",             // json
            2950 => "STRING",          // uuid
            17 => "VARBINARY",         // bytea
            1082 => "DATE",            // date
            1083 => "STRING",          // time
            1266 => "STRING",          // timetz
            _ => "STRING",             // default fallback
        }
    }

    /// Converts a CDC `Value` to a JSON value for Stream Load.
    ///
    /// # Arguments
    ///
    /// * `value` - The CDC value to convert
    ///
    /// # Returns
    ///
    /// JSON value suitable for Stream Load
    pub fn value_to_json(&self, value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::json!(b),
            Value::Int64(i) => serde_json::json!(i),
            Value::Float64(f) => serde_json::json!(f),
            Value::String(s) => serde_json::json!(s),
            Value::Bytes(b) => {
                // Encode bytes as hex for Stream Load (StarRocks VARBINARY)
                let encoded = hex::encode(b);
                serde_json::json!(encoded)
            }
            Value::Json(s) => {
                // Parse JSON string to JSON value
                serde_json::from_str(s).unwrap_or(serde_json::json!(s))
            }
            Value::Timestamp(ts) => {
                // Convert Unix timestamp (micros) to datetime string
                let secs = ts / 1_000_000;
                let nanos = ((ts % 1_000_000) * 1000) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                    serde_json::json!(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                } else {
                    serde_json::Value::Null
                }
            }
            Value::Decimal(s) => {
                // Keep decimal as string to preserve precision
                serde_json::json!(s)
            }
            Value::Uuid(s) => serde_json::json!(s),
            Value::Unchanged => {
                // TOAST/unchanged value - should be excluded in partial updates
                // Return null as fallback
                serde_json::Value::Null
            }
        }
    }

    /// Converts a PostgreSQL text value to JSON based on type OID.
    ///
    /// This is provided for backward compatibility with the legacy sink.
    ///
    /// # Arguments
    ///
    /// * `text` - The text representation of the value
    /// * `pg_type_id` - PostgreSQL type OID
    ///
    /// # Returns
    ///
    /// JSON value suitable for Stream Load
    pub fn pg_text_to_json(&self, text: &str, pg_type_id: u32) -> serde_json::Value {
        match pg_type_id {
            // Boolean
            16 => {
                match text.to_lowercase().as_str() {
                    "t" | "true" | "1" => serde_json::json!(true),
                    _ => serde_json::json!(false),
                }
            }
            // Integer types (INT2, INT4, INT8)
            21 | 23 | 20 => {
                text.parse::<i64>()
                    .map(|n| serde_json::json!(n))
                    .unwrap_or_else(|_| serde_json::json!(text))
            }
            // Float types (FLOAT4, FLOAT8)
            700 | 701 => {
                text.parse::<f64>()
                    .map(|f| serde_json::json!(f))
                    .unwrap_or_else(|_| serde_json::json!(text))
            }
            // NUMERIC/DECIMAL - keep as string for precision
            1700 => serde_json::json!(text),
            // Timestamp types
            1114 | 1184 => serde_json::json!(text),
            // JSON/JSONB
            114 | 3802 => {
                serde_json::from_str(text).unwrap_or(serde_json::json!(text))
            }
            // Default: string
            _ => serde_json::json!(text),
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
    fn test_datatype_to_starrocks() {
        let mapper = TypeMapper::new();

        assert_eq!(mapper.to_starrocks_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(mapper.to_starrocks_type(&DataType::Int16), "SMALLINT");
        assert_eq!(mapper.to_starrocks_type(&DataType::Int32), "INT");
        assert_eq!(mapper.to_starrocks_type(&DataType::Int64), "BIGINT");
        assert_eq!(mapper.to_starrocks_type(&DataType::Float32), "FLOAT");
        assert_eq!(mapper.to_starrocks_type(&DataType::Float64), "DOUBLE");
        assert_eq!(mapper.to_starrocks_type(&DataType::String), "STRING");
        assert_eq!(mapper.to_starrocks_type(&DataType::Text), "STRING");
        assert_eq!(mapper.to_starrocks_type(&DataType::Json), "JSON");
        assert_eq!(mapper.to_starrocks_type(&DataType::Jsonb), "JSON");
        assert_eq!(mapper.to_starrocks_type(&DataType::Uuid), "STRING");
        assert_eq!(mapper.to_starrocks_type(&DataType::Date), "DATE");
        assert_eq!(mapper.to_starrocks_type(&DataType::Time), "STRING");
        assert_eq!(mapper.to_starrocks_type(&DataType::Timestamp), "DATETIME");
        assert_eq!(mapper.to_starrocks_type(&DataType::TimestampTz), "DATETIME");
        assert_eq!(mapper.to_starrocks_type(&DataType::Bytes), "VARBINARY");
    }

    #[test]
    fn test_decimal_mapping() {
        let mapper = TypeMapper::new();

        assert_eq!(
            mapper.to_starrocks_type(&DataType::Decimal { precision: 10, scale: 2 }),
            "DECIMAL(10,2)"
        );

        // Test precision capping at 38
        assert_eq!(
            mapper.to_starrocks_type(&DataType::Decimal { precision: 50, scale: 10 }),
            "DECIMAL(38,10)"
        );
    }

    #[test]
    fn test_value_to_json() {
        let mapper = TypeMapper::new();

        assert_eq!(mapper.value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(mapper.value_to_json(&Value::Bool(true)), serde_json::json!(true));
        assert_eq!(mapper.value_to_json(&Value::Int64(42)), serde_json::json!(42));
        assert_eq!(mapper.value_to_json(&Value::Float64(3.5)), serde_json::json!(3.5));
        assert_eq!(
            mapper.value_to_json(&Value::String("hello".to_string())),
            serde_json::json!("hello")
        );
        assert_eq!(
            mapper.value_to_json(&Value::Decimal("123.45".to_string())),
            serde_json::json!("123.45")
        );
        assert_eq!(
            mapper.value_to_json(&Value::Uuid("550e8400-e29b-41d4-a716-446655440000".to_string())),
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn test_json_value_parsing() {
        let mapper = TypeMapper::new();

        let json_str = r#"{"key": "value", "num": 42}"#;
        let result = mapper.value_to_json(&Value::Json(json_str.to_string()));

        assert!(result.is_object());
        assert_eq!(result["key"], "value");
        assert_eq!(result["num"], 42);
    }

    #[test]
    fn test_timestamp_conversion() {
        let mapper = TypeMapper::new();

        // 2024-01-15 10:30:00 UTC in microseconds
        let ts = 1_705_315_800_000_000_i64;
        let result = mapper.value_to_json(&Value::Timestamp(ts));

        assert!(result.is_string());
        let ts_str = result.as_str().unwrap();
        assert!(ts_str.starts_with("2024-01-15"));
    }

    #[test]
    fn test_pg_type_mapping() {
        let mapper = TypeMapper::new();

        assert_eq!(mapper.pg_type_to_starrocks(16), "BOOLEAN");
        assert_eq!(mapper.pg_type_to_starrocks(21), "SMALLINT");
        assert_eq!(mapper.pg_type_to_starrocks(23), "INT");
        assert_eq!(mapper.pg_type_to_starrocks(20), "BIGINT");
        assert_eq!(mapper.pg_type_to_starrocks(25), "STRING");
        assert_eq!(mapper.pg_type_to_starrocks(3802), "JSON");
        assert_eq!(mapper.pg_type_to_starrocks(99999), "STRING"); // Unknown type
    }

    #[test]
    fn test_pg_text_to_json() {
        let mapper = TypeMapper::new();

        // Boolean
        assert_eq!(mapper.pg_text_to_json("t", 16), serde_json::json!(true));
        assert_eq!(mapper.pg_text_to_json("f", 16), serde_json::json!(false));

        // Integer
        assert_eq!(mapper.pg_text_to_json("42", 23), serde_json::json!(42));

        // Float
        assert_eq!(mapper.pg_text_to_json("3.5", 701), serde_json::json!(3.5));

        // Decimal (kept as string)
        assert_eq!(
            mapper.pg_text_to_json("123.456789", 1700),
            serde_json::json!("123.456789")
        );

        // Text
        assert_eq!(
            mapper.pg_text_to_json("hello world", 25),
            serde_json::json!("hello world")
        );
    }

    #[test]
    fn test_bytes_to_hex() {
        let mapper = TypeMapper::new();

        let bytes = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let result = mapper.value_to_json(&Value::Bytes(bytes));

        assert!(result.is_string());
        assert_eq!(result.as_str().unwrap(), "48656c6c6f"); // Hex of "Hello"
    }
}
