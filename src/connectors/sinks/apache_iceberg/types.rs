// Copyright 2025
// Licensed under the Elastic License v2.0

//! Apache Iceberg Type Mappings
//!
//! Maps between CDC `DataType` and Iceberg type strings for schema
//! definition. Also provides `Value` → JSON serialization for Parquet
//! data files.
//!
//! Iceberg type system: boolean, int, long, float, double,
//! decimal(precision,scale), string, binary, date, time, timestamp,
//! timestamptz.

use crate::core::{DataType, Value};

/// Type mapper for converting CDC types to Iceberg types.
#[derive(Debug, Clone)]
pub struct TypeMapper;

impl TypeMapper {
    pub fn new() -> Self {
        Self
    }

    /// Map a CDC DataType to an Iceberg type string (for schema definition).
    ///
    /// Iceberg types: boolean, int, long, float, double,
    /// decimal(precision,scale), string, binary, date, time, timestamp,
    /// timestamptz.
    pub fn to_iceberg_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Boolean => "boolean".to_string(),
            DataType::Int16 => "int".to_string(),
            DataType::Int32 => "int".to_string(),
            DataType::Int64 => "long".to_string(),
            // u64::MAX = 18_446_744_073_709_551_615 → 20 digits.
            // decimal(20,0) fits the full unsigned range exactly.
            DataType::UInt64 => "decimal(20,0)".to_string(),
            DataType::Float32 => "float".to_string(),
            DataType::Float64 => "double".to_string(),
            DataType::Decimal { precision, scale } => {
                // Iceberg decimal max precision is 38
                let p = (*precision).min(38);
                let s = (*scale).min(p);
                format!("decimal({},{})", p, s)
            }
            DataType::String | DataType::Text => "string".to_string(),
            DataType::Bytes => "binary".to_string(),
            DataType::Json | DataType::Jsonb => "string".to_string(),
            DataType::Uuid => "string".to_string(),
            DataType::Date => "date".to_string(),
            DataType::Time => "time".to_string(),
            DataType::Timestamp => "timestamp".to_string(),
            DataType::TimestampTz => "timestamptz".to_string(),
        }
    }

    /// Serialize a CDC Value to a serde_json::Value for writing to Parquet.
    pub fn value_to_json(&self, value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::json!(b),
            Value::Int64(i) => serde_json::json!(i),
            // Stringify u64 to avoid JSON-number precision loss in
            // downstream parsers that read as i64 / f64.
            Value::UInt64(u) => serde_json::json!(u.to_string()),
            Value::Float64(f) => serde_json::json!(f),
            Value::String(s) => serde_json::json!(s),
            Value::Bytes(b) => serde_json::json!(hex::encode(b)),
            Value::Json(j) => serde_json::from_str(j).unwrap_or(serde_json::json!({"raw": j})),
            Value::Timestamp(ts) => serde_json::json!(ts),
            Value::Decimal(d) => serde_json::json!(d),
            Value::Uuid(u) => serde_json::json!(u),
            Value::Unchanged => serde_json::Value::Null,
        }
    }

    /// Convert a CDC Value to a plain string for Parquet storage.
    ///
    /// Unlike `value_to_json(...).to_string()`, this does NOT add JSON quotes
    /// around string values. It extracts the raw value directly.
    pub fn value_to_parquet_string(&self, value: &Value) -> String {
        match value {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::UInt64(u) => u.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::String(s) => s.clone(),
            Value::Bytes(b) => hex::encode(b),
            Value::Json(j) => j.clone(),
            Value::Timestamp(ts) => ts.to_string(),
            Value::Decimal(d) => d.clone(),
            Value::Uuid(u) => u.clone(),
            Value::Unchanged => String::new(),
        }
    }
}
