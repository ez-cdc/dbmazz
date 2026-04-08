// Copyright 2025
// Licensed under the Elastic License v2.0

//! Snowflake Type Mappings
//!
//! Maps between CDC `DataType` / PostgreSQL OIDs and Snowflake SQL types.
//! Also provides `Value` → JSON serialization for Parquet raw table records.

use crate::core::{DataType, Value};

/// Type mapper for converting CDC types to Snowflake types.
#[derive(Debug, Clone)]
pub struct TypeMapper;

impl TypeMapper {
    pub fn new() -> Self {
        Self
    }

    /// Converts a CDC `DataType` to a Snowflake SQL type string.
    #[allow(dead_code)]
    pub fn to_snowflake_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int16 => "SMALLINT".to_string(),
            DataType::Int32 => "INT".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::Float32 => "FLOAT".to_string(),
            DataType::Float64 => "DOUBLE".to_string(),
            DataType::Decimal { precision, scale } => {
                let p = (*precision).min(38);
                let s = (*scale).min(p);
                format!("NUMBER({},{})", p, s)
            }
            DataType::String | DataType::Text => "VARCHAR".to_string(),
            DataType::Bytes => "BINARY".to_string(),
            DataType::Json | DataType::Jsonb => "VARIANT".to_string(),
            DataType::Uuid => "VARCHAR(36)".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::Timestamp => "TIMESTAMP_NTZ".to_string(),
            DataType::TimestampTz => "TIMESTAMP_TZ".to_string(),
        }
    }

    /// Converts a PostgreSQL type OID to a Snowflake SQL type string.
    pub fn pg_type_to_snowflake(&self, pg_type_id: u32) -> &'static str {
        match pg_type_id {
            16 => "BOOLEAN",               // bool
            21 => "SMALLINT",              // int2
            23 => "INT",                   // int4
            20 => "BIGINT",                // int8
            700 => "FLOAT",                // float4
            701 => "DOUBLE",               // float8
            1700 => "NUMBER(38,9)",        // numeric
            790 => "NUMBER(19,4)",         // money
            25 | 1043 | 1042 => "VARCHAR", // text, varchar, char
            17 => "BINARY",                // bytea
            114 | 3802 => "VARIANT",       // json, jsonb
            2950 => "VARCHAR(36)",         // uuid
            1082 => "DATE",                // date
            1083 | 1266 => "TIME",         // time, timetz
            1114 => "TIMESTAMP_NTZ",       // timestamp
            1184 => "TIMESTAMP_TZ",        // timestamptz
            1186 => "VARCHAR",             // interval (Snowflake has no interval)
            869 => "VARCHAR(45)",          // inet
            650 => "VARCHAR(49)",          // cidr
            // Array OIDs
            1005 | 1007 | 1016 | 1021 | 1022 | 1009 | 1015 => "ARRAY",
            _ => "VARCHAR", // safe default
        }
    }

    /// Converts a CDC `Value` to a JSON value for Parquet raw table records.
    pub fn value_to_json(&self, value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::json!(b),
            Value::Int64(i) => serde_json::json!(i),
            Value::Float64(f) => serde_json::json!(f),
            Value::String(s) => serde_json::json!(s),
            Value::Bytes(b) => {
                let encoded = hex::encode(b);
                serde_json::json!(encoded)
            }
            Value::Json(s) => serde_json::from_str(s).unwrap_or(serde_json::json!(s)),
            Value::Timestamp(ts) => {
                let secs = ts / 1_000_000;
                let nanos = ((ts % 1_000_000) * 1000) as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                    serde_json::json!(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                } else {
                    serde_json::Value::Null
                }
            }
            Value::Decimal(s) => serde_json::json!(s),
            Value::Uuid(s) => serde_json::json!(s),
            Value::Unchanged => serde_json::Value::Null,
        }
    }

    /// Returns the VARIANT extraction expression for a column in MERGE SQL.
    ///
    /// Used in FLATTENED CTE: `_DATA:col_name::SNOWFLAKE_TYPE AS "COL_NAME"`
    pub fn variant_extract_expr(&self, col_name: &str, pg_type_id: u32) -> String {
        let sf_type = self.pg_type_to_snowflake(pg_type_id);
        match pg_type_id {
            // Binary: base64 decode
            17 => format!("BASE64_DECODE_BINARY(_DATA:\"{}\")", col_name),
            // JSON/JSONB: parse to VARIANT
            114 | 3802 => {
                format!("TRY_PARSE_JSON(CAST(_DATA:\"{}\" AS VARCHAR))", col_name)
            }
            // Numeric with precision: use TRY_CAST to avoid breaking batch
            1700 | 790 => {
                format!("TRY_CAST((_DATA:\"{}\")::VARCHAR AS {})", col_name, sf_type)
            }
            // Arrays
            1005 | 1007 | 1016 | 1021 | 1022 | 1009 | 1015 => {
                format!("TRY_PARSE_JSON(CAST(_DATA:\"{}\" AS VARCHAR))", col_name)
            }
            // Default: direct cast
            _ => format!("_DATA:\"{}\"::{}", col_name, sf_type),
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
    fn test_datatype_to_snowflake() {
        let mapper = TypeMapper::new();

        assert_eq!(mapper.to_snowflake_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(mapper.to_snowflake_type(&DataType::Int16), "SMALLINT");
        assert_eq!(mapper.to_snowflake_type(&DataType::Int32), "INT");
        assert_eq!(mapper.to_snowflake_type(&DataType::Int64), "BIGINT");
        assert_eq!(mapper.to_snowflake_type(&DataType::Float32), "FLOAT");
        assert_eq!(mapper.to_snowflake_type(&DataType::Float64), "DOUBLE");
        assert_eq!(mapper.to_snowflake_type(&DataType::String), "VARCHAR");
        assert_eq!(mapper.to_snowflake_type(&DataType::Text), "VARCHAR");
        assert_eq!(mapper.to_snowflake_type(&DataType::Json), "VARIANT");
        assert_eq!(mapper.to_snowflake_type(&DataType::Jsonb), "VARIANT");
        assert_eq!(mapper.to_snowflake_type(&DataType::Uuid), "VARCHAR(36)");
        assert_eq!(mapper.to_snowflake_type(&DataType::Date), "DATE");
        assert_eq!(mapper.to_snowflake_type(&DataType::Time), "TIME");
        assert_eq!(
            mapper.to_snowflake_type(&DataType::Timestamp),
            "TIMESTAMP_NTZ"
        );
        assert_eq!(
            mapper.to_snowflake_type(&DataType::TimestampTz),
            "TIMESTAMP_TZ"
        );
        assert_eq!(mapper.to_snowflake_type(&DataType::Bytes), "BINARY");
    }

    #[test]
    fn test_decimal_mapping() {
        let mapper = TypeMapper::new();

        assert_eq!(
            mapper.to_snowflake_type(&DataType::Decimal {
                precision: 10,
                scale: 2
            }),
            "NUMBER(10,2)"
        );

        assert_eq!(
            mapper.to_snowflake_type(&DataType::Decimal {
                precision: 50,
                scale: 10
            }),
            "NUMBER(38,10)"
        );
    }

    #[test]
    fn test_pg_type_mapping() {
        let mapper = TypeMapper::new();

        assert_eq!(mapper.pg_type_to_snowflake(16), "BOOLEAN");
        assert_eq!(mapper.pg_type_to_snowflake(21), "SMALLINT");
        assert_eq!(mapper.pg_type_to_snowflake(23), "INT");
        assert_eq!(mapper.pg_type_to_snowflake(20), "BIGINT");
        assert_eq!(mapper.pg_type_to_snowflake(25), "VARCHAR");
        assert_eq!(mapper.pg_type_to_snowflake(17), "BINARY");
        assert_eq!(mapper.pg_type_to_snowflake(3802), "VARIANT");
        assert_eq!(mapper.pg_type_to_snowflake(2950), "VARCHAR(36)");
        assert_eq!(mapper.pg_type_to_snowflake(1114), "TIMESTAMP_NTZ");
        assert_eq!(mapper.pg_type_to_snowflake(1184), "TIMESTAMP_TZ");
        assert_eq!(mapper.pg_type_to_snowflake(1186), "VARCHAR"); // interval
        assert_eq!(mapper.pg_type_to_snowflake(869), "VARCHAR(45)"); // inet
        assert_eq!(mapper.pg_type_to_snowflake(1007), "ARRAY"); // int array
        assert_eq!(mapper.pg_type_to_snowflake(99999), "VARCHAR"); // unknown
    }

    #[test]
    fn test_value_to_json() {
        let mapper = TypeMapper::new();

        assert_eq!(mapper.value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(
            mapper.value_to_json(&Value::Bool(true)),
            serde_json::json!(true)
        );
        assert_eq!(
            mapper.value_to_json(&Value::Int64(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            mapper.value_to_json(&Value::Float64(3.5)),
            serde_json::json!(3.5)
        );
        assert_eq!(
            mapper.value_to_json(&Value::String("hello".to_string())),
            serde_json::json!("hello")
        );
        assert_eq!(
            mapper.value_to_json(&Value::Decimal("123.45".to_string())),
            serde_json::json!("123.45")
        );
        assert_eq!(
            mapper.value_to_json(&Value::Uuid(
                "550e8400-e29b-41d4-a716-446655440000".to_string()
            )),
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );
    }

    #[test]
    fn test_bytes_to_hex() {
        let mapper = TypeMapper::new();
        let bytes = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f];
        let result = mapper.value_to_json(&Value::Bytes(bytes));
        assert_eq!(result.as_str().unwrap(), "48656c6c6f");
    }

    #[test]
    fn test_variant_extract_expr() {
        let mapper = TypeMapper::new();

        // Default: direct cast
        assert_eq!(
            mapper.variant_extract_expr("name", 25),
            r#"_DATA:"name"::VARCHAR"#
        );

        // Binary: base64 decode
        assert_eq!(
            mapper.variant_extract_expr("avatar", 17),
            r#"BASE64_DECODE_BINARY(_DATA:"avatar")"#
        );

        // JSON: parse
        assert_eq!(
            mapper.variant_extract_expr("metadata", 3802),
            r#"TRY_PARSE_JSON(CAST(_DATA:"metadata" AS VARCHAR))"#
        );

        // Numeric: TRY_CAST
        assert_eq!(
            mapper.variant_extract_expr("amount", 1700),
            r#"TRY_CAST((_DATA:"amount")::VARCHAR AS NUMBER(38,9))"#
        );
    }
}
