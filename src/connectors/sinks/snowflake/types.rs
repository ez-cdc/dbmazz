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
    ///
    /// This is the primary type dispatcher. Sinks should use this against
    /// `SourceColumn::data_type`; `pg_type_id` is only consulted as a
    /// refinement when present (PG-source-only) AND the DataType is too
    /// coarse to disambiguate.
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

    /// PG-source-only refinement: when the OID gives information beyond
    /// `DataType` (arrays, money, inet/cidr, interval), return the
    /// refined Snowflake type. Sinks should call this only when
    /// `SourceColumn::pg_type_id.is_some()` AND the dispatch on
    /// `DataType` produced a default like `VARCHAR`.
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
    /// Used in FLATTENED CTE: `_DATA:col_name::SNOWFLAKE_TYPE AS "COL_NAME"`.
    ///
    /// Dispatches primarily on `data_type` (source-agnostic). When
    /// `pg_type_id` is present (PG source), it is consulted only as
    /// refinement for cases the `DataType` is too coarse to express —
    /// PG arrays, money, inet/cidr, interval — all of which currently
    /// collapse to `DataType::String` at the PG boundary.
    pub fn variant_extract_expr(
        &self,
        col_name: &str,
        data_type: &DataType,
        pg_type_id: Option<u32>,
    ) -> String {
        // 1. Cases where DataType alone is sufficient and unambiguous.
        match data_type {
            DataType::Bytes => return format!("BASE64_DECODE_BINARY(_DATA:\"{}\")", col_name),
            DataType::Json | DataType::Jsonb => {
                return format!("TRY_PARSE_JSON(CAST(_DATA:\"{}\" AS VARCHAR))", col_name);
            }
            DataType::Decimal { .. } => {
                let sf_type = self.to_snowflake_type(data_type);
                return format!("TRY_CAST((_DATA:\"{}\")::VARCHAR AS {})", col_name, sf_type);
            }
            _ => {}
        }

        // 2. PG-source-only refinement for DataType::String cases the
        // PG→DataType boundary cannot disambiguate (arrays, money,
        // inet/cidr, interval).
        if let Some(oid) = pg_type_id {
            match oid {
                1005 | 1007 | 1016 | 1021 | 1022 | 1009 | 1015 => {
                    return format!("TRY_PARSE_JSON(CAST(_DATA:\"{}\" AS VARCHAR))", col_name);
                }
                790 => {
                    return format!(
                        "TRY_CAST((_DATA:\"{}\")::VARCHAR AS NUMBER(19,4))",
                        col_name
                    );
                }
                _ => {}
            }
        }

        // 3. Default: direct cast using DataType.
        let sf_type = self.to_snowflake_type(data_type);
        format!("_DATA:\"{}\"::{}", col_name, sf_type)
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
    fn test_variant_extract_expr_datatype_path() {
        let mapper = TypeMapper::new();

        // Every DataType variant must produce a correct expression
        // without any OID (non-PG sources, e.g., MySQL).
        assert_eq!(
            mapper.variant_extract_expr("flag", &DataType::Boolean, None),
            r#"_DATA:"flag"::BOOLEAN"#
        );
        assert_eq!(
            mapper.variant_extract_expr("n", &DataType::Int32, None),
            r#"_DATA:"n"::INT"#
        );
        assert_eq!(
            mapper.variant_extract_expr("n", &DataType::Int64, None),
            r#"_DATA:"n"::BIGINT"#
        );
        assert_eq!(
            mapper.variant_extract_expr("amount", &DataType::Float64, None),
            r#"_DATA:"amount"::DOUBLE"#
        );
        assert_eq!(
            mapper.variant_extract_expr("name", &DataType::String, None),
            r#"_DATA:"name"::VARCHAR"#
        );
        assert_eq!(
            mapper.variant_extract_expr("name", &DataType::Text, None),
            r#"_DATA:"name"::VARCHAR"#
        );
        assert_eq!(
            mapper.variant_extract_expr("id", &DataType::Uuid, None),
            r#"_DATA:"id"::VARCHAR(36)"#
        );
        assert_eq!(
            mapper.variant_extract_expr("d", &DataType::Date, None),
            r#"_DATA:"d"::DATE"#
        );
        assert_eq!(
            mapper.variant_extract_expr("t", &DataType::Time, None),
            r#"_DATA:"t"::TIME"#
        );
        assert_eq!(
            mapper.variant_extract_expr("ts", &DataType::Timestamp, None),
            r#"_DATA:"ts"::TIMESTAMP_NTZ"#
        );
        assert_eq!(
            mapper.variant_extract_expr("ts", &DataType::TimestampTz, None),
            r#"_DATA:"ts"::TIMESTAMP_TZ"#
        );

        // Bytes always uses BASE64_DECODE_BINARY regardless of source.
        assert_eq!(
            mapper.variant_extract_expr("avatar", &DataType::Bytes, None),
            r#"BASE64_DECODE_BINARY(_DATA:"avatar")"#
        );

        // JSON / JSONB always uses TRY_PARSE_JSON.
        assert_eq!(
            mapper.variant_extract_expr("md", &DataType::Json, None),
            r#"TRY_PARSE_JSON(CAST(_DATA:"md" AS VARCHAR))"#
        );
        assert_eq!(
            mapper.variant_extract_expr("md", &DataType::Jsonb, None),
            r#"TRY_PARSE_JSON(CAST(_DATA:"md" AS VARCHAR))"#
        );

        // Decimal uses TRY_CAST with full precision.
        assert_eq!(
            mapper.variant_extract_expr(
                "amount",
                &DataType::Decimal {
                    precision: 38,
                    scale: 9,
                },
                None,
            ),
            r#"TRY_CAST((_DATA:"amount")::VARCHAR AS NUMBER(38,9))"#
        );
    }

    #[test]
    fn test_variant_extract_expr_pg_refinement() {
        let mapper = TypeMapper::new();

        // PG arrays collapse to DataType::String — OID refines to ARRAY.
        assert_eq!(
            mapper.variant_extract_expr("tags", &DataType::String, Some(1009)),
            r#"TRY_PARSE_JSON(CAST(_DATA:"tags" AS VARCHAR))"#
        );
        assert_eq!(
            mapper.variant_extract_expr("ids", &DataType::String, Some(1007)),
            r#"TRY_PARSE_JSON(CAST(_DATA:"ids" AS VARCHAR))"#
        );

        // PG money OID refines to NUMBER(19,4) TRY_CAST.
        assert_eq!(
            mapper.variant_extract_expr("price", &DataType::String, Some(790)),
            r#"TRY_CAST((_DATA:"price")::VARCHAR AS NUMBER(19,4))"#
        );

        // PG text OID (25) — no refinement, falls through to DataType.
        assert_eq!(
            mapper.variant_extract_expr("name", &DataType::String, Some(25)),
            r#"_DATA:"name"::VARCHAR"#
        );

        // Decimal with PG numeric OID — DataType wins (refinement path
        // is bypassed because the DataType is already precise).
        assert_eq!(
            mapper.variant_extract_expr(
                "amount",
                &DataType::Decimal {
                    precision: 38,
                    scale: 9,
                },
                Some(1700),
            ),
            r#"TRY_CAST((_DATA:"amount")::VARCHAR AS NUMBER(38,9))"#
        );
    }
}
