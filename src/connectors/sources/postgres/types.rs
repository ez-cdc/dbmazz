// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL type mappings to core DataType
//!
//! This module provides conversion between PostgreSQL type OIDs and
//! the database-agnostic DataType used throughout the CDC system.
//!
//! # PostgreSQL Type OIDs
//! PostgreSQL uses OIDs (Object Identifiers) to identify data types.
//! Common OIDs are defined in `pg_type.h` and can be queried from
//! the `pg_type` catalog table.
//!
//! # Reference
//! - PostgreSQL Type OIDs: https://www.postgresql.org/docs/current/datatype.html
//! - pg_type catalog: https://www.postgresql.org/docs/current/catalog-pg-type.html

use crate::connectors::sources::postgres::parser::{Column, Tuple, TupleData};
use crate::core::{ColumnDef, ColumnValue, DataType, Value};

/// PostgreSQL type OIDs for common types
pub mod pg_oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const CHAR: u32 = 18;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const XML: u32 = 142;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const MONEY: u32 = 790;
    pub const MACADDR: u32 = 829;
    pub const INET: u32 = 869;
    pub const CIDR: u32 = 650;
    pub const MACADDR8: u32 = 774;
    pub const BPCHAR: u32 = 1042;  // char(n)
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;
    pub const TIMETZ: u32 = 1266;
    pub const BIT: u32 = 1560;
    pub const VARBIT: u32 = 1562;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;

    // Array types (common ones)
    pub const INT2_ARRAY: u32 = 1005;
    pub const INT4_ARRAY: u32 = 1007;
    pub const INT8_ARRAY: u32 = 1016;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const VARCHAR_ARRAY: u32 = 1015;
    pub const FLOAT4_ARRAY: u32 = 1021;
    pub const FLOAT8_ARRAY: u32 = 1022;
}

/// PostgreSQL type mapper for converting between PG types and core DataType
pub struct PgTypeMapper;

impl PgTypeMapper {
    /// Convert a PostgreSQL type OID to a core DataType
    pub fn pg_oid_to_data_type(type_id: u32, type_mod: i32) -> DataType {
        pg_type_to_data_type(type_id, type_mod)
    }

    /// Check if a type is a numeric type
    pub fn is_numeric(type_id: u32) -> bool {
        matches!(
            type_id,
            pg_oid::INT2
                | pg_oid::INT4
                | pg_oid::INT8
                | pg_oid::FLOAT4
                | pg_oid::FLOAT8
                | pg_oid::NUMERIC
                | pg_oid::MONEY
        )
    }

    /// Check if a type is a text type
    pub fn is_text(type_id: u32) -> bool {
        matches!(
            type_id,
            pg_oid::TEXT | pg_oid::VARCHAR | pg_oid::BPCHAR | pg_oid::CHAR | pg_oid::NAME
        )
    }

    /// Check if a type is a temporal type
    pub fn is_temporal(type_id: u32) -> bool {
        matches!(
            type_id,
            pg_oid::DATE
                | pg_oid::TIME
                | pg_oid::TIMETZ
                | pg_oid::TIMESTAMP
                | pg_oid::TIMESTAMPTZ
                | pg_oid::INTERVAL
        )
    }

    /// Check if a type is a JSON type
    pub fn is_json(type_id: u32) -> bool {
        matches!(type_id, pg_oid::JSON | pg_oid::JSONB)
    }
}

/// Convert PostgreSQL type OID to core DataType
///
/// # Arguments
/// * `type_id` - PostgreSQL type OID
/// * `type_mod` - Type modifier (e.g., precision for numeric, length for varchar)
///
/// # Returns
/// The corresponding core DataType
pub fn pg_type_to_data_type(type_id: u32, type_mod: i32) -> DataType {
    match type_id {
        pg_oid::BOOL => DataType::Boolean,
        pg_oid::INT2 => DataType::Int16,
        pg_oid::INT4 | pg_oid::OID => DataType::Int32,
        pg_oid::INT8 => DataType::Int64,
        pg_oid::FLOAT4 => DataType::Float32,
        pg_oid::FLOAT8 | pg_oid::MONEY => DataType::Float64,

        pg_oid::NUMERIC => {
            // type_mod encodes precision and scale for NUMERIC
            // type_mod = (precision << 16) | scale + VARHDRSZ
            if type_mod > 0 {
                let precision = ((type_mod - 4) >> 16) as u8;
                let scale = ((type_mod - 4) & 0xFFFF) as u8;
                DataType::Decimal { precision, scale }
            } else {
                // No precision specified, use defaults
                DataType::Decimal {
                    precision: 38,
                    scale: 10,
                }
            }
        }

        pg_oid::CHAR | pg_oid::BPCHAR | pg_oid::VARCHAR | pg_oid::NAME => DataType::String,

        pg_oid::TEXT | pg_oid::XML => DataType::Text,

        pg_oid::BYTEA => DataType::Bytes,

        pg_oid::JSON => DataType::Json,
        pg_oid::JSONB => DataType::Jsonb,

        pg_oid::UUID => DataType::Uuid,

        pg_oid::DATE => DataType::Date,
        pg_oid::TIME | pg_oid::TIMETZ => DataType::Time,
        pg_oid::TIMESTAMP => DataType::Timestamp,
        pg_oid::TIMESTAMPTZ => DataType::TimestampTz,

        // Network types - treat as string
        pg_oid::INET | pg_oid::CIDR | pg_oid::MACADDR | pg_oid::MACADDR8 => DataType::String,

        // Bit types - treat as string
        pg_oid::BIT | pg_oid::VARBIT => DataType::String,

        // Interval - treat as string (no direct equivalent)
        pg_oid::INTERVAL => DataType::String,

        // Array types - treat as JSON for now
        pg_oid::INT2_ARRAY
        | pg_oid::INT4_ARRAY
        | pg_oid::INT8_ARRAY
        | pg_oid::TEXT_ARRAY
        | pg_oid::VARCHAR_ARRAY
        | pg_oid::FLOAT4_ARRAY
        | pg_oid::FLOAT8_ARRAY => DataType::Json,

        // Unknown types - default to String
        _ => {
            // Log warning for unknown types in debug builds
            #[cfg(debug_assertions)]
            eprintln!("WARNING: Unknown PostgreSQL type OID: {}", type_id);

            DataType::String
        }
    }
}

/// Convert a tuple data value to a core Value
pub fn tuple_data_to_value(data: &TupleData, type_id: u32) -> Value {
    match data {
        TupleData::Null => Value::Null,
        TupleData::Toast => Value::Unchanged,
        TupleData::Text(bytes) => {
            // Convert bytes to appropriate Value based on type
            let text = match std::str::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => return Value::Bytes(bytes.to_vec()),
            };

            match type_id {
                pg_oid::BOOL => {
                    let v = text == "t" || text == "true" || text == "1";
                    Value::Bool(v)
                }
                pg_oid::INT2 | pg_oid::INT4 | pg_oid::INT8 => {
                    text.parse::<i64>().map(Value::Int64).unwrap_or_else(|_| Value::String(text.to_string()))
                }
                pg_oid::FLOAT4 | pg_oid::FLOAT8 => {
                    text.parse::<f64>().map(Value::Float64).unwrap_or_else(|_| Value::String(text.to_string()))
                }
                pg_oid::NUMERIC | pg_oid::MONEY => Value::Decimal(text.to_string()),
                pg_oid::JSON | pg_oid::JSONB => Value::Json(text.to_string()),
                pg_oid::UUID => Value::Uuid(text.to_string()),
                pg_oid::TIMESTAMP | pg_oid::TIMESTAMPTZ => {
                    // Keep as string for now; could parse to i64 microseconds
                    Value::String(text.to_string())
                }
                pg_oid::BYTEA => {
                    // PostgreSQL sends bytea as hex-encoded with \x prefix
                    if text.starts_with("\\x") {
                        match hex::decode(&text[2..]) {
                            Ok(decoded) => Value::Bytes(decoded),
                            Err(_) => Value::String(text.to_string()),
                        }
                    } else {
                        Value::Bytes(bytes.to_vec())
                    }
                }
                _ => Value::String(text.to_string()),
            }
        }
    }
}

/// Convert a tuple to column values
pub fn tuple_to_column_values(tuple: &Tuple, columns: &[Column]) -> Vec<ColumnValue> {
    tuple
        .cols
        .iter()
        .zip(columns.iter())
        .map(|(data, col)| {
            let value = tuple_data_to_value(data, col.type_id);
            ColumnValue::new(col.name.clone(), value)
        })
        .collect()
}

/// Convert columns to column definitions
pub fn columns_to_defs(columns: &[Column]) -> Vec<ColumnDef> {
    columns
        .iter()
        .map(|col| {
            let data_type = pg_type_to_data_type(col.type_id, col.type_mod);
            // Columns are nullable unless they're part of the key
            let nullable = !col.is_key();
            ColumnDef::new(col.name.clone(), data_type, nullable)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_basic_type_mapping() {
        assert_eq!(pg_type_to_data_type(pg_oid::BOOL, -1), DataType::Boolean);
        assert_eq!(pg_type_to_data_type(pg_oid::INT2, -1), DataType::Int16);
        assert_eq!(pg_type_to_data_type(pg_oid::INT4, -1), DataType::Int32);
        assert_eq!(pg_type_to_data_type(pg_oid::INT8, -1), DataType::Int64);
        assert_eq!(pg_type_to_data_type(pg_oid::FLOAT4, -1), DataType::Float32);
        assert_eq!(pg_type_to_data_type(pg_oid::FLOAT8, -1), DataType::Float64);
        assert_eq!(pg_type_to_data_type(pg_oid::TEXT, -1), DataType::Text);
        assert_eq!(pg_type_to_data_type(pg_oid::VARCHAR, -1), DataType::String);
        assert_eq!(pg_type_to_data_type(pg_oid::UUID, -1), DataType::Uuid);
        assert_eq!(pg_type_to_data_type(pg_oid::JSON, -1), DataType::Json);
        assert_eq!(pg_type_to_data_type(pg_oid::JSONB, -1), DataType::Jsonb);
    }

    #[test]
    fn test_temporal_type_mapping() {
        assert_eq!(pg_type_to_data_type(pg_oid::DATE, -1), DataType::Date);
        assert_eq!(pg_type_to_data_type(pg_oid::TIME, -1), DataType::Time);
        assert_eq!(
            pg_type_to_data_type(pg_oid::TIMESTAMP, -1),
            DataType::Timestamp
        );
        assert_eq!(
            pg_type_to_data_type(pg_oid::TIMESTAMPTZ, -1),
            DataType::TimestampTz
        );
    }

    #[test]
    fn test_numeric_with_precision() {
        // NUMERIC(10,2) would have type_mod = (10 << 16) | (2 + 4) = 655366
        let type_mod = (10 << 16) | (2 + 4);
        let dt = pg_type_to_data_type(pg_oid::NUMERIC, type_mod);
        assert_eq!(
            dt,
            DataType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn test_tuple_data_conversion() {
        // NULL
        let null_val = tuple_data_to_value(&TupleData::Null, pg_oid::INT4);
        assert!(matches!(null_val, Value::Null));

        // TOAST
        let toast_val = tuple_data_to_value(&TupleData::Toast, pg_oid::TEXT);
        assert!(matches!(toast_val, Value::Unchanged));

        // Integer
        let int_val =
            tuple_data_to_value(&TupleData::Text(Bytes::from("42")), pg_oid::INT4);
        assert!(matches!(int_val, Value::Int64(42)));

        // Float
        let float_val =
            tuple_data_to_value(&TupleData::Text(Bytes::from("3.5")), pg_oid::FLOAT8);
        match float_val {
            Value::Float64(f) => assert!((f - 3.5).abs() < 0.001),
            _ => panic!("Expected Float64"),
        }

        // Boolean true
        let bool_val =
            tuple_data_to_value(&TupleData::Text(Bytes::from("t")), pg_oid::BOOL);
        assert!(matches!(bool_val, Value::Bool(true)));

        // Boolean false
        let bool_val =
            tuple_data_to_value(&TupleData::Text(Bytes::from("f")), pg_oid::BOOL);
        assert!(matches!(bool_val, Value::Bool(false)));

        // String
        let str_val =
            tuple_data_to_value(&TupleData::Text(Bytes::from("hello")), pg_oid::TEXT);
        match str_val {
            Value::String(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_type_predicates() {
        assert!(PgTypeMapper::is_numeric(pg_oid::INT4));
        assert!(PgTypeMapper::is_numeric(pg_oid::FLOAT8));
        assert!(PgTypeMapper::is_numeric(pg_oid::NUMERIC));
        assert!(!PgTypeMapper::is_numeric(pg_oid::TEXT));

        assert!(PgTypeMapper::is_text(pg_oid::TEXT));
        assert!(PgTypeMapper::is_text(pg_oid::VARCHAR));
        assert!(!PgTypeMapper::is_text(pg_oid::INT4));

        assert!(PgTypeMapper::is_temporal(pg_oid::TIMESTAMP));
        assert!(PgTypeMapper::is_temporal(pg_oid::DATE));
        assert!(!PgTypeMapper::is_temporal(pg_oid::TEXT));

        assert!(PgTypeMapper::is_json(pg_oid::JSON));
        assert!(PgTypeMapper::is_json(pg_oid::JSONB));
        assert!(!PgTypeMapper::is_json(pg_oid::TEXT));
    }

    #[test]
    fn test_columns_to_defs() {
        let columns = vec![
            Column {
                flags: 1, // key column
                name: "id".to_string(),
                type_id: pg_oid::INT4,
                type_mod: -1,
            },
            Column {
                flags: 0, // regular column
                name: "name".to_string(),
                type_id: pg_oid::VARCHAR,
                type_mod: -1,
            },
        ];

        let defs = columns_to_defs(&columns);

        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].name, "id");
        assert_eq!(defs[0].data_type, DataType::Int32);
        assert!(!defs[0].nullable); // key column

        assert_eq!(defs[1].name, "name");
        assert_eq!(defs[1].data_type, DataType::String);
        assert!(defs[1].nullable); // non-key column
    }
}
