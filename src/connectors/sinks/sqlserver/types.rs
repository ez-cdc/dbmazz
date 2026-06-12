// Copyright 2025
// Licensed under the Elastic License v2.0

//! SQL Server target type mapping.
//!
//! Maps CDC `DataType` to SQL Server T-SQL column types for DDL generation,
//! and `Value` to parameter placeholders for parameterized MERGE queries
//! via the tiberius driver.

use crate::core::record::{DataType, Value};
use crate::core::traits::SourceColumn;

/// Maps a `SourceColumn`'s type info to the target SQL Server DDL type.
///
/// Dispatch primary on `DataType` (source-agnostic). SQL Server has no
/// equivalent of PG OID refinement, so this is a pure delegation to
/// `data_type_to_sqlserver`.
#[allow(dead_code)]
pub fn column_type(col: &SourceColumn) -> &'static str {
    data_type_to_sqlserver(&col.data_type)
}

/// Map a source-agnostic `DataType` to a SQL Server column type DDL string.
///
/// Falls back to `NVARCHAR(MAX)` for `DataType::String` and unknown types.
pub fn data_type_to_sqlserver(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BIT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INT",
        DataType::Int64 => "BIGINT",
        // u64::MAX = 18_446_744_073_709_551_615 → 20 digits. NUMERIC(20, 0)
        // fits the full unsigned range exactly. BIGINT would overflow.
        DataType::UInt64 => "NUMERIC(20, 0)",
        DataType::Float32 => "REAL",
        DataType::Float64 => "FLOAT(53)",
        DataType::Decimal {
            precision: _,
            scale: _,
        } => {
            // Return default NUMERIC(38,0); the caller is expected to
            // format the full NUMERIC(p,s) string when precision/scale
            // are known at DDL generation time.
            "NUMERIC(38, 0)"
        }
        DataType::String | DataType::Text => "NVARCHAR(MAX)",
        DataType::Bytes => "VARBINARY(MAX)",
        // SQL Server has no native JSON type; store as NVARCHAR(MAX).
        DataType::Json | DataType::Jsonb => "NVARCHAR(MAX)",
        // DATETIME2 is used instead of DATE because tiberius 0.12.3
        // cannot decode TDS type 40 (DATE/DATETIME4) in query results.
        // DATETIME2 (TDS type 42) is fully supported and the verify
        // client can roundtrip it correctly.
        DataType::Date => "DATETIME2",
        DataType::Time => "TIME",
        DataType::Timestamp => "DATETIME2",
        // Datetime2 for timestamptz too — the CDC value is always UTC epoch
        // microseconds (no timezone offset), so DATETIMEOFFSET would add a
        // redundant +00:00 that conflicts with tiberius' NaiveDateTime param
        // type (sent as datetime2, not datetimeoffset).
        DataType::TimestampTz => "DATETIME2",
        DataType::Uuid => "UNIQUEIDENTIFIER",
    }
}

/// Returns the tiberius parameter placeholder for a CDC `Value`.
///
/// SQL Server parameterized queries use `@P1`, `@P2`, … as positional
/// placeholders.  This function returns the base placeholder label (`@P`)
/// that the caller suffixes with the 1-based parameter index at SQL
/// generation time.
///
/// The placeholder does not vary by Value variant — all values use the
/// same `@P` convention.  The caller is responsible for numbering and
/// binding the actual value through tiberius.
#[allow(dead_code)]
pub fn value_to_param(_value: &Value) -> &'static str {
    "@P"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;
    use crate::core::traits::SourceColumn;

    #[test]
    fn test_column_type() {
        let col = SourceColumn {
            name: "test_col".into(),
            data_type: DataType::Int32,
            nullable: false,
            pg_type_id: None,
        };
        assert_eq!(column_type(&col), "INT");
    }

    #[test]
    fn test_common_types() {
        assert_eq!(data_type_to_sqlserver(&DataType::Boolean), "BIT");
        assert_eq!(data_type_to_sqlserver(&DataType::Int16), "SMALLINT");
        assert_eq!(data_type_to_sqlserver(&DataType::Int32), "INT");
        assert_eq!(data_type_to_sqlserver(&DataType::Int64), "BIGINT");
        assert_eq!(data_type_to_sqlserver(&DataType::UInt64), "NUMERIC(20, 0)");
        assert_eq!(data_type_to_sqlserver(&DataType::Float32), "REAL");
        assert_eq!(data_type_to_sqlserver(&DataType::Float64), "FLOAT(53)");
        assert_eq!(data_type_to_sqlserver(&DataType::String), "NVARCHAR(MAX)");
        assert_eq!(data_type_to_sqlserver(&DataType::Text), "NVARCHAR(MAX)");
        assert_eq!(data_type_to_sqlserver(&DataType::Bytes), "VARBINARY(MAX)");
        assert_eq!(data_type_to_sqlserver(&DataType::Date), "DATETIME2");
        assert_eq!(data_type_to_sqlserver(&DataType::Time), "TIME");
        assert_eq!(data_type_to_sqlserver(&DataType::Timestamp), "DATETIME2");
        assert_eq!(data_type_to_sqlserver(&DataType::TimestampTz), "DATETIME2");
        assert_eq!(data_type_to_sqlserver(&DataType::Uuid), "UNIQUEIDENTIFIER");
    }

    #[test]
    fn test_json_types() {
        assert_eq!(data_type_to_sqlserver(&DataType::Json), "NVARCHAR(MAX)");
        assert_eq!(data_type_to_sqlserver(&DataType::Jsonb), "NVARCHAR(MAX)");
    }

    #[test]
    fn test_decimal_default() {
        // The free function returns a static default; precision/scale are
        // handled at the DDL-generation layer that calls this function.
        assert_eq!(
            data_type_to_sqlserver(&DataType::Decimal {
                precision: 10,
                scale: 2
            }),
            "NUMERIC(38, 0)"
        );
    }

    #[test]
    fn test_value_to_param() {
        use crate::core::record::Value;
        assert_eq!(value_to_param(&Value::Null), "@P");
        assert_eq!(value_to_param(&Value::Int64(42)), "@P");
        assert_eq!(value_to_param(&Value::String("hello".into())), "@P");
    }
}
