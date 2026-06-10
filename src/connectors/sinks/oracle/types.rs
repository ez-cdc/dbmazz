// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle sink type mapping: core::DataType → Oracle SQL types.

use crate::core::record::{DataType, Value};
use crate::core::traits::SourceColumn;

/// Check if a column is a date/time type that needs TO_TIMESTAMP wrapping.
pub fn is_date_column(col: &SourceColumn) -> bool {
    matches!(
        col.data_type,
        DataType::Date | DataType::Time | DataType::Timestamp | DataType::TimestampTz
    )
}

/// Convert a `Value` to an Oracle SQL expression for date/time columns.
/// Handles both string values (from snapshot, which casts PG values to text)
/// and Value::Timestamp (from CDC stream).
pub fn value_to_oracle_date_expr(value: &Value, col: &SourceColumn) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::String(s) => {
            // Snapshot sends dates as strings like "2024-01-15 10:30:00.123456+00"
            // Strip trailing timezone offset (e.g. +00, +00:00, -05, etc.).
            // Only strip if the string has a time component (contains a space)
            // AND the trailing +/- is AFTER the time portion (i.e. it's a tz offset,
            // not the hyphen in the date part).
            let cleaned = if s.contains(' ') {
                let s = s.trim_end();
                match s.rfind(|c: char| c == '+' || c == '-') {
                    Some(pos) if {
                        let last_space = s.rfind(' ').unwrap_or(0);
                        pos > last_space
                    } => {
                        // +/- found after the time component — likely a tz offset
                        let suffix = &s[pos..];
                        if suffix.len() > 1
                            && suffix[1..].chars().all(|c| c.is_ascii_digit() || c == ':')
                        {
                            &s[..pos]
                        } else {
                            s
                        }
                    }
                    _ => s,
                }
            } else {
                s
            }
            .trim_end_matches('.')
            .trim_end()
            .to_string();
            // Preserve fractional seconds so that
            // "2024-01-15 10:30:00.123456" keeps the ".123456" part.
            // Extract base time and optional fractional part.
            let (base_time, fractional) = match cleaned.split_once('.') {
                Some((base, frac)) => {
                    let frac_trimmed: String = frac.chars().take(6).collect();
                    let frac_trimmed = frac_trimmed.trim_end().to_string();
                    if frac_trimmed.is_empty() {
                        (base.trim_end().to_string(), None)
                    } else {
                        (base.trim_end().to_string(), Some(frac_trimmed))
                    }
                }
                None => (cleaned.trim_end().to_string(), None),
            };
            let escaped = base_time.replace('\'', "''");
            if col.data_type == DataType::Date {
                format!("TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS')", escaped)
            } else if let Some(frac) = &fractional {
                format!("TO_TIMESTAMP('{}.{}', 'YYYY-MM-DD HH24:MI:SS.FF6')", escaped, frac)
            } else {
                format!("TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS')", escaped)
            }
        }
        Value::Timestamp(ts) => {
            // CDC stream sends timestamps as epoch nanos
            // Use Euclidean division for correct negative-epoch decomposition
            let secs = ts.div_euclid(1_000_000_000);
            let subsec_nanos = ts.rem_euclid(1_000_000_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, subsec_nanos)
                .unwrap_or_default();
            if col.data_type == DataType::Date {
                format!(
                    "TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS')",
                    dt.format("%Y-%m-%d %H:%M:%S")
                )
            } else if subsec_nanos > 0 {
                let formatted = format!(
                    "{}.{:06}",
                    dt.format("%Y-%m-%d %H:%M:%S"),
                    subsec_nanos / 1000
                );
                format!(
                    "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                    formatted
                )
            } else {
                format!(
                    "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS')",
                    dt.format("%Y-%m-%d %H:%M:%S")
                )
            }
        }
        _ => value_to_oracle_expr(value),
    }
}

/// Convert a `Value` to an Oracle SQL expression, taking the column type into
/// account. Handles cross-type conversions like boolean strings to numbers.
pub fn value_to_oracle_typed_expr(value: &Value, col: &SourceColumn) -> String {
    if is_date_column(col) {
        return value_to_oracle_date_expr(value, col);
    }
    match value {
        Value::String(s) if col.data_type == DataType::Boolean => {
            // Snapshot sends booleans as "true"/"false" text
            match s.to_lowercase().as_str() {
                "true" | "t" | "yes" | "1" => "1".to_string(),
                "false" | "f" | "no" | "0" => "0".to_string(),
                _ => value_to_oracle_expr(value),
            }
        }
        _ => value_to_oracle_expr(value),
    }
}

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
            // j is already a JSON string — just escape single quotes for Oracle
            let escaped = j.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::Timestamp(ts) => {
            // ts is epoch nanos as i64 — convert to Oracle TIMESTAMP literal
            // Use Euclidean division for correct negative-epoch decomposition
            let secs = ts.div_euclid(1_000_000_000);
            let subsec_nanos = ts.rem_euclid(1_000_000_000) as u32;
            // Format as "YYYY-MM-DD HH24:MI:SS.FF6" — preserve sub-second precision.
            let dt = chrono::DateTime::from_timestamp(secs, subsec_nanos)
                .unwrap_or_default();
            if subsec_nanos > 0 {
                let formatted = format!(
                    "{}.{:06}",
                    dt.format("%Y-%m-%d %H:%M:%S"),
                    subsec_nanos / 1000
                );
                format!(
                    "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                    formatted
                )
            } else {
                format!(
                    "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS')",
                    dt.format("%Y-%m-%d %H:%M:%S")
                )
            }
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
    use crate::core::record::DataType;

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
        assert_eq!(
            value_to_oracle_expr(&Value::String("hello".into())),
            "'hello'"
        );
        assert_eq!(
            value_to_oracle_expr(&Value::String("it's".into())),
            "'it''s'"
        );
    }

    #[test]
    fn test_value_to_oracle_expr_timestamp_zero() {
        // BUG-1: subsec_nanos = 0 should produce no .FF in format mask
        let result = value_to_oracle_expr(&Value::Timestamp(0));
        assert_eq!(
            result,
            "TO_TIMESTAMP('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
        );
    }

    #[test]
    fn test_value_to_oracle_expr_negative_epoch() {
        // BUG-5: -1_000_000_001 nanos = 1969-12-31 23:59:58.999999
        let result = value_to_oracle_expr(&Value::Timestamp(-1_000_000_001));
        assert_eq!(
            result,
            "TO_TIMESTAMP('1969-12-31 23:59:58.999999', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        );
    }

    #[test]
    fn test_value_to_oracle_date_expr_string_preserves_fractional() {
        // BUG-3: snapshot string with fractional seconds and timezone —
        // fractional part should now be preserved.
        let col = SourceColumn {
            name: "ts_col".into(),
            data_type: DataType::Timestamp,
            nullable: false,
            pg_type_id: None,
        };
        let result = value_to_oracle_date_expr(
            &Value::String("2024-01-15 10:30:00.123456+00".into()),
            &col,
        );
        assert_eq!(
            result,
            "TO_TIMESTAMP('2024-01-15 10:30:00.123456', 'YYYY-MM-DD HH24:MI:SS.FF6')"
        );
    }

    #[test]
    fn test_value_to_oracle_date_expr_string_no_timezone() {
        // String without timezone or fractional seconds
        let col = SourceColumn {
            name: "ts_col".into(),
            data_type: DataType::Timestamp,
            nullable: false,
            pg_type_id: None,
        };
        let result = value_to_oracle_date_expr(
            &Value::String("2024-01-15 10:30:00".into()),
            &col,
        );
        assert_eq!(
            result,
            "TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')"
        );
    }

    #[test]
    fn test_value_to_oracle_date_expr_timestamp_no_fractional() {
        // Value::Timestamp with subsec_nanos == 0 should not produce fractional seconds
        let col = SourceColumn {
            name: "ts_col".into(),
            data_type: DataType::Timestamp,
            nullable: false,
            pg_type_id: None,
        };
        let result = value_to_oracle_date_expr(
            &Value::Timestamp(1705314600000000000), // 2024-01-15 10:30:00 UTC
            &col,
        );
        assert_eq!(
            result,
            "TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')"
        );
    }

    #[test]
    fn test_value_to_oracle_date_expr_date_type() {
        // DATE type should still work
        let col = SourceColumn {
            name: "date_col".into(),
            data_type: DataType::Date,
            nullable: false,
            pg_type_id: None,
        };
        let result = value_to_oracle_date_expr(
            &Value::String("2024-01-15".into()),
            &col,
        );
        assert_eq!(
            result,
            "TO_DATE('2024-01-15', 'YYYY-MM-DD HH24:MI:SS')"
        );
    }
}
