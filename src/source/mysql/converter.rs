use mysql_common::binlog::value::BinlogValue;
use mysql_common::value::Value as MysqlValue;

use crate::core::record::{CdcRecord, ColumnValue, DataType, TableRef, Value};
use anyhow::Result;

use super::parser::BinlogEvent;

/// Convert a parsed BinlogEvent (with typed BinlogValue rows) to CdcRecords.
pub fn convert_to_cdc_records(event: &BinlogEvent) -> Result<Vec<CdcRecord>> {
    Ok(match event {
        BinlogEvent::Insert {
            schema_name,
            table_name,
            rows,
            col_names,
            col_types,
            col_data_types,
            position,
            ..
        } => {
            if rows.is_empty() {
                vec![CdcRecord::Insert {
                    table: TableRef::new(Some(schema_name.clone()), table_name.clone()),
                    columns: vec![],
                    position: position.clone(),
                }]
            } else {
                let mut records = Vec::with_capacity(rows.len());
                for row_values in rows {
                    let cols = decode_row_values(row_values, col_names, col_types, col_data_types);
                    records.push(CdcRecord::Insert {
                        table: TableRef::new(Some(schema_name.clone()), table_name.clone()),
                        columns: cols,
                        position: position.clone(),
                    });
                }
                records
            }
        }
        BinlogEvent::Update {
            schema_name,
            table_name,
            before_rows,
            after_rows,
            col_names,
            col_types,
            col_data_types,
            position,
            ..
        } => {
            if before_rows.is_empty() && after_rows.is_empty() {
                vec![CdcRecord::Update {
                    table: TableRef::new(Some(schema_name.clone()), table_name.clone()),
                    old_columns: None,
                    new_columns: vec![],
                    position: position.clone(),
                }]
            } else {
                let mut records = Vec::with_capacity(before_rows.len().max(after_rows.len()));
                let pairs = before_rows.iter().zip(after_rows.iter());
                for (before, after) in pairs {
                    let old_cols = decode_row_values(before, col_names, col_types, col_data_types);
                    let new_cols = decode_row_values(after, col_names, col_types, col_data_types);
                    records.push(CdcRecord::Update {
                        table: TableRef::new(Some(schema_name.clone()), table_name.clone()),
                        old_columns: Some(old_cols),
                        new_columns: new_cols,
                        position: position.clone(),
                    });
                }
                records
            }
        }
        BinlogEvent::Delete {
            schema_name,
            table_name,
            rows,
            col_names,
            col_types,
            col_data_types,
            position,
            ..
        } => {
            if rows.is_empty() {
                vec![CdcRecord::Delete {
                    table: TableRef::new(Some(schema_name.clone()), table_name.clone()),
                    columns: vec![],
                    position: position.clone(),
                }]
            } else {
                let mut records = Vec::with_capacity(rows.len());
                for row_values in rows {
                    let cols = decode_row_values(row_values, col_names, col_types, col_data_types);
                    records.push(CdcRecord::Delete {
                        table: TableRef::new(Some(schema_name.clone()), table_name.clone()),
                        columns: cols,
                        position: position.clone(),
                    });
                }
                records
            }
        }
        BinlogEvent::Begin { .. } => vec![CdcRecord::Begin { xid: 0 }],
        BinlogEvent::Commit { position, .. } => vec![CdcRecord::Commit {
            xid: 0,
            position: position.clone(),
            commit_timestamp_us: 0,
        }],
        BinlogEvent::Heartbeat { position } => vec![CdcRecord::Heartbeat {
            position: position.clone(),
        }],
        BinlogEvent::TableMap { .. } => vec![],
        BinlogEvent::Ddl { .. } => vec![],
    })
}

// MySQL binlog column type codes used for col-type-aware dispatch.
// Source: `mysql_common::binlog::consts::ColumnType`.
const MYSQL_TYPE_DECIMAL: u8 = 0x00;
const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
const MYSQL_TYPE_DATETIME: u8 = 0x0c;
const MYSQL_TYPE_TIMESTAMP2: u8 = 0x11;
const MYSQL_TYPE_DATETIME2: u8 = 0x12;
const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;

/// Map a typed BinlogValue to our generic Value, dispatching on the
/// MySQL column type code (binlog wire) AND the introspected `DataType`.
///
/// `col_type` distinguishes TIMESTAMP / DATETIME / DECIMAL columns from
/// their non-temporal lookalikes at the wire level. `data_type` is the
/// authoritative source-side type from `information_schema.columns` —
/// used to route BIGINT UNSIGNED to `Value::UInt64` (and any future
/// dispatch that needs schema-level info).
fn map_binlog_value(bv: &BinlogValue<'static>, col_type: u8, data_type: &DataType) -> Value {
    match bv {
        BinlogValue::Value(val) => map_mysql_value(val, col_type, data_type),
        BinlogValue::Jsonb(val) => match serde_json::Value::try_from(val.clone()) {
            Ok(json) => Value::Json(json.to_string()),
            Err(_) => Value::Json("{}".to_string()),
        },
        BinlogValue::JsonDiff(_) => Value::Json("{}".to_string()),
    }
}

/// Map `mysql_common::Value` to our generic `Value`.
///
/// Dispatch matrix:
///
/// | MysqlValue arm     | col_type                  | Result                          |
/// |--------------------|---------------------------|---------------------------------|
/// | `Int(i)`           | TIMESTAMP / TIMESTAMP2    | `Value::Timestamp(i*1_000_000)` |
/// | `Bytes(b)`         | TIMESTAMP / TIMESTAMP2    | parse ASCII epoch → Timestamp   |
/// | `Date(...)`        | DATETIME / DATETIME2      | combine (y,m,d,h,m,s,us) → Timestamp |
/// | `Bytes(b)`         | DECIMAL / NEWDECIMAL      | UTF-8 → `Value::Decimal(s)`     |
/// | `UInt(u)`          | (any)                     | dispatch on `data_type`: `UInt64` for `DataType::UInt64`, else `Int64(u as i64)` |
/// | `Int(i)`           | (any non-temporal)        | `Value::Int64(i)`               |
/// | `Bytes(b)`         | (any non-decimal)         | `String(s)` if UTF-8, `Bytes(b)` otherwise |
/// | `Date(...)`        | non-DATETIME              | ISO string (date-only types)    |
fn map_mysql_value(val: &MysqlValue, col_type: u8, data_type: &DataType) -> Value {
    match val {
        MysqlValue::NULL => Value::Null,
        MysqlValue::Int(i) => match col_type {
            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => Value::Timestamp(*i * 1_000_000),
            _ => Value::Int64(*i),
        },
        MysqlValue::UInt(u) => match col_type {
            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => {
                #[allow(clippy::cast_possible_wrap)]
                let epoch = *u as i64;
                Value::Timestamp(epoch * 1_000_000)
            }
            _ => match data_type {
                // BIGINT UNSIGNED — preserve the full u64 range.
                DataType::UInt64 => Value::UInt64(*u),
                // Smaller unsigned types (INT/MEDIUMINT/SMALLINT/TINYINT
                // UNSIGNED) fit in i64 losslessly.
                _ =>
                {
                    #[allow(clippy::cast_possible_wrap)]
                    Value::Int64(*u as i64)
                }
            },
        },
        MysqlValue::Float(f) => Value::Float64(*f as f64),
        MysqlValue::Double(d) => Value::Float64(*d),
        MysqlValue::Bytes(b) => {
            // Empirical: mysql_common 0.32.4 in binlog mode returns
            // TIMESTAMP/TIMESTAMP2 columns as `MysqlValue::Bytes`
            // containing the ASCII epoch (e.g. b"1778368097"), not as
            // `MysqlValue::Int`. Handle both paths.
            if matches!(col_type, MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2) {
                if let Ok(s) = std::str::from_utf8(b) {
                    if let Ok(epoch) = s.parse::<i64>() {
                        return Value::Timestamp(epoch * 1_000_000);
                    }
                }
            }
            // DECIMAL columns arrive as ASCII-encoded numeric text.
            // Preserve the string shape end-to-end (PG NUMERIC, SR /
            // SF DECIMAL all accept decimal-string input via their
            // raw / Stream Load paths).
            if matches!(col_type, MYSQL_TYPE_NEWDECIMAL | MYSQL_TYPE_DECIMAL) {
                if let Ok(s) = std::str::from_utf8(b) {
                    return Value::Decimal(s.to_string());
                }
            }
            match String::from_utf8(b.clone()) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Bytes(b.clone()),
            }
        }
        MysqlValue::Date(y, m, d, hh, mm, ss, us) => {
            // DATETIME / DATETIME2 → Value::Timestamp(epoch_micros)
            // preserving microseconds. Non-DATETIME date types (DATE,
            // YEAR) fall through to ISO string for compatibility with
            // the existing snapshot path and sink-side date columns.
            if matches!(col_type, MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2) {
                if let Some(dt) = chrono::NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                    .and_then(|d| d.and_hms_micro_opt(*hh as u32, *mm as u32, *ss as u32, *us))
                {
                    return Value::Timestamp(dt.and_utc().timestamp_micros());
                }
                tracing::warn!(
                    year = y,
                    month = m,
                    day = d,
                    "MySQL DATETIME value out of chrono range; emitting NULL"
                );
                return Value::Null;
            }
            Value::String(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                y, m, d, hh, mm, ss
            ))
        }
        MysqlValue::Time(is_neg, days, h, m, s, _us) => {
            if *is_neg {
                Value::String(format!("-{} {}:{:02}:{:02}", days, h, m, s))
            } else {
                Value::String(format!("{} {}:{:02}:{:02}", days, h, m, s))
            }
        }
    }
}

/// Decode a Vec<BinlogValue> into Vec<ColumnValue> using column names
/// and the MySQL column type codes from the TableMapEvent.
///
/// All four slices (`row_values`, `col_names`, `col_types`,
/// `col_data_types`) should be the same length. When a binlog protocol
/// oddity produces shorter rows, the loop truncates to the shortest.
fn decode_row_values(
    row_values: &[BinlogValue<'static>],
    col_names: &[String],
    col_types: &[u8],
    col_data_types: &[DataType],
) -> Vec<ColumnValue> {
    let num = row_values
        .len()
        .min(col_names.len())
        .min(col_types.len())
        .min(col_data_types.len());
    let mut columns = Vec::with_capacity(num);
    for i in 0..num {
        let value = map_binlog_value(&row_values[i], col_types[i], &col_data_types[i]);
        columns.push(ColumnValue::new(col_names[i].clone(), value));
    }
    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::SourcePosition;

    fn v_int(i: i64) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Int(i))
    }

    fn v_uint(u: u64) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::UInt(u))
    }

    fn v_str(s: &str) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Bytes(s.as_bytes().to_vec()))
    }

    fn v_float(f: f32) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Float(f))
    }

    fn v_double(d: f64) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Double(d))
    }

    fn v_date(y: u16, m: u8, d: u8, hh: u8, mm: u8, ss: u8, us: u32) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Date(y, m, d, hh, mm, ss, us))
    }

    /// MySQL type code constants for tests.
    const MYSQL_TYPE_LONG: u8 = 0x03; // INT
    const MYSQL_TYPE_LONGLONG: u8 = 0x08; // BIGINT
    const MYSQL_TYPE_VARCHAR: u8 = 0x0f; // VARCHAR
    const MYSQL_TYPE_TIMESTAMP_T: u8 = 0x07;
    const MYSQL_TYPE_TIMESTAMP2_T: u8 = 0x11;
    const MYSQL_TYPE_DATETIME_T: u8 = 0x0c;
    const MYSQL_TYPE_DATETIME2_T: u8 = 0x12;
    const MYSQL_TYPE_NEWDECIMAL_T: u8 = 0xf6;
    const MYSQL_TYPE_FLOAT: u8 = 0x04;
    const MYSQL_TYPE_DOUBLE: u8 = 0x05;

    #[test]
    fn test_decode_row_values_simple() {
        let col_names = vec!["id".to_string(), "name".to_string()];
        let col_types = vec![MYSQL_TYPE_LONG, MYSQL_TYPE_VARCHAR];
        let col_data_types = vec![DataType::Int32, DataType::String];
        let row = vec![v_int(42), v_str("hi")];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].value, Value::Int64(42));
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].value, Value::String("hi".to_string()));
    }

    #[test]
    fn test_decode_row_values_empty() {
        let cols = decode_row_values(&[], &[], &[], &[]);
        assert!(cols.is_empty());
    }

    #[test]
    fn test_bigint_unsigned_emits_uint64() {
        // BIGINT UNSIGNED at the upper boundary — must NOT wrap into
        // negative i64 territory. The DataType::UInt64 hint routes
        // through `Value::UInt64`.
        let col_names = vec!["id".to_string()];
        let col_types = vec![MYSQL_TYPE_LONGLONG];
        let col_data_types = vec![DataType::UInt64];
        let row = vec![v_uint(u64::MAX)];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols[0].value, Value::UInt64(u64::MAX));
    }

    #[test]
    fn test_bigint_signed_via_uint_path_stays_int64() {
        // Signed BIGINT — the value still arrives via UInt path for
        // positive numbers; DataType::Int64 routes through Value::Int64.
        let col_names = vec!["id".to_string()];
        let col_types = vec![MYSQL_TYPE_LONGLONG];
        let col_data_types = vec![DataType::Int64];
        let row = vec![v_uint(42)];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols[0].value, Value::Int64(42));
    }

    #[test]
    fn test_timestamp_emits_value_timestamp_micros() {
        // 1778368097 epoch seconds → 1778368097 * 1_000_000 micros.
        let col_names = vec!["created_at".to_string()];
        let col_types = vec![MYSQL_TYPE_TIMESTAMP_T];
        let col_data_types = vec![DataType::TimestampTz];
        let row = vec![v_int(1778368097)];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols[0].value, Value::Timestamp(1778368097 * 1_000_000));
    }

    #[test]
    fn test_timestamp_bytes_path_emits_micros() {
        // mysql_common returns TIMESTAMP2 as ASCII epoch bytes.
        let col_names = vec!["created_at".to_string()];
        let col_types = vec![MYSQL_TYPE_TIMESTAMP2_T];
        let col_data_types = vec![DataType::TimestampTz];
        let row = vec![BinlogValue::Value(MysqlValue::Bytes(
            b"1778368097".to_vec(),
        ))];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols[0].value, Value::Timestamp(1778368097 * 1_000_000));
    }

    #[test]
    fn test_datetime_preserves_microseconds() {
        // 2026-05-09 14:30:00.123456 UTC
        let col_names = vec!["updated_at".to_string()];
        let col_types = vec![MYSQL_TYPE_DATETIME_T];
        let col_data_types = vec![DataType::Timestamp];
        let row = vec![v_date(2026, 5, 9, 14, 30, 0, 123456)];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        match &cols[0].value {
            Value::Timestamp(micros) => {
                // Verify whole second + microsecond fraction land
                // separately — the previous code dropped `us` entirely.
                assert_eq!(micros % 1_000_000, 123456);
            }
            other => panic!("expected Value::Timestamp, got {:?}", other),
        }
    }

    #[test]
    fn test_datetime2_path() {
        let col_names = vec!["ts".to_string()];
        let col_types = vec![MYSQL_TYPE_DATETIME2_T];
        let col_data_types = vec![DataType::Timestamp];
        let row = vec![v_date(2026, 1, 1, 0, 0, 0, 0)];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert!(matches!(cols[0].value, Value::Timestamp(_)));
    }

    #[test]
    fn test_decimal_emits_value_decimal_string() {
        // DECIMAL arrives as ASCII bytes; must land as Value::Decimal
        // not Value::String so sinks dispatch correctly.
        let col_names = vec!["amount".to_string()];
        let col_types = vec![MYSQL_TYPE_NEWDECIMAL_T];
        let col_data_types = vec![DataType::Decimal {
            precision: 10,
            scale: 2,
        }];
        let row = vec![BinlogValue::Value(MysqlValue::Bytes(b"123.45".to_vec()))];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols[0].value, Value::Decimal("123.45".to_string()));
    }

    #[test]
    fn test_int_for_plain_int_column_stays_int() {
        let col_names = vec!["count".to_string()];
        let col_types = vec![MYSQL_TYPE_LONG];
        let col_data_types = vec![DataType::Int32];
        let row = vec![v_int(1778368097)];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols[0].value, Value::Int64(1778368097));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_decode_row_values_float_double() {
        let col_names = vec!["f".to_string(), "d".to_string()];
        let col_types = vec![MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE];
        let col_data_types = vec![DataType::Float32, DataType::Float64];
        let row = vec![v_float(3.14), v_double(2.71828)];
        let cols = decode_row_values(&row, &col_names, &col_types, &col_data_types);
        assert_eq!(cols.len(), 2);
        match cols[0].value {
            Value::Float64(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!("expected float"),
        }
        match cols[1].value {
            Value::Float64(v) => assert!((v - 2.71828).abs() < 0.001),
            _ => panic!("expected float"),
        }
    }

    #[test]
    fn test_convert_insert_event() {
        let event = BinlogEvent::Insert {
            table_id: 1,
            schema_name: "test".to_string(),
            table_name: "users".to_string(),
            rows: vec![vec![v_int(42), v_str("hi")]],
            col_names: vec!["id".to_string(), "name".to_string()],
            col_types: vec![3, 15],
            col_data_types: vec![DataType::Int32, DataType::String],
            position: SourcePosition::GtidSet("1".to_string()),
        };

        let records = convert_to_cdc_records(&event).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            CdcRecord::Insert { table, columns, .. } => {
                assert_eq!(table.qualified_name(), "test.users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].value, Value::Int64(42));
                assert_eq!(columns[1].value, Value::String("hi".to_string()));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_convert_update_event() {
        let event = BinlogEvent::Update {
            table_id: 1,
            schema_name: "test".to_string(),
            table_name: "t".to_string(),
            before_rows: vec![vec![v_int(10)]],
            after_rows: vec![vec![v_int(20)]],
            col_names: vec!["id".to_string()],
            col_types: vec![3],
            col_data_types: vec![DataType::Int32],
            position: SourcePosition::GtidSet("1".to_string()),
        };

        let records = convert_to_cdc_records(&event).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            CdcRecord::Update {
                old_columns,
                new_columns,
                ..
            } => {
                let old = old_columns.as_ref().unwrap();
                assert_eq!(old[0].value, Value::Int64(10));
                assert_eq!(new_columns[0].value, Value::Int64(20));
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_convert_delete_event() {
        let event = BinlogEvent::Delete {
            table_id: 1,
            schema_name: "test".to_string(),
            table_name: "t".to_string(),
            rows: vec![vec![v_int(99)]],
            col_names: vec!["id".to_string()],
            col_types: vec![3],
            col_data_types: vec![DataType::Int32],
            position: SourcePosition::GtidSet("1".to_string()),
        };

        let records = convert_to_cdc_records(&event).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            CdcRecord::Delete { columns, .. } => {
                assert_eq!(columns[0].value, Value::Int64(99));
            }
            _ => panic!("Expected Delete"),
        }
    }
}
