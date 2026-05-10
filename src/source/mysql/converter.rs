use mysql_common::binlog::value::BinlogValue;
use mysql_common::value::Value as MysqlValue;

use crate::core::record::{CdcRecord, ColumnValue, TableRef, Value};
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
                    let cols = decode_row_values(row_values, col_names, col_types);
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
                    let old_cols = decode_row_values(before, col_names, col_types);
                    let new_cols = decode_row_values(after, col_names, col_types);
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
                    let cols = decode_row_values(row_values, col_names, col_types);
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

/// Relevant MySQL column type codes used to dispatch value formatting
/// for types where the wire representation alone is ambiguous (e.g.
/// `TIMESTAMP` arrives as an `Int(epoch_seconds)` indistinguishable
/// from a plain `INT` column unless we consult the column type code
/// from the preceding TableMapEvent).
///
/// Source: `mysql_common::binlog::consts::ColumnType`. Replicated as
/// `u8` constants here so we don't drag the enum through the value-
/// mapping hot path.
const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
const MYSQL_TYPE_TIMESTAMP2: u8 = 0x11;

/// Map a typed BinlogValue to our generic Value, dispatching on the
/// MySQL column type code where the wire representation is ambiguous.
///
/// `col_type` comes from the preceding TableMapEvent (parser.rs
/// extract_col_types). For columns we couldn't resolve a type for
/// (UNKNOWN_COLUMN_TYPE sentinel), the dispatch falls through to the
/// default arms.
fn map_binlog_value(bv: &BinlogValue<'static>, col_type: u8) -> Value {
    match bv {
        BinlogValue::Value(val) => map_mysql_value(val, col_type),
        BinlogValue::Jsonb(val) => match serde_json::Value::try_from(val.clone()) {
            Ok(json) => Value::Json(json.to_string()),
            Err(_) => Value::Json("{}".to_string()),
        },
        BinlogValue::JsonDiff(_) => Value::Json("{}".to_string()),
    }
}

/// Map `mysql_common::Value` to our generic `Value`, dispatching on the
/// MySQL column type code for ambiguous wire representations.
///
/// **TIMESTAMP / TIMESTAMP2** arrive as `MysqlValue::Int(epoch_seconds)`
/// — indistinguishable from a plain `INT` column at the wire level. We
/// consult `col_type` to format them as ISO-8601 strings the downstream
/// PG sink can cast to `TIMESTAMP WITH TIME ZONE`. Without this, every
/// row containing a TIMESTAMP column fails the sink's MERGE cast and
/// nothing lands in the target.
///
/// **DATETIME** already arrives as a structured `MysqlValue::Date` and
/// is formatted independently of `col_type`.
///
/// This is the minimum-viable temporal fidelity for MySQL beta. The
/// proper fix (`Value::Timestamp(i64 micros)` with `tz_aware`) is
/// scoped to `mysql-cdc-beta-to-ga`. Today's String-encoded ISO is
/// forward-compatible: the upcoming sink-side typed binding will
/// recognise both shapes during the transition.
fn map_mysql_value(val: &MysqlValue, col_type: u8) -> Value {
    match val {
        MysqlValue::NULL => Value::Null,
        MysqlValue::Int(i) => match col_type {
            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => format_epoch_as_iso(*i),
            _ => Value::Int64(*i),
        },
        MysqlValue::UInt(u) => match col_type {
            MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => format_epoch_as_iso(*u as i64),
            _ => Value::Int64(*u as i64),
        },
        MysqlValue::Float(f) => Value::Float64(*f as f64),
        MysqlValue::Double(d) => Value::Float64(*d),
        MysqlValue::Bytes(b) => {
            // Empirical observation: mysql_common 0.32.4 in binlog mode
            // returns TIMESTAMP/TIMESTAMP2 columns as `MysqlValue::Bytes`
            // containing the ASCII epoch (e.g. b"1778368097"), not as
            // `MysqlValue::Int`. Catch that path so the downstream sink
            // sees an ISO timestamp string instead of the raw epoch.
            if matches!(col_type, MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2) {
                if let Ok(s) = std::str::from_utf8(b) {
                    if let Ok(epoch) = s.parse::<i64>() {
                        return format_epoch_as_iso(epoch);
                    }
                }
            }
            match String::from_utf8(b.clone()) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Bytes(b.clone()),
            }
        }
        MysqlValue::Date(y, m, d, hh, mm, ss, _us) => Value::String(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            y, m, d, hh, mm, ss
        )),
        MysqlValue::Time(is_neg, days, h, m, s, _us) => {
            if *is_neg {
                Value::String(format!("-{} {}:{:02}:{:02}", days, h, m, s))
            } else {
                Value::String(format!("{} {}:{:02}:{:02}", days, h, m, s))
            }
        }
    }
}

/// Format a unix epoch (seconds) as an ISO-8601 UTC timestamp string the
/// PG / Snowflake / StarRocks sinks can cast to a timestamp column.
///
/// MySQL TIMESTAMP is always stored UTC internally regardless of the
/// session timezone, so `from_timestamp` with UTC is the correct mapping.
fn format_epoch_as_iso(epoch_seconds: i64) -> Value {
    use chrono::DateTime;
    match DateTime::from_timestamp(epoch_seconds, 0) {
        Some(dt) => Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
        // chrono only fails for values outside MIN..=MAX (~262k years
        // before/after epoch). Surface as Null with a structured log
        // rather than panic; the row still flows.
        None => {
            tracing::warn!(
                epoch_seconds,
                "MySQL TIMESTAMP value outside chrono representable range; emitting NULL"
            );
            Value::Null
        }
    }
}

/// Decode a Vec<BinlogValue> into Vec<ColumnValue> using column names
/// and the MySQL column type codes from the TableMapEvent.
///
/// `col_types` MUST be the same length as `col_names` (both come from
/// schema introspection / TableMapEvent in lockstep). When a row has
/// fewer values than expected (binlog protocol oddity), the loop
/// truncates to the shortest of the three slices.
fn decode_row_values(
    row_values: &[BinlogValue<'static>],
    col_names: &[String],
    col_types: &[u8],
) -> Vec<ColumnValue> {
    let num = row_values.len().min(col_names.len()).min(col_types.len());
    let mut columns = Vec::with_capacity(num);
    for i in 0..num {
        let value = map_binlog_value(&row_values[i], col_types[i]);
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

    fn v_str(s: &str) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Bytes(s.as_bytes().to_vec()))
    }

    fn v_float(f: f32) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Float(f))
    }

    fn v_double(d: f64) -> BinlogValue<'static> {
        BinlogValue::Value(MysqlValue::Double(d))
    }

    /// MySQL type code constants for tests (mirror the private consts).
    const MYSQL_TYPE_LONG: u8 = 0x03; // INT
    const MYSQL_TYPE_VARCHAR: u8 = 0x0f; // VARCHAR
    const MYSQL_TYPE_TIMESTAMP_T: u8 = 0x07;
    const MYSQL_TYPE_TIMESTAMP2_T: u8 = 0x11;
    const MYSQL_TYPE_FLOAT: u8 = 0x04;
    const MYSQL_TYPE_DOUBLE: u8 = 0x05;

    #[test]
    fn test_decode_row_values_simple() {
        let col_names = vec!["id".to_string(), "name".to_string()];
        let col_types = vec![MYSQL_TYPE_LONG, MYSQL_TYPE_VARCHAR];
        let row = vec![v_int(42), v_str("hi")];
        let cols = decode_row_values(&row, &col_names, &col_types);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].value, Value::Int64(42));
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].value, Value::String("hi".to_string()));
    }

    #[test]
    fn test_decode_row_values_empty() {
        let cols = decode_row_values(&[], &[], &[]);
        assert!(cols.is_empty());
    }

    #[test]
    fn test_bytes_for_timestamp_column_emits_iso() {
        // mysql_common 0.32.4 in binlog mode returns TIMESTAMP as
        // ASCII-epoch bytes, not as Int. Catch that path too.
        let col_names = vec!["created_at".to_string()];
        let col_types = vec![MYSQL_TYPE_TIMESTAMP_T];
        let row = vec![BinlogValue::Value(MysqlValue::Bytes(
            b"1778368097".to_vec(),
        ))];
        let cols = decode_row_values(&row, &col_names, &col_types);
        assert_eq!(
            cols[0].value,
            Value::String("2026-05-09 23:08:17".to_string())
        );
    }

    #[test]
    fn test_int_for_timestamp_column_emits_iso() {
        // 1778368097 = 2026-05-09 23:08:17 UTC
        let col_names = vec!["created_at".to_string()];
        let col_types = vec![MYSQL_TYPE_TIMESTAMP_T];
        let row = vec![v_int(1778368097)];
        let cols = decode_row_values(&row, &col_names, &col_types);
        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].value,
            Value::String("2026-05-09 23:08:17".to_string())
        );
    }

    #[test]
    fn test_int_for_timestamp2_column_emits_iso() {
        let col_names = vec!["updated_at".to_string()];
        let col_types = vec![MYSQL_TYPE_TIMESTAMP2_T];
        let row = vec![v_int(0)]; // 1970-01-01 00:00:00 UTC
        let cols = decode_row_values(&row, &col_names, &col_types);
        assert_eq!(
            cols[0].value,
            Value::String("1970-01-01 00:00:00".to_string())
        );
    }

    #[test]
    fn test_int_for_plain_int_column_stays_int() {
        // Same Int(value) payload, but col_type is INT — must NOT
        // be reinterpreted as a timestamp.
        let col_names = vec!["count".to_string()];
        let col_types = vec![MYSQL_TYPE_LONG];
        let row = vec![v_int(1778368097)];
        let cols = decode_row_values(&row, &col_names, &col_types);
        assert_eq!(cols[0].value, Value::Int64(1778368097));
    }

    #[test]
    #[allow(clippy::approx_constant)] // arbitrary test values, not π/e
    fn test_decode_row_values_float_double() {
        let col_names = vec!["f".to_string(), "d".to_string()];
        let col_types = vec![MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE];
        let row = vec![v_float(3.14), v_double(2.71828)];
        let cols = decode_row_values(&row, &col_names, &col_types);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "f");
        assert!(
            (match &cols[0].value {
                Value::Float64(v) => *v,
                _ => 0.0,
            } - 3.14)
                .abs()
                < 0.001
        );
        assert_eq!(cols[1].name, "d");
        assert!(
            (match &cols[1].value {
                Value::Float64(v) => *v,
                _ => 0.0,
            } - 2.71828)
                .abs()
                < 0.001
        );
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
            position: SourcePosition::GtidSet("1".to_string()),
        };

        let records = convert_to_cdc_records(&event).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            CdcRecord::Insert { table, columns, .. } => {
                assert_eq!(table.qualified_name(), "test.users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].value, Value::Int64(42));
                assert_eq!(columns[1].name, "name");
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
                let new = new_columns;
                assert_eq!(old.len(), 1);
                assert_eq!(old[0].value, Value::Int64(10));
                assert_eq!(new.len(), 1);
                assert_eq!(new[0].value, Value::Int64(20));
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
            position: SourcePosition::GtidSet("1".to_string()),
        };

        let records = convert_to_cdc_records(&event).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            CdcRecord::Delete { columns, .. } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].value, Value::Int64(99));
            }
            _ => panic!("Expected Delete"),
        }
    }
}
