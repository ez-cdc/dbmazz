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
                    let cols = decode_row_values(row_values, col_names);
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
                    let old_cols = decode_row_values(before, col_names);
                    let new_cols = decode_row_values(after, col_names);
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
                    let cols = decode_row_values(row_values, col_names);
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

/// Map a typed BinlogValue to our generic Value.
fn map_binlog_value(bv: &BinlogValue<'static>) -> Value {
    match bv {
        BinlogValue::Value(val) => map_mysql_value(val),
        BinlogValue::Jsonb(val) => match serde_json::Value::try_from(val.clone()) {
            Ok(json) => Value::Json(json.to_string()),
            Err(_) => Value::Json("{}".to_string()),
        },
        BinlogValue::JsonDiff(_) => Value::Json("{}".to_string()),
    }
}

/// Map mysql_common::Value to our generic Value.
fn map_mysql_value(val: &MysqlValue) -> Value {
    match val {
        MysqlValue::NULL => Value::Null,
        MysqlValue::Int(i) => Value::Int64(*i),
        MysqlValue::UInt(u) => Value::Int64(*u as i64),
        MysqlValue::Float(f) => Value::Float64(*f as f64),
        MysqlValue::Double(d) => Value::Float64(*d),
        MysqlValue::Bytes(b) => match String::from_utf8(b.clone()) {
            Ok(s) => Value::String(s),
            Err(_) => Value::Bytes(b.clone()),
        },
        MysqlValue::Date(y, m, d, hh, mm, ss, _us) => Value::String(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            y, m, d, hh, mm, ss
        )),
        // MySQL TIMESTAMP is stored as Int(i64) in binlog (epoch seconds)
        // MySQL TIME is stored as Bytes or Int
        MysqlValue::Time(is_neg, days, h, m, s, _us) => {
            if *is_neg {
                Value::String(format!("-{} {}:{:02}:{:02}", days, h, m, s))
            } else {
                Value::String(format!("{} {}:{:02}:{:02}", days, h, m, s))
            }
        }
    }
}

/// Decode a Vec<BinlogValue> into Vec<ColumnValue> using column names.
fn decode_row_values(
    row_values: &[BinlogValue<'static>],
    col_names: &[String],
) -> Vec<ColumnValue> {
    let num = row_values.len().min(col_names.len());
    let mut columns = Vec::with_capacity(num);
    for i in 0..num {
        let value = map_binlog_value(&row_values[i]);
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

    #[test]
    fn test_decode_row_values_simple() {
        let col_names = vec!["id".to_string(), "name".to_string()];
        let row = vec![v_int(42), v_str("hi")];
        let cols = decode_row_values(&row, &col_names);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].value, Value::Int64(42));
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].value, Value::String("hi".to_string()));
    }

    #[test]
    fn test_decode_row_values_empty() {
        let cols = decode_row_values(&[], &[]);
        assert!(cols.is_empty());
    }

    #[test]
    #[allow(clippy::approx_constant)] // arbitrary test values, not π/e
    fn test_decode_row_values_float_double() {
        let col_names = vec!["f".to_string(), "d".to_string()];
        let row = vec![v_float(3.14), v_double(2.71828)];
        let cols = decode_row_values(&row, &col_names);
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
