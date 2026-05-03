use std::collections::HashMap;

use anyhow::{Context, Result};
use mysql_common::binlog::events::{
    DeleteRowsEvent, DeleteRowsEventV1, EventData, GtidEvent, PartialUpdateRowsEvent,
    RowsEventData, RowsEventRows, TableMapEvent, UpdateRowsEvent, UpdateRowsEventV1,
    WriteRowsEvent, WriteRowsEventV1,
};
use mysql_common::binlog::value::BinlogValue;

use crate::core::SourcePosition;

/// MySQL binlog event types relevant for CDC processing.
/// Populated from mysql_async's typed events — no raw byte parsing.
#[derive(Debug, Clone)]
pub enum BinlogEvent {
    Begin {
        gtid: String,
        timestamp: u32,
    },
    Commit {
        gtid: String,
        timestamp: u32,
        position: SourcePosition,
    },
    Insert {
        table_id: u64,
        schema_name: String,
        table_name: String,
        rows: Vec<Vec<BinlogValue<'static>>>,
        col_names: Vec<String>,
        col_types: Vec<u8>,
        position: SourcePosition,
    },
    Update {
        table_id: u64,
        schema_name: String,
        table_name: String,
        before_rows: Vec<Vec<BinlogValue<'static>>>,
        after_rows: Vec<Vec<BinlogValue<'static>>>,
        col_names: Vec<String>,
        col_types: Vec<u8>,
        position: SourcePosition,
    },
    Delete {
        table_id: u64,
        schema_name: String,
        table_name: String,
        rows: Vec<Vec<BinlogValue<'static>>>,
        col_names: Vec<String>,
        col_types: Vec<u8>,
        position: SourcePosition,
    },
    TableMap {
        table_id: u64,
        col_types: Vec<u8>,
    },
    Heartbeat,
    Ddl {
        schema_name: String,
        query: String,
    },
}

impl Default for BinlogEvent {
    fn default() -> Self {
        Self::Heartbeat
    }
}

/// Process a typed binlog event from mysql_async into our BinlogEvent enum.
///
/// Required:
/// - `tme_cache`: persistent cache mapping table_id → TableMapEvent (populated by TABLE_MAP_EVENT)
/// - `col_names_map`: (schema, table) → Vec<column_name> from schema introspection
pub fn process_typed_event(
    event: &mysql_common::binlog::events::Event,
    tme_cache: &mut HashMap<u64, TableMapEvent<'static>>,
    col_names_map: &HashMap<(String, String), Vec<String>>,
) -> Result<Vec<BinlogEvent>> {
    let data = event
        .read_data()
        .context("Failed to read binlog event data")?
        .context("Unknown binlog event type")?;

    let header = event.header();
    let timestamp = header.timestamp();
    let log_pos = header.log_pos();

    match data {
        EventData::TableMapEvent(tme) => {
            tme_cache.insert(tme.table_id(), tme.clone().into_owned());
            Ok(vec![BinlogEvent::TableMap {
                table_id: tme.table_id(),
                col_types: extract_col_types(&tme),
            }])
        }

        EventData::RowsEvent(rows_data) => {
            let table_id = rows_data.table_id();
            let tme = tme_cache
                .get(&table_id)
                .unwrap_or_else(|| panic!("No TableMapEvent found for table_id {}", table_id));

            let schema = tme.database_name().to_string();
            let table = tme.table_name().to_string();

            let col_types = extract_col_types(tme);
            let col_names = col_names_map
                .get(&(schema.clone(), table.clone()))
                .cloned()
                .unwrap_or_else(|| (0..col_types.len()).map(|i| format!("@{}", i)).collect());

            /// Helper: extract rows from any rows event that has `rows()`.
            fn collect_after<R>(
                re: &R,
                tme: &TableMapEvent,
            ) -> Result<Vec<Vec<BinlogValue<'static>>>>
            where
                R: RowsEventLike,
            {
                let mut rows = Vec::new();
                for row_result in re.rows(tme) {
                    let (_before, after) = row_result?;
                    if let Some(row) = after {
                        rows.push(row.unwrap());
                    }
                }
                Ok(rows)
            }

            fn collect_before_after<R>(
                re: &R,
                tme: &TableMapEvent,
            ) -> Result<(
                Vec<Vec<BinlogValue<'static>>>,
                Vec<Vec<BinlogValue<'static>>>,
            )>
            where
                R: RowsEventLike,
            {
                let mut before = Vec::new();
                let mut after = Vec::new();
                for row_result in re.rows(tme) {
                    let (b, a) = row_result?;
                    if let Some(row) = b {
                        before.push(row.unwrap());
                    }
                    if let Some(row) = a {
                        after.push(row.unwrap());
                    }
                }
                Ok((before, after))
            }

            fn collect_before<R>(
                re: &R,
                tme: &TableMapEvent,
            ) -> Result<Vec<Vec<BinlogValue<'static>>>>
            where
                R: RowsEventLike,
            {
                let mut rows = Vec::new();
                for row_result in re.rows(tme) {
                    let (before, _after) = row_result?;
                    if let Some(row) = before {
                        rows.push(row.unwrap());
                    }
                }
                Ok(rows)
            }

            match rows_data {
                RowsEventData::WriteRowsEvent(ref re) => {
                    let rows = collect_after(re, tme)?;
                    Ok(vec![BinlogEvent::Insert {
                        table_id,
                        schema_name: schema,
                        table_name: table,
                        rows,
                        col_names,
                        col_types,
                        position: SourcePosition::GtidSet(format!("{:X}", log_pos)),
                    }])
                }
                RowsEventData::WriteRowsEventV1(ref re) => {
                    let rows = collect_after(re, tme)?;
                    Ok(vec![BinlogEvent::Insert {
                        table_id,
                        schema_name: schema,
                        table_name: table,
                        rows,
                        col_names,
                        col_types,
                        position: SourcePosition::GtidSet(format!("{:X}", log_pos)),
                    }])
                }
                RowsEventData::UpdateRowsEvent(ref re) => {
                    let (before_rows, after_rows) = collect_before_after(re, tme)?;
                    Ok(vec![BinlogEvent::Update {
                        table_id,
                        schema_name: schema,
                        table_name: table,
                        before_rows,
                        after_rows,
                        col_names,
                        col_types,
                        position: SourcePosition::GtidSet(format!("{:X}", log_pos)),
                    }])
                }
                RowsEventData::UpdateRowsEventV1(ref re) => {
                    let (before_rows, after_rows) = collect_before_after(re, tme)?;
                    Ok(vec![BinlogEvent::Update {
                        table_id,
                        schema_name: schema,
                        table_name: table,
                        before_rows,
                        after_rows,
                        col_names,
                        col_types,
                        position: SourcePosition::GtidSet(format!("{:X}", log_pos)),
                    }])
                }
                RowsEventData::DeleteRowsEvent(ref re) => {
                    let rows = collect_before(re, tme)?;
                    Ok(vec![BinlogEvent::Delete {
                        table_id,
                        schema_name: schema,
                        table_name: table,
                        rows,
                        col_names,
                        col_types,
                        position: SourcePosition::GtidSet(format!("{:X}", log_pos)),
                    }])
                }
                RowsEventData::DeleteRowsEventV1(ref re) => {
                    let rows = collect_before(re, tme)?;
                    Ok(vec![BinlogEvent::Delete {
                        table_id,
                        schema_name: schema,
                        table_name: table,
                        rows,
                        col_names,
                        col_types,
                        position: SourcePosition::GtidSet(format!("{:X}", log_pos)),
                    }])
                }
                _ => Ok(vec![]),
            }
        }

        EventData::GtidEvent(gtid_event) => {
            let gtid = format_gtid(&gtid_event);
            Ok(vec![BinlogEvent::Begin { gtid, timestamp }])
        }

        EventData::XidEvent(_xid) => Ok(vec![BinlogEvent::Commit {
            gtid: format!("XID:{}", timestamp),
            timestamp,
            position: SourcePosition::GtidSet(format!("{:X}", log_pos)),
        }]),

        EventData::QueryEvent(qe) => Ok(vec![BinlogEvent::Ddl {
            schema_name: qe.schema().to_string(),
            query: qe.query().to_string(),
        }]),

        EventData::HeartbeatEvent => Ok(vec![BinlogEvent::Heartbeat]),

        EventData::RotateEvent(_) | EventData::FormatDescriptionEvent(_) | EventData::StopEvent => {
            Ok(vec![])
        }

        _ => Ok(vec![]),
    }
}

/// Extract raw column type codes from a TableMapEvent.
fn extract_col_types(tme: &TableMapEvent<'_>) -> Vec<u8> {
    (0..tme.columns_count() as usize)
        .filter_map(|i| tme.get_raw_column_type(i).ok().flatten())
        .map(|ct| ct as u8)
        .collect()
}

/// Abstraction over rows events that can produce `RowsEventRows`.
trait RowsEventLike {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a>;
}

impl RowsEventLike for WriteRowsEvent<'_> {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        self.rows(table_map_event)
    }
}
impl RowsEventLike for WriteRowsEventV1<'_> {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        self.rows(table_map_event)
    }
}
impl RowsEventLike for UpdateRowsEvent<'_> {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        self.rows(table_map_event)
    }
}
impl RowsEventLike for UpdateRowsEventV1<'_> {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        self.rows(table_map_event)
    }
}
impl RowsEventLike for DeleteRowsEvent<'_> {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        self.rows(table_map_event)
    }
}
impl RowsEventLike for DeleteRowsEventV1<'_> {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        self.rows(table_map_event)
    }
}
impl RowsEventLike for PartialUpdateRowsEvent<'_> {
    fn rows<'a>(&'a self, table_map_event: &'a TableMapEvent<'a>) -> RowsEventRows<'a> {
        self.rows(table_map_event)
    }
}

/// Format a GTID from a GtidEvent.
fn format_gtid(event: &GtidEvent) -> String {
    let sid = event.sid();
    let gno = event.gno();
    format!(
        "{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-\
         {:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}:{}",
        sid[0],
        sid[1],
        sid[2],
        sid[3],
        sid[4],
        sid[5],
        sid[6],
        sid[7],
        sid[8],
        sid[9],
        sid[10],
        sid[11],
        sid[12],
        sid[13],
        sid[14],
        sid[15],
        gno
    )
}
