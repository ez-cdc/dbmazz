use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use mysql_common::binlog::events::{
    DeleteRowsEvent, DeleteRowsEventV1, EventData, GtidEvent, PartialUpdateRowsEvent,
    RowsEventData, RowsEventRows, TableMapEvent, UpdateRowsEvent, UpdateRowsEventV1,
    WriteRowsEvent, WriteRowsEventV1,
};
use mysql_common::binlog::value::BinlogValue;

use crate::core::SourcePosition;
use crate::source::mysql::gtid::GtidSet;

/// MySQL binlog event types relevant for CDC processing.
/// Populated from mysql_async's typed events — no raw byte parsing.
#[derive(Debug, Clone, Default)]
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
    #[default]
    Heartbeat,
    Ddl {
        schema_name: String,
        query: String,
    },
}

/// Mutable parser state carried across binlog events on the same stream.
///
/// `current_file` advances on real `RotateEvent`s. `current_gtid_set`
/// accumulates every `GtidEvent` in transaction order. `current_txn_gtid`
/// holds the GTID of the in-flight transaction (used to label `Commit`).
///
/// All three fields are part of the on-the-wire `MysqlBinlog` checkpoint
/// emitted with each event; restart correctness depends on them being
/// kept up to date.
///
/// `current_gtid_set` is wrapped in `Arc<RwLock<...>>` so the snapshot
/// worker can read the consumer's live GTID set when registering chunks
/// (DBLog read-only LOW/HIGH watermarks). The replication loop is the
/// single writer; the lock is `std::sync::RwLock` because
/// `process_typed_event` is synchronous and held for negligibly short
/// windows (one `format()` or one `add_gtid()` per event).
#[derive(Debug, Default, Clone)]
pub struct ParserState {
    pub current_file: String,
    pub current_gtid_set: Arc<RwLock<GtidSet>>,
    pub current_txn_gtid: Option<String>,
}

impl ParserState {
    /// Construct a fresh parser state, wrapping the initial GTID set in a new
    /// `Arc<RwLock<...>>`. Use [`with_shared_gtid`] when the loop wants to
    /// share the same Arc with snapshot workers.
    pub fn new(initial_file: String, initial_gtid_set: GtidSet) -> Self {
        Self::with_shared_gtid(initial_file, Arc::new(RwLock::new(initial_gtid_set)))
    }

    /// Construct a parser state that shares an externally-owned GTID set
    /// (typically from `LoopContext::consumer_gtid`). The replication loop
    /// uses this so the snapshot worker can observe the live GTID set
    /// without going through the loop itself.
    pub fn with_shared_gtid(
        initial_file: String,
        current_gtid_set: Arc<RwLock<GtidSet>>,
    ) -> Self {
        Self {
            current_file: initial_file,
            current_gtid_set,
            current_txn_gtid: None,
        }
    }

    /// Cheap clone of the shared GTID set Arc — for plumbing into
    /// `LoopContext::consumer_gtid` and snapshot workers.
    pub fn shared_gtid(&self) -> Arc<RwLock<GtidSet>> {
        self.current_gtid_set.clone()
    }
}

/// Process a typed binlog event from mysql_async into our BinlogEvent enum.
///
/// Required:
/// - `tme_cache`: persistent cache mapping table_id → TableMapEvent (populated by TABLE_MAP_EVENT)
/// - `col_names_map`: (schema, table) → Vec<column_name> from schema introspection
/// - `state`: mutable parser state (current binlog file, accumulated GTID set,
///   in-flight transaction GTID). Must outlive a single event so GTIDs and
///   file rotations are tracked across calls.
pub fn process_typed_event(
    event: &mysql_common::binlog::events::Event,
    tme_cache: &mut HashMap<u64, TableMapEvent<'static>>,
    col_names_map: &HashMap<(String, String), Vec<String>>,
    state: &mut ParserState,
) -> Result<Vec<BinlogEvent>> {
    let data = event
        .read_data()
        .context("Failed to read binlog event data")?
        .context("Unknown binlog event type")?;

    let header = event.header();
    let timestamp = header.timestamp();
    let log_pos = header.log_pos();

    // Helper: build the canonical MySQL checkpoint position for an event in
    // this stream. The triple (file, position, gtid_executed) is what restart
    // logic in MysqlSource::start_replication consumes. Holds a brief read
    // lock on the shared GTID set; snapshot workers reading concurrently
    // see the same `gtid_executed` snapshot the position carries.
    let make_position = |state: &ParserState| SourcePosition::MysqlBinlog {
        file: state.current_file.clone(),
        position: log_pos as u64,
        gtid_executed: state
            .current_gtid_set
            .read()
            .expect("MySQL parser GTID set lock poisoned")
            .format(),
    };

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
            let tme = tme_cache.get(&table_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL binlog protocol violation: RowsEvent for table_id {} \
                     arrived without a preceding TableMapEvent in this session. \
                     Aborting CDC to avoid emitting events with incorrect schema metadata.",
                    table_id
                )
            })?;

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

            // Update events return (before_rows, after_rows). Local helper —
            // not worth a top-level type alias just to satisfy clippy.
            #[allow(clippy::type_complexity)]
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
                        position: make_position(state),
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
                        position: make_position(state),
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
                        position: make_position(state),
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
                        position: make_position(state),
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
                        position: make_position(state),
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
                        position: make_position(state),
                    }])
                }
                _ => Ok(vec![]),
            }
        }

        EventData::GtidEvent(gtid_event) => {
            // Track the in-flight transaction GTID and merge it into the
            // executed-set running tally. Both are used for the next emitted
            // events' SourcePosition and for the Commit label. Brief write
            // lock — `add_gtid` is O(1) amortised when transactions arrive
            // monotonically (Debezium's standard streaming case).
            let gtid = format_gtid(&gtid_event);
            if let Some((uuid, txn_str)) = gtid.split_once(':') {
                if let Ok(txn) = txn_str.parse::<u64>() {
                    state
                        .current_gtid_set
                        .write()
                        .expect("MySQL parser GTID set lock poisoned")
                        .add_gtid(uuid.to_string(), txn);
                }
            }
            state.current_txn_gtid = Some(gtid.clone());
            Ok(vec![BinlogEvent::Begin { gtid, timestamp }])
        }

        EventData::XidEvent(_xid) => {
            // The Commit event labels the transaction that just finished.
            // Use the GTID parsed from the preceding GtidEvent if present.
            // Falling back to an XID-derived label is only for non-GTID setups
            // where MySQL never emits a GtidEvent — but in that mode restart
            // is already (file, position)-only.
            let gtid = state
                .current_txn_gtid
                .take()
                .unwrap_or_else(|| format!("XID:{}", timestamp));
            Ok(vec![BinlogEvent::Commit {
                gtid,
                timestamp,
                position: make_position(state),
            }])
        }

        EventData::QueryEvent(qe) => Ok(vec![BinlogEvent::Ddl {
            schema_name: qe.schema().to_string(),
            query: qe.query().to_string(),
        }]),

        EventData::HeartbeatEvent => Ok(vec![BinlogEvent::Heartbeat]),

        EventData::RotateEvent(rotate) => {
            // RotateEvent advances current_file. The TableMap cache is left
            // intact: real rotations re-emit TableMaps for active tables, and
            // mysql_async/MariaDB occasionally emit "fake" RotateEvents on
            // reconnect (see Debezium PR #4959, MariaDB KB on fake rotate).
            // Invalidating the cache here breaks those streams.
            state.current_file = rotate.name().to_string();
            Ok(vec![])
        }

        EventData::FormatDescriptionEvent(_) | EventData::StopEvent => Ok(vec![]),

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
