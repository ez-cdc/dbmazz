// Copyright 2025
// Licensed under the Elastic License v2.0

//! MySQL replication loop — reads binlog events and pushes to pipeline.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use mysql_common::binlog::events::TableMapEvent;
use tracing::{debug, error, info, warn};

use crate::core::SourcePosition;
use crate::engine::replication::{LoopContext, LoopHelper, ReplicationLoop};
use crate::engine::ControlFlow;
use crate::pipeline::PipelineEvent;
use crate::source::mysql::converter::convert_to_cdc_records;
use crate::source::mysql::gtid::GtidSet;
use crate::source::mysql::parser::{process_typed_event, BinlogEvent, ParserState};

/// MySQL replication loop — reads binlog events and pushes to pipeline.
pub struct MysqlReplicationLoop {
    binlog_stream: mysql_async::BinlogStream,
}

impl MysqlReplicationLoop {
    /// Create a new MySQL replication loop.
    pub fn new(binlog_stream: mysql_async::BinlogStream) -> Self {
        Self { binlog_stream }
    }

    /// For a CDC record (Insert/Update/Delete), check whether it mutates a
    /// row that falls inside any active snapshot chunk's PK range AND
    /// whose event GTID lies in the chunk's `(LOW, HIGH]` window. If so,
    /// register the eviction — the snapshot worker will drop the
    /// corresponding row from the chunk buffer before emitting.
    ///
    /// No-op for non-mutation records (Begin/Commit/Heartbeat/SchemaChange).
    async fn record_chunk_eviction_if_applicable(
        active_chunks: &crate::engine::snapshot::active_chunks::ActiveChunks,
        record: &crate::core::CdcRecord,
        parser_state: &crate::source::mysql::parser::ParserState,
    ) {
        use crate::core::CdcRecord;
        use crate::engine::snapshot::active_chunks::pk_in_range;

        let (table_ref, columns) = match record {
            CdcRecord::Insert { table, columns, .. } | CdcRecord::Delete { table, columns, .. } => {
                (table, columns)
            }
            CdcRecord::Update {
                table, new_columns, ..
            } => (table, new_columns),
            _ => return,
        };
        let schema = match &table_ref.schema {
            Some(s) => s.clone(),
            None => return,
        };
        let table = table_ref.name.clone();

        let snapshots = active_chunks.chunks_for_table(&schema, &table).await;
        if snapshots.is_empty() {
            return;
        }

        // Snapshot the parser's current GTID set once for this batch of chunks
        // (cheap clone; HashMap of intervals). Avoids holding the read lock
        // across the eviction loop and matches the LOW-via-consumer
        // discipline the snapshot worker uses on the other side.
        let consumer_gtid_now = parser_state
            .current_gtid_set
            .read()
            .expect("MySQL parser GTID set lock poisoned")
            .clone();

        // The "in window" check has two regimes:
        //   - high_set is None  → SELECT still in flight; every event
        //     since LOW is a candidate (it can't be after HIGH yet).
        //   - high_set is Some  → strict (LOW, HIGH] check. parser_state
        //     ⊇ LOW means we've passed LOW; not (parser_state ⊇ HIGH)
        //     means we haven't drained yet. Both must hold for the
        //     event to be inside the chunk's window.
        for snap in &snapshots {
            // Find the PK value by column name on this record. If the
            // record doesn't carry the PK column (shouldn't happen for
            // ROW format with REPLICA IDENTITY FULL but defensive), skip.
            let pk_value = match columns.iter().find(|c| c.name == snap.pk_column) {
                Some(c) => c.value.clone(),
                None => continue,
            };
            if !pk_in_range(&pk_value, &snap.pk_range) {
                continue;
            }
            let in_window = match &snap.high_set {
                None => consumer_gtid_now.is_superset_of(&snap.low_set),
                Some(high) => {
                    consumer_gtid_now.is_superset_of(&snap.low_set)
                        && !consumer_gtid_now.is_superset_of(high)
                }
            };
            if in_window {
                active_chunks.record_eviction(snap.id, &pk_value).await;
            }
        }
    }

    /// Returns `true` if the event targets a tracked table.
    ///
    /// Unfiltered types (`Begin`, `Commit`, `Heartbeat`, `TableMap`) always pass.
    fn is_tracked_table_event(event: &BinlogEvent, tracked: &HashSet<(String, String)>) -> bool {
        match event {
            BinlogEvent::Insert {
                schema_name,
                table_name,
                ..
            }
            | BinlogEvent::Update {
                schema_name,
                table_name,
                ..
            }
            | BinlogEvent::Delete {
                schema_name,
                table_name,
                ..
            } => tracked.contains(&(schema_name.clone(), table_name.clone())),
            BinlogEvent::Ddl { schema_name, .. } => {
                let has_any = tracked.iter().any(|(s, _)| s == schema_name);
                if !has_any {
                    debug!(
                        "MySQL CDC: skipping DDL for untracked schema '{}'",
                        schema_name
                    );
                }
                has_any
            }
            _ => true,
        }
    }
}

#[async_trait]
impl ReplicationLoop for MysqlReplicationLoop {
    async fn run(self: Box<Self>, ctx: LoopContext) -> Result<()> {
        let LoopContext {
            shared_state,
            config,
            state_store,
            pipeline_tx,
            mut feedback_rx,
            source_schemas,
            sink_factory,
            active_chunks,
            consumer_gtid,
            ..
        } = ctx;

        let mut shutdown_rx = shared_state.shutdown_tx.subscribe();
        let mut snapshot_trigger_rx = shared_state.subscribe_snapshot_trigger();
        let mut iteration = 0u64;

        // Table map event cache and column name map from schema introspection
        let mut tme_cache: HashMap<u64, TableMapEvent<'static>> = HashMap::new();
        let mut col_names_map: HashMap<(String, String), Vec<String>> = HashMap::new();
        let mut tracked_tables: HashSet<(String, String)> = HashSet::new();
        for s in source_schemas.iter() {
            let names: Vec<String> = s.columns.iter().map(|c| c.name.clone()).collect();
            col_names_map.insert((s.schema.clone(), s.name.clone()), names);
            tracked_tables.insert((s.schema.clone(), s.name.clone()));
        }
        info!(
            "MySQL CDC: tracking {} table(s): {:?}",
            tracked_tables.len(),
            tracked_tables
        );

        // Parser state — tracks the binlog file we're currently in and the
        // accumulated GTID-executed set. The set seeds from any prior
        // checkpoint persisted in StateStore so resumed streams continue
        // adding to a real cumulative set, not start from zero. The GTID
        // set lives behind `consumer_gtid` so snapshot workers can read
        // it for LOW/HIGH watermarks (DBLog read-only mode).
        let (initial_file, initial_set): (String, GtidSet) = match state_store
            .load_mysql_checkpoint(&format!("mysql_{}", config.source.mysql().server_id))
            .await
        {
            Ok(Some((file, _pos, gtid_str))) => {
                info!(
                    "MySQL CDC resume: starting from checkpoint file={}, gtid_executed={}",
                    file, gtid_str
                );
                (file, GtidSet::parse(&gtid_str).unwrap_or_default())
            }
            Ok(None) => {
                info!("MySQL CDC: no prior checkpoint, tracking from stream start");
                (String::new(), GtidSet::default())
            }
            Err(e) => {
                warn!(
                    "MySQL CDC: failed to load checkpoint ({}); tracking from stream start",
                    e
                );
                (String::new(), GtidSet::default())
            }
        };
        // Seed the shared consumer-GTID handle with the persisted set, then
        // hand the same Arc to ParserState. From here on, every parser write
        // also updates what snapshot workers observe — no separate channel.
        *consumer_gtid
            .write()
            .expect("consumer_gtid lock poisoned at startup") = initial_set;
        let mut parser_state = ParserState::with_shared_gtid(initial_file, consumer_gtid.clone());

        // Synthetic LSN counter — required because the generic pipeline
        // feedback channel speaks in `u64`, but MySQL positions are triples.
        // Each PipelineEvent gets a monotonically-increasing synthetic LSN,
        // and we keep a (synthetic_lsn -> SourcePosition) map so we can
        // resolve back to the real triple when the sink confirms.
        let mut next_synthetic_lsn: u64 = 1;
        let mut pending_positions: BTreeMap<u64, SourcePosition> = BTreeMap::new();
        let slot_name = format!("mysql_{}", config.source.mysql().server_id);

        let mut binlog_stream = self.binlog_stream;
        let tx = pipeline_tx;

        LoopHelper::set_cdc_stage(&shared_state).await;

        loop {
            iteration = iteration.wrapping_add(1);

            // Every 256 iterations, check state control (Pause/Stop/Draining)
            if iteration & 0xFF == 0 {
                let state = shared_state.state();
                if let Some(flow) =
                    LoopHelper::check_state_control(&state, tx.capacity(), config.flush_size)
                {
                    match flow {
                        ControlFlow::Break => break,
                        ControlFlow::Continue => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }
            }

            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received");
                        break;
                    }
                }

                Ok(()) = snapshot_trigger_rx.changed() => {
                    if *snapshot_trigger_rx.borrow() && !shared_state.is_snapshot_active() {
                        info!("On-demand snapshot triggered");
                        let _ = shared_state.snapshot_trigger.send(false);
                        let snap_config = config.clone();
                        let snap_state = shared_state.clone();
                        let snap_factory = sink_factory.clone();
                        let snap_active_chunks = active_chunks.clone();
                        tokio::spawn(async move {
                            match crate::engine::snapshot::mysql::run_mysql_snapshot(
                                snap_config,
                                snap_state.clone(),
                                snap_factory,
                                snap_active_chunks,
                            )
                            .await
                            {
                                Ok(()) => {
                                    snap_state.set_snapshot_active(false);
                                    info!("MySQL snapshot completed successfully");
                                }
                                Err(e) => {
                                    snap_state.set_snapshot_active(false);
                                    snap_state.set_snapshot_error(Some(format!("{}", e))).await;
                                    error!("MySQL snapshot worker error: {}", e);
                                }
                            }
                        });
                        info!("MySQL snapshot worker spawned");
                    }
                }

                event_res = binlog_stream.next() => {
                    match event_res {
                        Some(Ok(event)) => {
                            let binlog_events = process_typed_event(
                                &event,
                                &mut tme_cache,
                                &col_names_map,
                                &mut parser_state,
                            )?;
                            for event in &binlog_events {
                                if !Self::is_tracked_table_event(event, &tracked_tables) {
                                    continue;
                                }
                                let records = convert_to_cdc_records(event)?;
                                for record in records {
                                    // DBLog reconciliation: if this record mutates a row
                                    // that falls inside an active snapshot chunk's PK
                                    // range AND the event GTID falls in (LOW, HIGH], mark
                                    // the snapshot row as evicted so the worker drops it
                                    // before emitting.
                                    Self::record_chunk_eviction_if_applicable(
                                        &active_chunks,
                                        &record,
                                        &parser_state,
                                    )
                                    .await;

                                    let synthetic_lsn = next_synthetic_lsn;
                                    next_synthetic_lsn = next_synthetic_lsn.wrapping_add(1);
                                    if let Some(pos) = record.position().cloned() {
                                        pending_positions.insert(synthetic_lsn, pos);
                                    }
                                    let pipeline_event = PipelineEvent {
                                        lsn: synthetic_lsn,
                                        record,
                                    };
                                    tx.send(pipeline_event).await.map_err(|_| {
                                        anyhow::anyhow!("Pipeline channel closed")
                                    })?;
                                }
                            }
                            // After a batch of events, wake any snapshot worker whose
                            // chunk window has now been fully covered by this consumer.
                            // Snapshot the GTID set under a brief read lock so try_drain
                            // doesn't hold it for the duration of its registry walk.
                            let consumer_gtid_now = parser_state
                                .current_gtid_set
                                .read()
                                .expect("MySQL parser GTID set lock poisoned")
                                .clone();
                            active_chunks.try_drain(&consumer_gtid_now).await;
                        }
                        Some(Err(e)) => {
                            error!("MySQL binlog stream error: {}", e);
                            break;
                        }
                        None => {
                            warn!("MySQL binlog stream ended");
                            break;
                        }
                    }
                }

                // MySQL has no client → server position ack (binlogs are time-purged,
                // not consumer-purged), but we DO use the pipeline feedback to drive
                // the client-side checkpoint that restart correctness depends on.
                // Resolve the confirmed synthetic LSN back to the (file, pos, gtid)
                // triple from pending_positions and persist it.
                Some(confirmed_lsn) = feedback_rx.recv() => {
                    if let Some((_, position)) = pending_positions
                        .range(..=confirmed_lsn)
                        .next_back()
                        .map(|(k, v)| (*k, v.clone()))
                    {
                        if let SourcePosition::MysqlBinlog {
                            ref file,
                            position: pos,
                            ref gtid_executed,
                        } = position
                        {
                            if let Err(e) = state_store
                                .save_mysql_checkpoint(&slot_name, file, pos, gtid_executed)
                                .await
                            {
                                warn!("MySQL checkpoint save failed: {}", e);
                            } else {
                                debug!(
                                    "MySQL checkpoint saved: file={}, pos={}, gtid_executed={}",
                                    file, pos, gtid_executed
                                );
                            }
                        }
                        // Drop confirmed entries.
                        pending_positions = pending_positions.split_off(&(confirmed_lsn + 1));
                    }
                }
            }
        }

        info!("MySQL CDC shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod send_tests {
    #[test]
    fn assert_binlog_stream_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<mysql_async::BinlogStream>();
    }
}
