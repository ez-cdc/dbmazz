// Copyright 2025
// Licensed under the Elastic License v2.0

//! MySQL replication loop — reads binlog events and pushes to pipeline.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use mysql_common::binlog::events::TableMapEvent;
use tracing::{debug, error, info, warn};

use crate::engine::replication::{LoopContext, LoopHelper, ReplicationLoop};
use crate::engine::ControlFlow;
use crate::pipeline::PipelineEvent;
use crate::source::mysql::converter::convert_to_cdc_records;
use crate::source::mysql::parser::{process_typed_event, BinlogEvent};

/// MySQL replication loop using typed binlog events.
///
/// Reads from a `mysql_async::BinlogStream` and pushes `PipelineEvent`s
/// through the pipeline sender. Runs until shutdown, stream end, or error.
pub struct MysqlReplicationLoop {
    binlog_stream: mysql_async::BinlogStream,
}

impl MysqlReplicationLoop {
    /// Create a new MySQL replication loop from a `BinlogStream`.
    pub fn new(binlog_stream: mysql_async::BinlogStream) -> Self {
        Self { binlog_stream }
    }

    /// Check whether a `BinlogEvent` targets a user-tracked table.
    ///
    /// Unfiltered event types (`Begin`, `Commit`, `Heartbeat`, `TableMap`)
    /// always pass and are forwarded unconditionally.
    fn is_tracked_table_event(
        event: &BinlogEvent,
        tracked: &HashSet<(String, String)>,
    ) -> bool {
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
                // Allow DDL events for tracked schemas
                let has_any = tracked.iter().any(|(s, _)| s == schema_name);
                if !has_any {
                    debug!(
                        "MySQL CDC: skipping DDL for untracked schema '{}'",
                        schema_name
                    );
                }
                has_any
            }
            // Begin, Commit, Heartbeat, TableMap — always pass
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
            pipeline_tx,
            mut feedback_rx,
            source_schemas,
            sink_factory,
            ..
        } = ctx;

        let mut shutdown_rx = shared_state.shutdown_tx.subscribe();
        let mut snapshot_trigger_rx = shared_state.subscribe_snapshot_trigger();
        let mut iteration = 0u64;

        // TME cache: map table_id → TableMapEvent (populated from typed events)
        let mut tme_cache: HashMap<u64, TableMapEvent<'static>> = HashMap::new();

        // Column name map from schema introspection (for reliable names)
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
                        info!("On-demand snapshot triggered (CDC_RUNNING → SNAPSHOT)");
                        let _ = shared_state.snapshot_trigger.send(false);
                        let snap_config = config.clone();
                        let snap_state = shared_state.clone();
                        let snap_factory = sink_factory.clone();
                        tokio::spawn(async move {
                            match crate::engine::snapshot::mysql::run_mysql_snapshot(
                                snap_config,
                                snap_state.clone(),
                                snap_factory,
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
                                &event, &mut tme_cache, &col_names_map
                            )?;
                            for event in &binlog_events {
                                if !Self::is_tracked_table_event(event, &tracked_tables) {
                                    continue;
                                }
                                let records = convert_to_cdc_records(event)?;
                                for record in records {
                                    let pipeline_event = PipelineEvent {
                                        lsn: 0,
                                        record,
                                    };
                                    tx.send(pipeline_event).await.map_err(|_| {
                                        anyhow::anyhow!("Pipeline channel closed")
                                    })?;
                                }
                            }
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

                // MySQL ignores checkpoint feedback (GTID is self-contained)
                Some(_) = feedback_rx.recv() => {}
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
