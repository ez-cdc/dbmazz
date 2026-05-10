// Copyright 2025
// Licensed under the Elastic License v2.0

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::control::state::{CdcState, SharedState};
use crate::core::ReplicationStream;
use crate::engine::replication::{LoopContext, LoopHelper, ReplicationLoop};
use crate::engine::ControlFlow;
use crate::pipeline::schema_cache::SchemaCache;
use crate::pipeline::PipelineEvent;
use crate::replication::{handle_xlog_data, parse_replication_message, WalMessage};
use crate::source::postgres::build_standby_status_update;
use crate::state_store::StateStore;

/// PostgreSQL replication loop.
///
/// Owns the replication stream and schema cache, and runs the main CDC event
/// loop: reading WAL messages from the stream, handling KeepAlive replies,
/// dispatching checkpoint feedback, and coordinating with the pipeline.
pub struct PgReplicationLoop {
    /// Raw replication stream (pgoutput protocol over CopyBothDuplex)
    pub replication_stream: Box<dyn ReplicationStream>,
    /// Schema cache — updated on Relation messages; used by the converter
    /// to resolve column names / types when building CdcRecord.
    pub schema_cache: SchemaCache,
}

impl PgReplicationLoop {
    pub fn new(stream: Box<dyn ReplicationStream>, schema_cache: SchemaCache) -> Self {
        Self {
            replication_stream: stream,
            schema_cache,
        }
    }

    /// Process a single WAL message.
    ///
    /// - `XLogData`: delegate to `handle_xlog_data` for pgoutput parsing →
    ///   SchemaCache update → CdcRecord conversion → pipeline send.
    /// - `KeepAlive`: if reply is requested, send standby status update back.
    /// - `Unknown`: log and skip.
    async fn handle_replication_message(
        &mut self,
        msg: WalMessage,
        tx: &mpsc::Sender<PipelineEvent>,
        shared_state: &SharedState,
        flush_size: usize,
    ) -> Result<u64> {
        match msg {
            WalMessage::XLogData { lsn, data } => {
                handle_xlog_data(
                    data,
                    lsn,
                    tx,
                    shared_state,
                    &mut self.schema_cache,
                    flush_size,
                )
                .await?;
                Ok(lsn)
            }
            WalMessage::KeepAlive {
                lsn,
                reply_requested,
            } => {
                if reply_requested {
                    let status = build_standby_status_update(lsn);
                    self.replication_stream.send_feedback(&status).await?;
                }
                Ok(lsn)
            }
            WalMessage::Unknown(tag) => {
                warn!("Unknown replication message tag: {}", tag);
                Ok(0)
            }
        }
    }

    /// Persist a checkpoint and confirm to PostgreSQL.
    ///
    /// Order is critical:
    ///   1. Save checkpoint to persistent storage FIRST.
    ///   2. Update in-memory confirmed LSN.
    ///   3. Send standby status update to PostgreSQL LAST.
    ///
    /// If we confirmed to PG before saving locally and crashed, PG would
    /// discard WAL that we haven't persisted — permanent data loss.
    async fn handle_checkpoint_feedback(
        &mut self,
        confirmed_lsn: u64,
        state_store: &StateStore,
        shared_state: &SharedState,
        slot_name: &str,
    ) -> Result<()> {
        // 1. Save checkpoint to persistent storage
        if let Err(e) = state_store.save_checkpoint(slot_name, confirmed_lsn).await {
            error!(
                "Failed to save checkpoint at LSN 0x{:X}: {}",
                confirmed_lsn, e
            );
            error!("NOT confirming to PostgreSQL to prevent data loss");
            return Err(e);
        }

        // 2. Update SharedState (after successful persistence)
        shared_state.confirm_lsn(confirmed_lsn);

        // 3. Confirm to PostgreSQL (only after checkpoint is safely persisted)
        let status = build_standby_status_update(confirmed_lsn);
        if let Err(e) = self.replication_stream.send_feedback(&status).await {
            error!("Failed to send status update to PostgreSQL: {}", e);
            error!("Checkpoint saved but not confirmed - may cause duplicate events on restart");
            return Err(e);
        }

        debug!("Checkpoint confirmed: LSN 0x{:X}", confirmed_lsn);
        Ok(())
    }
}

#[async_trait]
impl ReplicationLoop for PgReplicationLoop {
    /// Run the PostgreSQL CDC event loop.
    ///
    /// Loop structure (every iteration, with state check every 256 iterations):
    ///   tokio::select! {
    ///       shutdown signal  → break & cleanup
    ///       snapshot trigger → spawn snapshot worker
    ///       WAL event        → parse → handle (XLogData / KeepAlive)
    ///       checkpoint       → save + confirm to PG
    ///   }
    async fn run(self: Box<Self>, ctx: LoopContext) -> Result<()> {
        // async_trait wraps the body in an async move block where `self` is
        // an immutable binding.  Rebind as mutable so we can call &mut self
        // methods (next_event, handle_replication_message, etc.).
        let mut this = self;

        let LoopContext {
            shared_state,
            config,
            state_store,
            pipeline_tx,
            mut feedback_rx,
            source_schemas: _,
            sink_factory,
            // active_chunks is MySQL-only; Postgres ignores it.
            ..
        } = ctx;

        let mut shutdown_rx = shared_state.shutdown_tx.subscribe();
        let mut snapshot_trigger_rx = shared_state.subscribe_snapshot_trigger();
        let mut iteration = 0u64;

        let slot_name = config.source.postgres().slot_name.clone();
        let flush_size = config.flush_size;

        loop {
            iteration = iteration.wrapping_add(1);

            // Periodically check whether the CDC state (Pause / Stop / Drain)
            // has changed via the shared state atom.
            if iteration & 0xFF == 0 {
                let state = shared_state.state();
                let capacity = pipeline_tx.capacity();
                if let Some(flow) = LoopHelper::check_state_control(&state, capacity, flush_size) {
                    match flow {
                        ControlFlow::Break => {
                            if state == CdcState::Draining {
                                shared_state.set_state(CdcState::Stopped);
                            }
                            break;
                        }
                        ControlFlow::Continue => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }
            }

            tokio::select! {
                biased;

                // ── Shutdown signal ────────────────────────────────────
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown signal received");
                        break;
                    }
                }

                // ── On-demand snapshot trigger ─────────────────────────
                Ok(()) = snapshot_trigger_rx.changed() => {
                    if *snapshot_trigger_rx.borrow() && !shared_state.is_snapshot_active() {
                        info!("On-demand snapshot triggered (CDC_RUNNING → SNAPSHOT)");
                        let _ = shared_state.snapshot_trigger.send(false);

                        let snap_config = config.clone();
                        let snap_state = shared_state.clone();
                        let snap_sink_factory = sink_factory.clone();
                        tokio::spawn(async move {
                            match crate::engine::snapshot::run_snapshot(
                                snap_config,
                                snap_state.clone(),
                                snap_sink_factory,
                            )
                            .await
                            {
                                Ok(()) => {
                                    snap_state.set_snapshot_active(false);
                                    info!("Snapshot completed successfully");
                                }
                                Err(e) => {
                                    snap_state.set_snapshot_active(false);
                                    snap_state
                                        .set_snapshot_error(Some(format!("{}", e)))
                                        .await;
                                    error!("Snapshot worker error: {}", e);
                                }
                            }
                        });
                        info!("Snapshot worker spawned");
                    }
                }

                // ── WAL event from the replication stream ──────────────
                data_res = this.replication_stream.next_event() => {
                    match data_res {
                        Some(Ok(data)) => {
                            let mut bytes = Bytes::from(data);
                            if let Some(msg) = parse_replication_message(&mut bytes) {
                                this.handle_replication_message(
                                    msg,
                                    &pipeline_tx,
                                    &shared_state,
                                    flush_size,
                                ).await?;
                            }
                        }
                        Some(Err(e)) => {
                            error!("Replication stream error: {}", e);
                            break;
                        }
                        None => {
                            warn!("Replication stream ended");
                            break;
                        }
                    }
                }

                // ── Checkpoint feedback from the pipeline ──────────────
                Some(confirmed_lsn) = feedback_rx.recv() => {
                    this.handle_checkpoint_feedback(
                        confirmed_lsn,
                        &state_store,
                        &shared_state,
                        &slot_name,
                    ).await?;
                }
            }
        }

        // Cleanup PG replication slot
        if shared_state.should_skip_slot_cleanup() {
            info!("[SKIP] Skipping slot cleanup (upgrade/restart mode)");
        } else {
            crate::engine::setup::cleanup_postgres_resources(
                &config.source.url,
                &config.source.postgres().slot_name,
            )
            .await
            .map_err(|e| warn!("Cleanup warning: {}", e))
            .ok();
        }

        info!("CDC shutdown complete");
        Ok(())
    }
}
