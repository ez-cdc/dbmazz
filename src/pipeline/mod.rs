pub mod schema_cache;

use crate::core::{CdcRecord, Sink};
use crate::grpc::state::SharedState;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Event sent from the WAL handler to the Pipeline.
/// Contains a generic CdcRecord (not pgoutput-specific).
pub struct PipelineEvent {
    pub lsn: u64,
    pub record: CdcRecord,
}

pub struct Pipeline {
    rx: mpsc::Receiver<PipelineEvent>,
    sink: Box<dyn Sink + Send>,
    batch_size: usize,
    batch_timeout: Duration,
    feedback_tx: Option<mpsc::Sender<u64>>,
    shared_state: Option<Arc<SharedState>>,
    last_commit_timestamp_us: u64,
}

impl Pipeline {
    pub fn new(
        rx: mpsc::Receiver<PipelineEvent>,
        sink: Box<dyn Sink + Send>,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            rx,
            sink,
            batch_size,
            batch_timeout,
            feedback_tx: None,
            shared_state: None,
            last_commit_timestamp_us: 0,
        }
    }

    /// Configure the feedback channel to send confirmed LSNs to the main loop
    pub fn with_feedback_channel(mut self, feedback_tx: mpsc::Sender<u64>) -> Self {
        self.feedback_tx = Some(feedback_tx);
        self
    }

    /// Configure the shared state for metrics
    pub fn with_shared_state(mut self, shared_state: Arc<SharedState>) -> Self {
        self.shared_state = Some(shared_state);
        self
    }

    pub async fn run(mut self) {
        let mut batch: Vec<CdcRecord> = Vec::with_capacity(self.batch_size);
        let mut interval = tokio::time::interval(self.batch_timeout);
        let mut last_lsn: u64 = 0;

        loop {
            // Check if paused before processing
            if let Some(ref state) = self.shared_state {
                if state.state() == crate::grpc::state::CdcState::Paused {
                    // Flush pending batch before pausing
                    if !batch.is_empty() && !self.flush_batch(&mut batch, last_lsn).await {
                        break; // Stop on flush failure
                    }
                    // Sleep while paused
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
            }

            tokio::select! {
                event_option = self.rx.recv() => {
                    match event_option {
                        Some(event) => {
                            last_lsn = event.lsn; // Update LSN

                            // Track the latest commit timestamp for lag calculation
                            if let CdcRecord::Commit { commit_timestamp_us, .. } = &event.record {
                                self.last_commit_timestamp_us = *commit_timestamp_us;
                            }

                            batch.push(event.record);

                            if batch.len() >= self.batch_size {
                                if !self.flush_batch(&mut batch, last_lsn).await {
                                    break; // Stop on flush failure
                                }
                                if let Some(ref state) = self.shared_state {
                                    state.set_pending(0);
                                }
                            }
                        }
                        None => {
                            // Channel closed - sender dropped
                            info!("Pipeline channel closed, initiating graceful shutdown");
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        if !self.flush_batch(&mut batch, last_lsn).await {
                            break; // Stop on flush failure
                        }
                        if let Some(ref state) = self.shared_state {
                            state.set_pending(0);
                        }
                    }
                }
            }
        }

        // Graceful shutdown: flush any remaining batch
        if !batch.is_empty() {
            warn!(
                "Pipeline stopped with {} pending events in batch",
                batch.len()
            );
            self.flush_batch(&mut batch, last_lsn).await;
        }
        info!("Pipeline shutdown complete");
    }

    /// Flush batch to sink. Returns true on success, false on failure (pipeline should stop).
    /// Drains the batch Vec so it can be reused without reallocation.
    async fn flush_batch(&mut self, batch: &mut Vec<CdcRecord>, lsn: u64) -> bool {
        let records = std::mem::take(batch);
        let record_count = records.len();

        match self.sink.write_batch(records).await {
            Ok(_result) => {
                // Update metric for batches sent
                if let Some(ref state) = self.shared_state {
                    state.increment_batches();

                    // Calculate end-to-end replication lag
                    if self.last_commit_timestamp_us > 0 {
                        const PG_EPOCH_OFFSET_USEC: u64 = 946_684_800_000_000;
                        let commit_unix_us = self.last_commit_timestamp_us + PG_EPOCH_OFFSET_USEC;
                        let now_us = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_micros() as u64;
                        let lag_ms = now_us.saturating_sub(commit_unix_us) / 1_000;
                        state.set_replication_lag_ms(lag_ms);
                    }
                }

                // Send LSN to the feedback channel to confirm checkpoint
                if let Some(ref tx) = self.feedback_tx {
                    if let Err(e) = tx.send(lsn).await {
                        error!("Failed to send checkpoint feedback: {}", e);
                        // This is critical - cannot confirm checkpoint
                        return false;
                    }
                }
                true
            }
            Err(e) => {
                // CRITICAL: Sink failure (StarRocks down, network error, etc.)
                error!("CRITICAL: Sink write_batch failed: {}", e);
                error!(
                    "CRITICAL: Batch details: {} events, LSN 0x{:X}",
                    record_count, lsn
                );

                // Set CDC state to Stopped to signal error
                if let Some(ref state) = self.shared_state {
                    state.set_state(crate::grpc::state::CdcState::Stopped);
                    error!("CRITICAL: CDC state set to Stopped due to sink failure");
                }

                // Return false to break the pipeline loop
                false
            }
        }
    }
}
