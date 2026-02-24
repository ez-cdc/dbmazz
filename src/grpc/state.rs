use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CdcState {
    Running = 0,
    Paused = 1,
    Draining = 2,
    Stopped = 3,
}

impl CdcState {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => CdcState::Running,
            1 => CdcState::Paused,
            2 => CdcState::Draining,
            _ => CdcState::Stopped,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Stage {
    Init,
    Setup,
    Snapshot,
    Cdc,
}

#[derive(Clone)]
pub struct CdcConfig {
    pub flush_size: usize,
    pub flush_interval_ms: u64,
    pub tables: Vec<String>,
    pub slot_name: String,
}

pub struct SharedState {
    pub state: AtomicU8,
    pub stage: RwLock<Stage>,
    pub stage_detail: RwLock<String>,
    pub setup_error: RwLock<Option<String>>,  // Descriptive setup error
    pub current_lsn: AtomicU64,
    pub confirmed_lsn: AtomicU64,
    pub pending_events: AtomicU64,
    pub events_processed: AtomicU64,
    pub batches_sent: AtomicU64,
    pub shutdown_tx: watch::Sender<bool>,
    pub config: RwLock<CdcConfig>,
    // Timestamp of last processed event (to calculate events/sec)
    pub last_event_time: RwLock<std::time::Instant>,
    pub events_last_second: AtomicU64,
    // If true, don't drop the replication slot on shutdown (for upgrades/restarts)
    pub skip_slot_cleanup: AtomicBool,
    pub replication_lag_ms: AtomicU64,
    #[cfg(feature = "demo")]
    pub demo_event_tx: tokio::sync::broadcast::Sender<String>,
    // Snapshot progress (written by snapshot worker, read by gRPC status service)
    pub snapshot_chunks_total: AtomicU64,
    pub snapshot_chunks_done: AtomicU64,
    pub snapshot_rows_synced: AtomicU64,
    // Signal channel for on-demand snapshot trigger (set by StartSnapshot RPC)
    pub snapshot_trigger: watch::Sender<bool>,
    // True while the snapshot worker is running
    pub snapshot_active: AtomicBool,
    /// Finished snapshot chunks: (start_pk, end_pk) -> hw_lsn
    /// Written by snapshot worker after each chunk, read by WAL handler for should_emit()
    pub finished_chunks: RwLock<BTreeMap<(i64, i64), u64>>,
    /// Relation PK column indices: relation_id -> list of column indices that form the PK
    /// Populated from Relation messages by the WAL handler
    pub relation_pk_cols: RwLock<HashMap<u32, Vec<usize>>>,
}

impl SharedState {
    pub fn new(config: CdcConfig) -> Arc<Self> {
        let (shutdown_tx, _) = watch::channel(false);
        let (snapshot_trigger, _) = watch::channel(false);
        Arc::new(Self {
            state: AtomicU8::new(CdcState::Running as u8),
            stage: RwLock::new(Stage::Init),
            stage_detail: RwLock::new("Initializing".to_string()),
            setup_error: RwLock::new(None),
            current_lsn: AtomicU64::new(0),
            confirmed_lsn: AtomicU64::new(0),
            pending_events: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            shutdown_tx,
            config: RwLock::new(config),
            last_event_time: RwLock::new(std::time::Instant::now()),
            events_last_second: AtomicU64::new(0),
            skip_slot_cleanup: AtomicBool::new(false),
            replication_lag_ms: AtomicU64::new(0),
            #[cfg(feature = "demo")]
            demo_event_tx: {
                let (tx, _) = tokio::sync::broadcast::channel(256);
                tx
            },
            snapshot_chunks_total: AtomicU64::new(0),
            snapshot_chunks_done: AtomicU64::new(0),
            snapshot_rows_synced: AtomicU64::new(0),
            snapshot_trigger,
            snapshot_active: AtomicBool::new(false),
            finished_chunks: RwLock::new(BTreeMap::new()),
            relation_pk_cols: RwLock::new(HashMap::new()),
        })
    }

    pub fn set_skip_slot_cleanup(&self, skip: bool) {
        self.skip_slot_cleanup.store(skip, Ordering::Release);
    }

    pub fn should_skip_slot_cleanup(&self) -> bool {
        self.skip_slot_cleanup.load(Ordering::Acquire)
    }

    pub fn update_lsn(&self, lsn: u64) {
        self.current_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn confirm_lsn(&self, lsn: u64) {
        self.confirmed_lsn.store(lsn, Ordering::Relaxed);
    }

    pub fn increment_events(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
        self.events_last_second.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_batches(&self) {
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_pending(&self, count: u64) {
        self.pending_events.store(count, Ordering::Relaxed);
    }

    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::Relaxed)
    }

    pub fn confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn.load(Ordering::Relaxed)
    }

    pub fn pending_events(&self) -> u64 {
        self.pending_events.load(Ordering::Relaxed)
    }

    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    pub fn batches_sent(&self) -> u64 {
        self.batches_sent.load(Ordering::Relaxed)
    }

    pub fn events_last_second(&self) -> u64 {
        self.events_last_second.swap(0, Ordering::Relaxed)
    }

    pub fn estimate_memory(&self) -> u64 {
        // Basic estimation: pending_events * 1KB average per event
        self.pending_events.load(Ordering::Relaxed) * 1024
    }

    pub async fn set_stage(&self, stage: Stage, detail: &str) {
        *self.stage.write().await = stage;
        *self.stage_detail.write().await = detail.to_string();
    }
    
    pub async fn stage(&self) -> (Stage, String) {
        let stage = *self.stage.read().await;
        let detail = self.stage_detail.read().await.clone();
        (stage, detail)
    }

    pub async fn set_setup_error(&self, error: Option<String>) {
        *self.setup_error.write().await = error;
    }

    pub async fn setup_error(&self) -> Option<String> {
        self.setup_error.read().await.clone()
    }

    // Synchronous methods for CDC state (no await)
    pub fn state(&self) -> CdcState {
        CdcState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn set_state(&self, state: CdcState) {
        self.state.store(state as u8, Ordering::Release);
    }

    pub fn compare_and_set_state(&self, expected: CdcState, new: CdcState) -> bool {
        self.state.compare_exchange(
            expected as u8,
            new as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ).is_ok()
    }

    pub fn set_replication_lag_ms(&self, lag: u64) {
        self.replication_lag_ms.store(lag, Ordering::Relaxed);
    }

    pub fn replication_lag_ms(&self) -> u64 {
        self.replication_lag_ms.load(Ordering::Relaxed)
    }

    /// Update snapshot progress counters (called by snapshot worker after each chunk).
    pub fn update_snapshot_progress(&self, total: u64, done: u64, rows: u64) {
        self.snapshot_chunks_total.store(total, Ordering::Relaxed);
        self.snapshot_chunks_done.store(done, Ordering::Relaxed);
        self.snapshot_rows_synced.store(rows, Ordering::Relaxed);
    }

    pub fn snapshot_chunks_total(&self) -> u64 {
        self.snapshot_chunks_total.load(Ordering::Relaxed)
    }

    pub fn snapshot_chunks_done(&self) -> u64 {
        self.snapshot_chunks_done.load(Ordering::Relaxed)
    }

    pub fn snapshot_rows_synced(&self) -> u64 {
        self.snapshot_rows_synced.load(Ordering::Relaxed)
    }

    /// Subscribe to the snapshot trigger channel (for on-demand snapshot signaling).
    pub fn subscribe_snapshot_trigger(&self) -> watch::Receiver<bool> {
        self.snapshot_trigger.subscribe()
    }

    /// Signal that a snapshot should start (called by StartSnapshot gRPC handler).
    pub fn trigger_snapshot(&self) {
        let _ = self.snapshot_trigger.send(true);
    }

    pub fn set_snapshot_active(&self, active: bool) {
        self.snapshot_active.store(active, Ordering::Release);
    }

    pub fn is_snapshot_active(&self) -> bool {
        self.snapshot_active.load(Ordering::Acquire)
    }

    /// Register a finished chunk so the WAL handler can suppress duplicate events.
    /// Called by the snapshot worker after each chunk completes.
    pub async fn register_finished_chunk(&self, start_pk: i64, end_pk: i64, hw_lsn: u64) {
        let mut map = self.finished_chunks.write().await;
        map.insert((start_pk, end_pk), hw_lsn);
    }

    /// Determine whether a WAL event should be emitted to the pipeline.
    ///
    /// Returns `false` (suppress) only when:
    /// - snapshot is active
    /// - the event's PK falls within a finished chunk range
    /// - the event's LSN is at or before the chunk's high-watermark LSN
    ///
    /// This is O(log n) via BTreeMap range lookup.
    pub async fn should_emit(&self, event_lsn: u64, pk: Option<i64>) -> bool {
        // Fast path: no snapshot running
        if !self.is_snapshot_active() {
            return true;
        }
        let Some(pk_val) = pk else {
            return true; // Can't determine PK → emit (safe)
        };
        let map = self.finished_chunks.read().await;
        // Find the last range whose start <= pk_val
        if let Some(((start, end), &hw_lsn)) = map.range(..=(pk_val, i64::MAX)).next_back() {
            if *start <= pk_val && pk_val < *end {
                // PK is inside this chunk; suppress if event is at or before the HW
                return event_lsn > hw_lsn;
            }
        }
        true // PK not in any finished chunk → emit
    }
}

