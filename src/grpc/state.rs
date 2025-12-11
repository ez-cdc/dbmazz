use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CdcState {
    Running,
    Paused,
    Draining,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Stage {
    Init,
    Setup,
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
    pub state: RwLock<CdcState>,
    pub stage: RwLock<Stage>,
    pub stage_detail: RwLock<String>,
    pub current_lsn: AtomicU64,
    pub confirmed_lsn: AtomicU64,
    pub pending_events: AtomicU64,
    pub events_processed: AtomicU64,
    pub batches_sent: AtomicU64,
    pub shutdown_tx: watch::Sender<bool>,
    pub config: RwLock<CdcConfig>,
    // Timestamp del último evento procesado (para calcular events/sec)
    pub last_event_time: RwLock<std::time::Instant>,
    pub events_last_second: AtomicU64,
}

impl SharedState {
    pub fn new(config: CdcConfig) -> Arc<Self> {
        let (shutdown_tx, _) = watch::channel(false);
        Arc::new(Self {
            state: RwLock::new(CdcState::Running),
            stage: RwLock::new(Stage::Init),
            stage_detail: RwLock::new("Initializing".to_string()),
            current_lsn: AtomicU64::new(0),
            confirmed_lsn: AtomicU64::new(0),
            pending_events: AtomicU64::new(0),
            events_processed: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            shutdown_tx,
            config: RwLock::new(config),
            last_event_time: RwLock::new(std::time::Instant::now()),
            events_last_second: AtomicU64::new(0),
        })
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

    pub fn get_current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::Relaxed)
    }

    pub fn get_confirmed_lsn(&self) -> u64 {
        self.confirmed_lsn.load(Ordering::Relaxed)
    }

    pub fn get_pending_events(&self) -> u64 {
        self.pending_events.load(Ordering::Relaxed)
    }

    pub fn get_events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    pub fn get_batches_sent(&self) -> u64 {
        self.batches_sent.load(Ordering::Relaxed)
    }

    pub fn get_events_last_second(&self) -> u64 {
        self.events_last_second.swap(0, Ordering::Relaxed)
    }

    pub fn estimate_memory(&self) -> u64 {
        // Estimación básica: pending_events * 1KB promedio por evento
        self.pending_events.load(Ordering::Relaxed) * 1024
    }

    pub async fn set_stage(&self, stage: Stage, detail: &str) {
        *self.stage.write().await = stage;
        *self.stage_detail.write().await = detail.to_string();
    }
    
    pub async fn get_stage(&self) -> (Stage, String) {
        let stage = *self.stage.read().await;
        let detail = self.stage_detail.read().await.clone();
        (stage, detail)
    }
}

