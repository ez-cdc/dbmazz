pub mod schema_cache;

use crate::source::parser::{CdcMessage, CdcEvent};
use crate::grpc::state::SharedState;
use tokio::sync::mpsc;
use crate::pipeline::schema_cache::SchemaCache;
use crate::sink::Sink;
use std::sync::Arc;
use std::time::Duration;

pub struct Pipeline {
    rx: mpsc::Receiver<CdcEvent>,
    schema_cache: SchemaCache,
    sink: Box<dyn Sink + Send>,
    batch_size: usize,
    batch_timeout: Duration,
    feedback_tx: Option<mpsc::Sender<u64>>,
    shared_state: Option<Arc<SharedState>>,
}

impl Pipeline {
    pub fn new(
        rx: mpsc::Receiver<CdcEvent>, 
        sink: Box<dyn Sink + Send>,
        batch_size: usize,
        batch_timeout: Duration
    ) -> Self {
        Self {
            rx,
            schema_cache: SchemaCache::new(),
            sink,
            batch_size,
            batch_timeout,
            feedback_tx: None,
            shared_state: None,
        }
    }

    /// Configura el canal de feedback para enviar LSNs confirmados al main loop
    pub fn with_feedback_channel(mut self, feedback_tx: mpsc::Sender<u64>) -> Self {
        self.feedback_tx = Some(feedback_tx);
        self
    }

    /// Configura el estado compartido para métricas
    pub fn with_shared_state(mut self, shared_state: Arc<SharedState>) -> Self {
        self.shared_state = Some(shared_state);
        self
    }

    pub async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut interval = tokio::time::interval(self.batch_timeout);
        let mut last_lsn: u64 = 0;

        loop {
            tokio::select! {
                Some(event) = self.rx.recv() => {
                    last_lsn = event.lsn; // Actualizar LSN
                    
                    // Detectar cambios de schema
                    if let Some(delta) = self.schema_cache.update(&event.message) {
                        println!("🔧 Schema change detected for table {}: {} new columns", 
                            delta.table_name, delta.added_columns.len());
                        if let Err(e) = self.sink.apply_schema_delta(&delta).await {
                            eprintln!("❌ Schema evolution failed: {}", e);
                            // Continuar procesando - no detener el pipeline por errores de DDL
                        }
                    }
                    
                    batch.push(event.message);
                    
                    if batch.len() >= self.batch_size {
                        self.flush_batch(&batch, last_lsn).await;
                        batch.clear();
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        self.flush_batch(&batch, last_lsn).await;
                        batch.clear();
                    }
                }
            }
        }
    }

    async fn flush_batch(&mut self, batch: &[CdcMessage], lsn: u64) {
        match self.sink.push_batch(batch, &self.schema_cache, lsn).await {
            Ok(_) => {
                // Actualizar métrica de batches enviados
                if let Some(ref state) = self.shared_state {
                    state.increment_batches();
                }

                // Enviar LSN al canal de feedback para confirmar checkpoint
                if let Some(ref tx) = self.feedback_tx {
                    if let Err(e) = tx.send(lsn).await {
                        eprintln!("Failed to send feedback: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Sink error (will not checkpoint): {}", e);
            }
        }
    }
}


