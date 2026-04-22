use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::time::interval;

use super::cpu_metrics::CpuTracker;
use super::state::{MetricsSample, SharedState};

/// Interval between metric samples. Matches the resolution that downstream
/// Grafana panels render at — finer doesn't buy anything.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Spawned by the engine at startup. Runs until the shared shutdown
/// signal fires. Writes every new sample into `shared.latest_metrics`
/// so the HTTP handler can answer in constant time.
pub async fn run_metrics_sampler(shared: Arc<SharedState>) {
    let mut cpu = CpuTracker::new();
    let mut ticker = interval(SAMPLE_INTERVAL);
    let mut last_events = shared.events_processed();
    let mut last_time = Instant::now();
    let mut shutdown = shared.shutdown_tx.subscribe();

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = ticker.tick() => {
                let now = Instant::now();
                let elapsed = now.duration_since(last_time).as_secs_f64();
                let current_events = shared.events_processed();
                let events_per_second = if elapsed > 0.0 {
                    current_events.saturating_sub(last_events) as f64 / elapsed
                } else {
                    0.0
                };

                let current_lsn = shared.current_lsn();
                let confirmed_lsn = shared.confirmed_lsn();

                let sample = MetricsSample {
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    events_per_second,
                    lag_bytes: current_lsn.saturating_sub(confirmed_lsn),
                    lag_events: shared.pending_events(),
                    memory_bytes: shared.estimate_memory(),
                    total_events_processed: current_events,
                    total_batches_sent: shared.batches_sent(),
                    cpu_millicores: cpu.cpu_millicores(),
                    replication_lag_ms: shared.replication_lag_ms(),
                };

                *shared.latest_metrics.write().await = sample;

                last_events = current_events;
                last_time = now;
            }
        }
    }
}
