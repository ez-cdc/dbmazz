//! Background traffic generator that runs INSERT/UPDATE/DELETE against the source PG.
//!
//! Runs as a `tokio::spawn` task, controlled via `CancellationToken` (stop)
//! and `Arc<AtomicBool>` (pause). Errors are silently counted.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// ── Stats ──────────────────────────────────────────────────────────────────

/// Atomic counters shared between the generator task and the dashboard.
pub struct GeneratorStats {
    pub inserts: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub errors: AtomicU64,
}

impl GeneratorStats {
    pub fn new() -> Self {
        Self {
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    #[allow(dead_code)]
    pub fn total_ops(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
            + self.updates.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
    }
}

impl Default for GeneratorStats {
    fn default() -> Self {
        Self::new()
    }
}

// ── Snapshot of stats for display ──────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    #[allow(dead_code)]
    pub errors: u64,
}

// ── TrafficGenerator ───────────────────────────────────────────────────────

/// Background traffic generator that exercises the source database.
pub struct TrafficGenerator {
    source_dsn: String,
    rate_eps: f64,
    stats: Arc<GeneratorStats>,
    cancel: CancellationToken,
    paused: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl TrafficGenerator {
    /// Create a new generator (not yet started).
    ///
    /// `rate_eps` is target events per second.
    pub fn new(source_dsn: &str, rate_eps: f64) -> Self {
        Self {
            source_dsn: source_dsn.to_string(),
            rate_eps,
            stats: Arc::new(GeneratorStats::new()),
            cancel: CancellationToken::new(),
            paused: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Start the background task. No-op if already running.
    pub fn start(&mut self) {
        if self.handle.is_some() {
            return;
        }

        let dsn = self.source_dsn.clone();
        let stats = Arc::clone(&self.stats);
        let cancel = self.cancel.clone();
        let paused = Arc::clone(&self.paused);
        let interval = Duration::from_secs_f64(1.0 / self.rate_eps.max(0.1));

        let handle = tokio::spawn(async move {
            generator_loop(dsn, stats, cancel, paused, interval).await;
        });

        self.handle = Some(handle);
    }

    /// Stop the generator and wait for the task to finish.
    pub async fn stop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Toggle the paused flag.
    #[allow(dead_code)]
    pub fn toggle(&self) {
        let prev = self.paused.load(Ordering::Relaxed);
        self.paused.store(!prev, Ordering::Relaxed);
    }

    /// Return true if the generator is paused.
    #[allow(dead_code)]
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Return true if the generator task is running (started and not cancelled).
    pub fn is_running(&self) -> bool {
        self.handle.is_some() && !self.cancel.is_cancelled()
    }

    /// Return a snapshot of the current counters.
    pub fn stats(&self) -> StatsSnapshot {
        StatsSnapshot {
            inserts: self.stats.inserts.load(Ordering::Relaxed),
            updates: self.stats.updates.load(Ordering::Relaxed),
            deletes: self.stats.deletes.load(Ordering::Relaxed),
            errors: self.stats.errors.load(Ordering::Relaxed),
        }
    }

    /// Access the shared stats arc (for the dashboard to read totals).
    #[allow(dead_code)]
    pub fn shared_stats(&self) -> Arc<GeneratorStats> {
        Arc::clone(&self.stats)
    }
}

// ── Generator loop ─────────────────────────────────────────────────────────

async fn generator_loop(
    dsn: String,
    stats: Arc<GeneratorStats>,
    cancel: CancellationToken,
    paused: Arc<AtomicBool>,
    interval: Duration,
) {
    // Retry connection in a loop.
    loop {
        if cancel.is_cancelled() {
            return;
        }

        let conn_result = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await;
        let (client, connection) = match conn_result {
            Ok(pair) => pair,
            Err(_) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                }
            }
        };

        // Spawn connection handler.
        let cancel_conn = cancel.clone();
        let conn_handle = tokio::spawn(async move {
            tokio::select! {
                _ = cancel_conn.cancelled() => {}
                result = connection => {
                    if let Err(_e) = result {
                        // Connection closed — generator will retry.
                    }
                }
            }
        });

        // Run operations until error or cancel.
        let result = run_operations(&client, &stats, &cancel, &paused, interval).await;

        conn_handle.abort();
        let _ = conn_handle.await;

        if cancel.is_cancelled() {
            return;
        }

        if result.is_err() {
            stats.errors.fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }
}

async fn run_operations(
    client: &tokio_postgres::Client,
    stats: &Arc<GeneratorStats>,
    cancel: &CancellationToken,
    paused: &Arc<AtomicBool>,
    interval: Duration,
) -> Result<(), tokio_postgres::Error> {
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};

    let mut rng = SmallRng::from_os_rng();
    let statuses = ["pending", "processing", "shipped", "delivered", "cancelled"];

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }

        // If paused, sleep and re-check.
        if paused.load(Ordering::Relaxed) {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(250)) => continue,
            }
        }

        // Pick random operation: 70% insert, 25% update, 5% delete.
        let roll: f64 = rng.random();

        if roll < 0.70 {
            // INSERT — use plain string SQL to avoid tokio-postgres type serialization issues
            // with DECIMAL columns and parameter type inference.
            // The `status` values come from a hardcoded ASCII array — no SQL injection risk.
            let customer_id = rng.random_range(1..=1000);
            let total = rng.random_range(10.0_f64..=500.0_f64);
            let status = statuses[rng.random_range(0..statuses.len())];

            // Use RETURNING to get the new id atomically — avoids the TOCTOU
            // race of a separate "SELECT id … ORDER BY id DESC LIMIT 1".
            // SERIAL / INT4 maps to i32 in tokio-postgres.
            let sql = format!(
                "INSERT INTO orders (customer_id, total, status) VALUES ({customer_id}, {total:.2}, '{status}') RETURNING id"
            );
            let result = client.query_opt(&sql as &str, &[]).await;

            match result {
                Ok(Some(row)) => {
                    stats.inserts.fetch_add(1, Ordering::Relaxed);
                    let order_id: i32 = row.get(0);

                    // Add 1-3 order_items for the newly inserted order.
                    let item_count = rng.random_range(1..=3);
                    for _ in 0..item_count {
                        let product_name = format!("Product-{}", rng.random_range(1..=500));
                        let quantity = rng.random_range(1..=10);
                        let price = rng.random_range(5.0_f64..=200.0_f64);

                        let item_sql = format!(
                            "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES ({order_id}, '{product_name}', {quantity}, {price:.2})"
                        );
                        let item_result = client.execute(&item_sql as &str, &[]).await;

                        match item_result {
                            Ok(_) => {
                                stats.inserts.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                stats.errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Ok(None) => {
                    // INSERT … RETURNING returned no row — table constraint violation
                    // or the RETURNING clause was optimised away. Count as an error
                    // and keep running (not fatal — no Err to propagate).
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        } else if roll < 0.95 {
            // UPDATE — pick a random existing order and change its status.
            let new_status = statuses[rng.random_range(0..statuses.len())];
            let sql = format!(
                "UPDATE orders SET status = '{new_status}' WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)"
            );
            let result = client.execute(&sql as &str, &[]).await;

            match result {
                Ok(_) => {
                    stats.updates.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        } else {
            // DELETE — pick a random pending order.
            let result = client
                .execute(
                    "DELETE FROM orders WHERE id = (SELECT id FROM orders WHERE status = 'pending' ORDER BY random() LIMIT 1)",
                    &[],
                )
                .await;

            match result {
                Ok(_) => {
                    stats.deletes.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }

        // Wait for next tick.
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_default() {
        let stats = GeneratorStats::new();
        assert_eq!(stats.total_ops(), 0);
        assert_eq!(stats.inserts.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn stats_increment() {
        let stats = GeneratorStats::new();
        stats.inserts.fetch_add(10, Ordering::Relaxed);
        stats.updates.fetch_add(5, Ordering::Relaxed);
        stats.deletes.fetch_add(2, Ordering::Relaxed);
        assert_eq!(stats.total_ops(), 17);
    }

    #[test]
    fn generator_toggle_pause() {
        let gen = TrafficGenerator::new("postgres://localhost/test", 10.0);
        assert!(!gen.is_paused());
        gen.toggle();
        assert!(gen.is_paused());
        gen.toggle();
        assert!(!gen.is_paused());
    }

    #[test]
    fn generator_not_running_before_start() {
        let gen = TrafficGenerator::new("postgres://localhost/test", 10.0);
        assert!(!gen.is_running());
    }

    #[test]
    fn stats_snapshot() {
        let mut gen = TrafficGenerator::new("postgres://localhost/test", 10.0);
        // Not started, stats should be zero.
        let snap = gen.stats();
        assert_eq!(snap.inserts, 0);
        assert_eq!(snap.updates, 0);
        assert_eq!(snap.deletes, 0);
        assert_eq!(snap.errors, 0);
        // Manually bump via shared stats.
        gen.shared_stats().inserts.fetch_add(42, Ordering::Relaxed);
        let snap = gen.stats();
        assert_eq!(snap.inserts, 42);
    }
}
