// Copyright 2025
// Licensed under the Elastic License v2.0

//! Shared helpers for the MySQL integration tests.
//!
//! Tests are gated behind `TEST_MYSQL_URL`. To run them locally:
//!
//! ```sh
//! docker run -d --rm --name dbmazz-test-mysql \
//!   -p 13306:3306 \
//!   -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
//!   -e MYSQL_DATABASE=dbmazz_test \
//!   mysql:8.0 \
//!   --gtid-mode=ON --enforce-gtid-consistency=ON \
//!   --log-bin=mysql-bin --server-id=1 \
//!   --binlog-format=ROW --binlog-row-image=FULL
//!
//! # wait ~10s for MySQL to come up
//! TEST_MYSQL_URL=mysql://root@127.0.0.1:13306/dbmazz_test \
//!   cargo test --features mysql-source -- --ignored
//! ```
//!
//! Each test creates a unique database under the configured server so
//! parallel tests don't collide; the database is dropped on teardown.

#![allow(dead_code)] // helpers are unused in some test files

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use async_trait::async_trait;
use mysql_async::prelude::Queryable;

use dbmazz::core::traits::{LoadingModel, Sink, SinkCapabilities, SinkResult, SourceTableSchema};
use dbmazz::core::CdcRecord;

/// Returns the MySQL URL for tests, or `None` if the env var is unset
/// (tests skip with a helpful message in that case).
pub fn test_mysql_url() -> Option<String> {
    std::env::var("TEST_MYSQL_URL").ok()
}

/// Skip-or-panic helper: returns the URL if set, otherwise prints a
/// recipe and exits the test successfully (so CI doesn't fail when
/// the env var is unset).
pub fn require_mysql_url() -> String {
    match test_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!(
                "TEST_MYSQL_URL not set; skipping integration test. \
                 See tests/common/mod.rs for setup instructions."
            );
            // The `#[ignore]` attribute already keeps these out of CI;
            // this `panic!` only fires when someone runs `--ignored`
            // without TEST_MYSQL_URL set, in which case skipping
            // silently would be misleading.
            panic!("TEST_MYSQL_URL must be set to run this integration test");
        }
    }
}

/// Build a randomized database name so parallel tests don't collide
/// (and a leftover db from a prior crashed run doesn't poison state).
pub fn unique_db_name(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}_{}", prefix, nanos)
}

/// Open a non-replication connection to the MySQL server (root URL).
/// Used by tests for setup/teardown SQL.
pub async fn connect_admin(url: &str) -> Result<mysql_async::Conn> {
    let opts = mysql_async::Opts::from_url(url).context("parse mysql url")?;
    let conn = mysql_async::Conn::new(opts)
        .await
        .context("connect to mysql")?;
    Ok(conn)
}

/// Drop the test database. Best-effort; logs but doesn't fail.
pub async fn drop_database(admin_url: &str, db: &str) {
    if let Ok(mut conn) = connect_admin(admin_url).await {
        let _ = conn
            .query_drop(format!("DROP DATABASE IF EXISTS `{}`", db))
            .await;
        let _ = conn.disconnect().await;
    }
}

/// In-memory sink that records every `write_batch` call. Cheaply
/// cloneable — the captured records live behind an `Arc<Mutex<...>>` so
/// the test can inspect them after the engine completes.
#[derive(Clone, Default)]
pub struct RecordingSink {
    pub records: Arc<Mutex<Vec<CdcRecord>>>,
    pub setup_calls: Arc<Mutex<u32>>,
    pub close_calls: Arc<Mutex<u32>>,
}

impl RecordingSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of all records emitted so far. Cloned so the caller can
    /// hold it without keeping the lock.
    pub fn captured(&self) -> Vec<CdcRecord> {
        self.records.lock().unwrap().clone()
    }

    /// Count of records of a specific shape, by table name. Useful for
    /// assertions like "exactly N inserts for users table".
    pub fn count_inserts_for(&self, schema: &str, table: &str) -> usize {
        self.records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| match r {
                CdcRecord::Insert { table: t, .. } => {
                    t.schema.as_deref() == Some(schema) && t.name == table
                }
                _ => false,
            })
            .count()
    }

    pub fn count_updates_for(&self, schema: &str, table: &str) -> usize {
        self.records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| match r {
                CdcRecord::Update { table: t, .. } => {
                    t.schema.as_deref() == Some(schema) && t.name == table
                }
                _ => false,
            })
            .count()
    }

    /// Build a sink_factory function that constructs an Arc-cloned
    /// view of this RecordingSink for each call (snapshot workers
    /// request a fresh sink via the factory).
    pub fn factory(&self) -> dbmazz::engine::SinkFactory {
        let recorder = self.clone();
        Arc::new(move || -> anyhow::Result<Box<dyn Sink>> { Ok(Box::new(recorder.clone())) })
    }
}

#[async_trait]
impl Sink for RecordingSink {
    fn name(&self) -> &'static str {
        "recording"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: false,
            supports_transactions: false,
            loading_model: LoadingModel::Streaming,
            min_batch_size: None,
            max_batch_size: Some(10_000),
            optimal_flush_interval_ms: 500,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        Ok(())
    }

    async fn setup(&mut self, _source_schemas: &[SourceTableSchema]) -> Result<()> {
        *self.setup_calls.lock().unwrap() += 1;
        Ok(())
    }

    async fn write_batch(&mut self, batch: Vec<CdcRecord>) -> Result<SinkResult> {
        let count = batch.len();
        let mut records = self.records.lock().unwrap();
        records.extend(batch);
        Ok(SinkResult {
            records_written: count,
            bytes_written: 0,
            last_position: None,
            schema_evolution_skipped: 0,
        })
    }

    async fn close(&mut self) -> Result<()> {
        *self.close_calls.lock().unwrap() += 1;
        Ok(())
    }
}
