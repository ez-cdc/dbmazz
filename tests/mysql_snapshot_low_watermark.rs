// Copyright 2025
// Licensed under the Elastic License v2.0

//! Integration test for the snapshot LOW-watermark race fix (C3).
//!
//! Pre-fix, the snapshot worker captured `LOW = SELECT @@global.gtid_executed`
//! BEFORE registering the chunk in the active-chunks registry. Any binlog
//! event with `gtid > LOW` that mutated a row in the chunk's PK range and
//! arrived between the capture and the registration was processed by the
//! consumer with no chunk present — no eviction recorded — and the
//! subsequent SELECT then read the post-mutation value. The snapshot would
//! emit an Insert for the row at the same time the CDC consumer emitted
//! an Update, producing duplicate emission for one PK.
//!
//! Post-fix, the chunk is registered using the binlog consumer's live GTID
//! set as LOW (atomically with the registry insert), so any event the
//! consumer hasn't seen yet is in the chunk's window and gets evicted.
//!
//! This test:
//!   1. populates `users` with 1000 rows
//!   2. spawns dbmazz with a recording sink
//!   3. triggers a single snapshot chunk
//!   4. concurrently issues an UPDATE for PK 500
//!   5. waits for snapshot completion
//!   6. asserts: PK 500 appears AT MOST once in snapshot Inserts AND
//!      the post-update value is observable in the captured records.
//!
//! Gated behind TEST_MYSQL_URL — see tests/common/mod.rs for the docker
//! recipe and run instructions.

#![cfg(feature = "mysql-source")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use mysql_async::prelude::Queryable;

use common::{connect_admin, drop_database, require_mysql_url, unique_db_name, RecordingSink};

const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(60);
const ROW_COUNT: i32 = 1000;
const TARGET_PK: i32 = 500;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_MYSQL_URL — see tests/common/mod.rs"]
async fn snapshot_low_watermark_no_duplicate_emission() -> Result<()> {
    let admin_url = require_mysql_url();
    let db = unique_db_name("dbmazz_lw");

    // -------------------------------------------------------------------
    // Setup: create database, populate test table.
    // -------------------------------------------------------------------
    let mut admin = connect_admin(&admin_url).await?;
    admin
        .query_drop(format!("CREATE DATABASE `{}`", db))
        .await?;

    // Build a URL that targets the new database for dbmazz config.
    let test_url = url_with_database(&admin_url, &db);

    admin
        .query_drop(format!(
            "CREATE TABLE `{}`.`users` (\
                id INT PRIMARY KEY, \
                name VARCHAR(64) NOT NULL\
            ) ENGINE=InnoDB",
            db
        ))
        .await?;

    // Insert ROW_COUNT rows.
    let inserts: String = (1..=ROW_COUNT)
        .map(|i| format!("({}, 'snap-{}')", i, i))
        .collect::<Vec<_>>()
        .join(",");
    admin
        .query_drop(format!(
            "INSERT INTO `{}`.`users` (id, name) VALUES {}",
            db, inserts
        ))
        .await?;
    admin.disconnect().await?;

    // -------------------------------------------------------------------
    // Drive dbmazz programmatically. We want full control over snapshot
    // trigger timing, so we DO NOT pass DO_SNAPSHOT=true; we call
    // `shared_state.snapshot_trigger.send(true)` after a brief delay,
    // and concurrently issue the racing UPDATE.
    // -------------------------------------------------------------------
    let recorder = RecordingSink::new();
    let config = make_config(&test_url, &db, /*do_snapshot=*/ false);
    let engine = dbmazz::engine::CdcEngine::with_sink_factory(config, recorder.factory()).await?;

    let shared_state = engine.shared_state();
    let recorder_for_update = recorder.clone();
    let url_for_update = test_url.clone();
    let db_for_update = db.clone();
    let shared_for_trigger = Arc::clone(&shared_state);

    let engine_handle = tokio::spawn(async move { engine.run().await });

    // Wait briefly for the binlog consumer to be running and tracking.
    // 1 second is generous on a local docker instance.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Trigger snapshot.
    let _ = shared_for_trigger.snapshot_trigger.send(true);

    // Concurrent UPDATE. Run in a separate task; small jitter so the
    // race window is exercised across runs.
    let update_handle = tokio::spawn(async move {
        // Tiny jitter to interleave with snapshot SELECT timing.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut conn = connect_admin(&url_for_update).await?;
        conn.exec_drop(
            format!(
                "UPDATE `{}`.`users` SET name = ? WHERE id = ?",
                db_for_update
            ),
            ("cdc-updated-500", TARGET_PK),
        )
        .await?;
        conn.disconnect().await?;
        let _ = recorder_for_update; // keep recorder alive across awaits
        Ok::<_, anyhow::Error>(())
    });

    // Wait for snapshot to complete or timeout. Snapshot completion is
    // signalled by `is_snapshot_active() == false` AFTER the trigger
    // landed (so we wait for it to flip true → false rather than
    // spuriously matching the initial false).
    let snap_completed = tokio::time::timeout(SNAPSHOT_TIMEOUT, async {
        loop {
            if shared_state.is_snapshot_active() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        loop {
            if !shared_state.is_snapshot_active() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        snap_completed.is_ok(),
        "snapshot did not complete within {:?}",
        SNAPSHOT_TIMEOUT
    );
    update_handle.await??;

    // Give the CDC consumer a moment to flush the post-update row event
    // through the pipeline / sink.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // -------------------------------------------------------------------
    // Tear down the engine.
    // -------------------------------------------------------------------
    let _ = shared_state.shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), engine_handle).await;

    // -------------------------------------------------------------------
    // Assertions.
    // -------------------------------------------------------------------
    let captured = recorder.captured();
    let inserts_for_target: Vec<_> = captured
        .iter()
        .filter(|r| matches!(r,
            dbmazz::core::CdcRecord::Insert { table, columns, .. }
                if table.name == "users"
                && columns.iter().any(|c| c.name == "id"
                    && matches!(&c.value, dbmazz::core::Value::Int64(i) if *i == TARGET_PK as i64))
        ))
        .collect();
    let updates_for_target: Vec<_> = captured
        .iter()
        .filter(|r| matches!(r,
            dbmazz::core::CdcRecord::Update { table, new_columns, .. }
                if table.name == "users"
                && new_columns.iter().any(|c| c.name == "id"
                    && matches!(&c.value, dbmazz::core::Value::Int64(i) if *i == TARGET_PK as i64))
        ))
        .collect();

    // Pre-fix: 1 Insert (snap-500) + 1 Update (cdc-updated-500) = 2 events.
    // Post-fix correct: either
    //   (a) 0 Insert + 1 Update (eviction worked, only CDC saw the row), OR
    //   (b) 1 Insert (cdc-updated-500) + 1 Update (cdc-updated-500), where
    //       the snapshot SELECT already saw the new value because the
    //       UPDATE landed before SELECT.
    // What MUST NOT happen: 1 Insert (snap-500, the stale value) + 1 Update
    // — that means we emitted the snapshot row on top of a CDC update.

    let stale_insert = inserts_for_target.iter().any(|r| match r {
        dbmazz::core::CdcRecord::Insert { columns, .. } => columns
            .iter()
            .any(|c| c.name == "name"
                && matches!(&c.value, dbmazz::core::Value::String(s) if s == &format!("snap-{}", TARGET_PK))),
        _ => false,
    });
    assert!(
        !stale_insert,
        "PK {} was emitted as snapshot Insert with the pre-update value 'snap-{}'; \
         eviction failed (LOW-watermark race regression). \
         Inserts: {}, Updates: {}",
        TARGET_PK,
        TARGET_PK,
        inserts_for_target.len(),
        updates_for_target.len()
    );

    // We should still see the post-update value reach the sink (via either
    // path).
    let saw_post_update = captured.iter().any(|r| {
        let cols_check = |columns: &[dbmazz::core::ColumnValue]| {
            let id_match = columns.iter().any(|c| {
                c.name == "id"
                    && matches!(&c.value, dbmazz::core::Value::Int64(i) if *i == TARGET_PK as i64)
            });
            let name_match = columns.iter().any(|c| {
                c.name == "name"
                    && matches!(&c.value, dbmazz::core::Value::String(s) if s == "cdc-updated-500")
            });
            id_match && name_match
        };
        match r {
            dbmazz::core::CdcRecord::Insert { table, columns, .. } if table.name == "users" => {
                cols_check(columns)
            }
            dbmazz::core::CdcRecord::Update {
                table, new_columns, ..
            } if table.name == "users" => cols_check(new_columns),
            _ => false,
        }
    });
    assert!(
        saw_post_update,
        "the post-update value 'cdc-updated-500' for PK {} never reached the sink",
        TARGET_PK
    );

    // Cleanup.
    drop_database(&admin_url, &db).await;
    Ok(())
}

/// Build a dbmazz Config pointed at the test MySQL with a recording sink
/// stand-in (sink_type=postgres but the factory is overridden by
/// `with_sink_factory`, so the SINK_URL is never dialed).
fn make_config(source_url: &str, _db: &str, do_snapshot: bool) -> dbmazz::config::Config {
    use dbmazz::config::{Config, MysqlSourceConfig, SinkConfig, SourceConfig, SourceType};

    let mysql_source = MysqlSourceConfig {
        server_id: 5400,
        gtid_enabled: true,
        tls_skip_verify: false,
    };

    let source = SourceConfig {
        source_type: SourceType::Mysql,
        url: source_url.to_string(),
        tables: vec!["users".to_string()],
        postgres: None,
        mysql: Some(mysql_source),
    };

    // Sink config is required by `Config` but the factory we pass to
    // `with_sink_factory` is what actually creates the sink — these
    // values are never dialled in this test.
    let sink = SinkConfig {
        sink_type: dbmazz::config::SinkType::Postgres,
        url: "127.0.0.1".to_string(),
        port: 5432,
        database: "irrelevant".to_string(),
        user: "irrelevant".to_string(),
        password: "irrelevant".to_string(),
        specific: dbmazz::config::SinkSpecificConfig::Postgres(
            dbmazz::config::PostgresSinkConfig {
                schema: "public".to_string(),
                job_name: "test".to_string(),
            },
        ),
    };

    Config {
        source,
        sink,
        flush_size: 100,
        flush_interval_ms: 200,
        do_snapshot,
        initial_snapshot_only: false,
        control_port: 0, // disabled — control server bound to "0" is dev-only
        snapshot_chunk_size: ROW_COUNT as u64, // single chunk
        snapshot_parallel_workers: 1,
    }
}

/// Replace the path component of a `mysql://...` URL with `/<db>` so
/// connections target the test database.
fn url_with_database(base_url: &str, db: &str) -> String {
    match url::Url::parse(base_url) {
        Ok(mut u) => {
            u.set_path(&format!("/{}", db));
            u.to_string()
        }
        Err(_) => format!("{}/{}", base_url.trim_end_matches('/'), db),
    }
}
