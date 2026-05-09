// Copyright 2025
// Licensed under the Elastic License v2.0

//! End-to-end integration test: concurrent snapshot + CDC against a real
//! MySQL instance, with a recording sink.
//!
//! Exercises the full DBLog read-only reconciliation path:
//!   * snapshot reads chunks bounded by `[start_pk, end_pk)`,
//!   * binlog consumer concurrently processes random UPDATEs,
//!   * eviction is recorded for PKs that fall in active chunks' windows,
//!   * snapshot worker drains drained chunks and emits non-evicted rows,
//!   * `try_drain` fires on every event including heartbeats so quiet
//!     intervals don't stall the snapshot worker.
//!
//! Final assertion: every PK observed by the source ends up in the
//! recorder's captured records exactly the right number of times — no
//! PK is missing, no PK has a "stale snapshot Insert" emitted on top
//! of a CDC Update for the same PK with a more recent value.
//!
//! Gated behind TEST_MYSQL_URL — see tests/common/mod.rs.

#![cfg(feature = "mysql-source")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use mysql_async::prelude::Queryable;

use common::{connect_admin, drop_database, require_mysql_url, unique_db_name, RecordingSink};

const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(180);
const ROW_COUNT: i32 = 5_000;
const UPDATE_COUNT: usize = 500;
const CHUNK_SIZE: u64 = 500;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires TEST_MYSQL_URL — see tests/common/mod.rs"]
async fn concurrent_snapshot_and_cdc_no_data_loss_or_duplicates() -> Result<()> {
    let admin_url = require_mysql_url();
    let db = unique_db_name("dbmazz_cs");

    // -------------------------------------------------------------------
    // Setup: create database + populate.
    // -------------------------------------------------------------------
    let mut admin = connect_admin(&admin_url).await?;
    admin
        .query_drop(format!("CREATE DATABASE `{}`", db))
        .await?;
    admin
        .query_drop(format!(
            "CREATE TABLE `{}`.`accounts` (\
                id INT PRIMARY KEY, \
                balance INT NOT NULL\
            ) ENGINE=InnoDB",
            db
        ))
        .await?;
    let inserts: String = (1..=ROW_COUNT)
        .map(|i| format!("({}, {})", i, i * 10))
        .collect::<Vec<_>>()
        .join(",");
    admin
        .query_drop(format!(
            "INSERT INTO `{}`.`accounts` (id, balance) VALUES {}",
            db, inserts
        ))
        .await?;
    admin.disconnect().await?;

    let test_url = url_with_database(&admin_url, &db);

    // -------------------------------------------------------------------
    // Spawn dbmazz with a recording sink and DO_SNAPSHOT=true so the
    // snapshot starts as soon as the daemon comes up. CDC is running
    // concurrently from the moment the binlog stream opens.
    // -------------------------------------------------------------------
    let recorder = RecordingSink::new();
    let config = make_config(&test_url, /*do_snapshot=*/ true);
    let engine = dbmazz::engine::CdcEngine::with_sink_factory(config, recorder.factory()).await?;
    let shared_state = engine.shared_state();
    let shared_for_main = Arc::clone(&shared_state);

    let engine_handle = tokio::spawn(async move { engine.run().await });

    // Wait for the engine to enter Cdc stage (binlog stream open).
    wait_for_cdc(&shared_for_main, Duration::from_secs(20)).await?;

    // -------------------------------------------------------------------
    // Drive concurrent UPDATEs. Random PKs in [1, ROW_COUNT].
    // -------------------------------------------------------------------
    let mut update_handles = Vec::new();
    // Deterministic stride pick — UPDATE_COUNT distinct PKs spread over
    // [1, ROW_COUNT]. Avoids pulling rand into dev-deps and keeps the
    // test reproducible across runs.
    let stride = (ROW_COUNT as usize / UPDATE_COUNT).max(1);
    let targets: Vec<i32> = (1..=ROW_COUNT).step_by(stride).take(UPDATE_COUNT).collect();
    let expected_post_update_balance: HashMap<i32, i32> = targets
        .iter()
        .map(|&pk| (pk, pk * 10 + 1)) // increment by 1 — distinct from pre-snapshot value
        .collect();

    for &pk in &targets {
        let url = test_url.clone();
        let db_name = db.clone();
        let new_balance = pk * 10 + 1;
        update_handles.push(tokio::spawn(async move {
            // Spread UPDATEs across the snapshot window.
            tokio::time::sleep(Duration::from_millis((pk as u64 % 1000).saturating_add(10))).await;
            let mut conn = connect_admin(&url).await?;
            conn.exec_drop(
                format!(
                    "UPDATE `{}`.`accounts` SET balance = ? WHERE id = ?",
                    db_name
                ),
                (new_balance, pk),
            )
            .await?;
            conn.disconnect().await?;
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for snapshot completion or timeout.
    let snap_done = tokio::time::timeout(SNAPSHOT_TIMEOUT, async {
        loop {
            if !shared_state.is_snapshot_active() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(
        snap_done.is_ok(),
        "snapshot did not complete within {:?}",
        SNAPSHOT_TIMEOUT
    );

    // Ensure all UPDATEs have actually committed; flush via sleep so
    // the binlog consumer has a chance to forward them.
    for h in update_handles {
        h.await??;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Tear down.
    let _ = shared_state.shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(15), engine_handle).await;

    // -------------------------------------------------------------------
    // Assertions.
    // -------------------------------------------------------------------
    let captured = recorder.captured();

    // 1. Every PK 1..=ROW_COUNT appears at least once in the captured
    //    records (either as snapshot Insert or — for late-bootstrap —
    //    no record, but our test bootstraps before the binlog stream
    //    rotates, so all rows must be observed).
    let mut seen_pks: std::collections::HashSet<i32> = std::collections::HashSet::new();
    let mut latest_balance: HashMap<i32, i32> = HashMap::new();
    let mut snapshot_inserts_per_pk: HashMap<i32, u32> = HashMap::new();

    for r in &captured {
        match r {
            dbmazz::core::CdcRecord::Insert { table, columns, .. } if table.name == "accounts" => {
                if let (Some(pk), Some(balance)) =
                    (extract_int(columns, "id"), extract_int(columns, "balance"))
                {
                    seen_pks.insert(pk as i32);
                    latest_balance.insert(pk as i32, balance as i32);
                    *snapshot_inserts_per_pk.entry(pk as i32).or_default() += 1;
                }
            }
            dbmazz::core::CdcRecord::Update {
                table, new_columns, ..
            } if table.name == "accounts" => {
                if let (Some(pk), Some(balance)) = (
                    extract_int(new_columns, "id"),
                    extract_int(new_columns, "balance"),
                ) {
                    seen_pks.insert(pk as i32);
                    latest_balance.insert(pk as i32, balance as i32);
                }
            }
            _ => {}
        }
    }

    let missing: Vec<i32> = (1..=ROW_COUNT)
        .filter(|pk| !seen_pks.contains(pk))
        .collect();
    assert!(
        missing.is_empty(),
        "{} PKs missing from sink output (first 20: {:?})",
        missing.len(),
        missing.iter().take(20).collect::<Vec<_>>()
    );

    // 2. For every PK that received a concurrent UPDATE, the latest
    //    balance observed at the sink matches the post-update value
    //    (`pk*10 + 1`), not the pre-update value (`pk*10`). This is
    //    the C3 fix: even if the snapshot SELECT raced the UPDATE,
    //    the eviction (or order-of-arrival) must surface the latest
    //    value as the last write to the sink.
    let mut wrong: Vec<(i32, i32, i32)> = Vec::new();
    for (pk, &expected) in &expected_post_update_balance {
        match latest_balance.get(pk) {
            Some(&actual) if actual == expected => {} // correct
            Some(&actual) => wrong.push((*pk, expected, actual)),
            None => wrong.push((*pk, expected, -1)),
        }
    }
    assert!(
        wrong.is_empty(),
        "{} PKs have wrong final balance at sink (sample: {:?})",
        wrong.len(),
        wrong.iter().take(5).collect::<Vec<_>>()
    );

    // 3. Snapshot Inserts per PK should be ≤ 1. More than one means a
    //    chunk overlap — would indicate a regression in chunker logic.
    let multi_insert: Vec<(i32, u32)> = snapshot_inserts_per_pk
        .iter()
        .filter(|(_, &n)| n > 1)
        .map(|(pk, n)| (*pk, *n))
        .collect();
    assert!(
        multi_insert.is_empty(),
        "PKs received >1 snapshot Insert: {:?}",
        multi_insert.iter().take(5).collect::<Vec<_>>()
    );

    drop_database(&admin_url, &db).await;
    Ok(())
}

async fn wait_for_cdc(state: &dbmazz::control::SharedState, timeout: Duration) -> Result<()> {
    let res = tokio::time::timeout(timeout, async {
        loop {
            let (stage, _detail) = state.stage().await;
            if matches!(stage, dbmazz::control::Stage::Cdc) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    res.map_err(|_| anyhow::anyhow!("dbmazz did not reach CDC stage within {:?}", timeout))?;
    Ok(())
}

fn extract_int(columns: &[dbmazz::core::ColumnValue], name: &str) -> Option<i64> {
    columns.iter().find(|c| c.name == name).and_then(|c| {
        if let dbmazz::core::Value::Int64(i) = &c.value {
            Some(*i)
        } else {
            None
        }
    })
}

fn make_config(source_url: &str, do_snapshot: bool) -> dbmazz::config::Config {
    use dbmazz::config::{
        Config, MysqlSourceConfig, PostgresSinkConfig, SinkConfig, SinkSpecificConfig, SinkType,
        SourceConfig, SourceType,
    };

    let mysql_source = MysqlSourceConfig {
        server_id: 5401,
        gtid_enabled: true,
        tls_skip_verify: false,
    };

    let source = SourceConfig {
        source_type: SourceType::Mysql,
        url: source_url.to_string(),
        tables: vec!["accounts".to_string()],
        postgres: None,
        mysql: Some(mysql_source),
    };

    let sink = SinkConfig {
        sink_type: SinkType::Postgres,
        url: "127.0.0.1".to_string(),
        port: 5432,
        database: "irrelevant".to_string(),
        user: "irrelevant".to_string(),
        password: "irrelevant".to_string(),
        specific: SinkSpecificConfig::Postgres(PostgresSinkConfig {
            schema: "public".to_string(),
            job_name: "test".to_string(),
        }),
    };

    Config {
        source,
        sink,
        flush_size: 200,
        flush_interval_ms: 200,
        do_snapshot,
        initial_snapshot_only: false,
        control_port: 0,
        snapshot_chunk_size: CHUNK_SIZE,
        snapshot_parallel_workers: 1,
    }
}

fn url_with_database(base_url: &str, db: &str) -> String {
    match url::Url::parse(base_url) {
        Ok(mut u) => {
            u.set_path(&format!("/{}", db));
            u.to_string()
        }
        Err(_) => format!("{}/{}", base_url.trim_end_matches('/'), db),
    }
}
