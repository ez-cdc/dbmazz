// Copyright 2025
// Licensed under the Elastic License v2.0

//! Persistent state for snapshot chunks in the source PostgreSQL database.
//!
//! Stores progress in `dbmazz_snapshot_state` so snapshots can be resumed
//! after a restart (chunks with status COMPLETE are skipped).

use std::collections::HashSet;

use anyhow::{Context, Result};
use tokio_postgres::Client;
use tracing::debug;

pub const STATUS_PENDING: &str = "PENDING";
pub const STATUS_IN_PROGRESS: &str = "IN_PROGRESS";
pub const STATUS_COMPLETE: &str = "COMPLETE";
/// Create the state table if it doesn't exist.
pub async fn ensure_state_table(client: &Client) -> Result<()> {
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS dbmazz_snapshot_state (
            slot_name    TEXT    NOT NULL,
            table_name   TEXT    NOT NULL,
            partition_id INT     NOT NULL,
            start_pk     BIGINT  NOT NULL,
            end_pk       BIGINT  NOT NULL,
            rows_synced  BIGINT  NOT NULL DEFAULT 0,
            status       TEXT    NOT NULL DEFAULT 'PENDING',
            hw_lsn       BIGINT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (slot_name, table_name, partition_id)
        )",
            &[],
        )
        .await
        .context("failed to create dbmazz_snapshot_state")?;
    Ok(())
}

/// Load partition IDs of already-COMPLETE chunks for a specific table.
/// Used during streaming chunk computation to skip completed chunks (resumability).
pub async fn load_complete_partition_ids(
    client: &Client,
    slot_name: &str,
    table_name: &str,
) -> Result<HashSet<i32>> {
    let rows = client
        .query(
            "SELECT partition_id FROM dbmazz_snapshot_state
         WHERE slot_name = $1 AND table_name = $2 AND status = $3",
            &[&slot_name, &table_name, &STATUS_COMPLETE],
        )
        .await
        .context("failed to load complete chunk IDs")?;

    Ok(rows.iter().map(|r| r.get::<_, i32>(0)).collect())
}

/// Insert a new chunk if it doesn't already exist (idempotent).
pub async fn upsert_chunk(
    client: &Client,
    slot_name: &str,
    table_name: &str,
    partition_id: i32,
    start_pk: i64,
    end_pk: i64,
) -> Result<()> {
    client
        .execute(
            "INSERT INTO dbmazz_snapshot_state
             (slot_name, table_name, partition_id, start_pk, end_pk, status)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (slot_name, table_name, partition_id) DO NOTHING",
            &[
                &slot_name,
                &table_name,
                &partition_id,
                &start_pk,
                &end_pk,
                &STATUS_PENDING,
            ],
        )
        .await
        .context("failed to upsert snapshot chunk")?;
    Ok(())
}

/// Mark a chunk as in-progress.
pub async fn mark_chunk_in_progress(
    client: &Client,
    slot_name: &str,
    table_name: &str,
    partition_id: i32,
) -> Result<()> {
    client
        .execute(
            "UPDATE dbmazz_snapshot_state
         SET status = $1, updated_at = NOW()
         WHERE slot_name = $2 AND table_name = $3 AND partition_id = $4",
            &[&STATUS_IN_PROGRESS, &slot_name, &table_name, &partition_id],
        )
        .await?;
    Ok(())
}

/// Mark a chunk as complete and record progress.
pub async fn mark_chunk_complete(
    client: &Client,
    slot_name: &str,
    table_name: &str,
    partition_id: i32,
    rows_synced: i64,
    hw_lsn: i64,
) -> Result<()> {
    client
        .execute(
            "UPDATE dbmazz_snapshot_state
         SET status = $1, rows_synced = $2, hw_lsn = $3, updated_at = NOW()
         WHERE slot_name = $4 AND table_name = $5 AND partition_id = $6",
            &[
                &STATUS_COMPLETE,
                &rows_synced,
                &hw_lsn,
                &slot_name,
                &table_name,
                &partition_id,
            ],
        )
        .await?;
    debug!(
        "Chunk {}.{}/{} complete: {} rows, hw_lsn={}",
        slot_name, table_name, partition_id, rows_synced, hw_lsn
    );
    Ok(())
}

/// Check whether all chunks for a given slot are complete.
pub async fn all_chunks_complete(client: &Client, slot_name: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM dbmazz_snapshot_state
         WHERE slot_name = $1 AND status != $2",
            &[&slot_name, &STATUS_COMPLETE],
        )
        .await?;
    let pending: i64 = row.get(0);
    Ok(pending == 0)
}

/// Count total and done chunks.
pub async fn chunk_counts(client: &Client, slot_name: &str) -> Result<(i64, i64)> {
    let row = client
        .query_one(
            "SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE status = $2)
         FROM dbmazz_snapshot_state
         WHERE slot_name = $1",
            &[&slot_name, &STATUS_COMPLETE],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// Per-table chunk counts and rows synced for a single table.
pub async fn table_chunk_progress(
    client: &Client,
    slot_name: &str,
    table_name: &str,
) -> Result<(i64, i64)> {
    let row = client
        .query_one(
            "SELECT
            COUNT(*) FILTER (WHERE status = $3),
            COALESCE(SUM(rows_synced) FILTER (WHERE status = $3), 0)::bigint
         FROM dbmazz_snapshot_state
         WHERE slot_name = $1 AND table_name = $2",
            &[&slot_name, &table_name, &STATUS_COMPLETE],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// Per-table chunk counts and rows synced (for per-table progress reporting).
pub async fn per_table_progress(
    client: &Client,
    slot_name: &str,
) -> Result<Vec<(String, i64, i64, i64)>> {
    let rows = client
        .query(
            "SELECT
            table_name,
            COUNT(*),
            COUNT(*) FILTER (WHERE status = $2),
            COALESCE(SUM(rows_synced) FILTER (WHERE status = $2), 0)::bigint
         FROM dbmazz_snapshot_state
         WHERE slot_name = $1
         GROUP BY table_name
         ORDER BY table_name",
            &[&slot_name, &STATUS_COMPLETE],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect())
}

/// Total rows synced across all complete chunks.
pub async fn total_rows_synced(client: &Client, slot_name: &str) -> Result<i64> {
    let row = client
        .query_one(
            "SELECT COALESCE(SUM(rows_synced), 0)::bigint
         FROM dbmazz_snapshot_state
         WHERE slot_name = $1 AND status = $2",
            &[&slot_name, &STATUS_COMPLETE],
        )
        .await?;
    Ok(row.get(0))
}
