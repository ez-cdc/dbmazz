// Copyright 2025
// Licensed under the Elastic License v2.0

//! Divides a table into PK-range chunks suitable for parallel snapshot.
//!
//! Only integer (BIGINT / INT) primary keys are supported for chunking.
//! Tables with non-integer PKs are excluded from snapshot with a warning.
//!
//! Chunk count is based on estimated row count (`pg_class.reltuples`) when
//! available, falling back to PK range division for un-ANALYZEd tables.

use anyhow::{Context, Result};
use tokio_postgres::Client;
use tracing::{debug, info, warn};

use super::quote_ident;
use super::utils::find_integer_pk_column;

/// Maximum number of chunks per table to avoid excessive state_store rows.
/// 500K chunks × 50K rows/chunk = 25 billion rows coverage.
const MAX_CHUNKS_PER_TABLE: u64 = 500_000;

/// A PK range chunk: [start_pk, end_pk) — start inclusive, end exclusive.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub partition_id: i32,
    pub start_pk: i64,
    pub end_pk: i64, // exclusive upper bound
}

/// Divide a table into chunks of approximately `chunk_size` rows.
///
/// Uses `pg_class.reltuples` to estimate total rows and calculate the number
/// of chunks. Falls back to PK-range-based calculation when reltuples is
/// unavailable (table never ANALYZEd).
///
/// Returns an empty Vec if the table is empty or has no suitable integer PK.
/// The caller should treat an empty result as "no snapshot needed" for that table.
pub async fn chunk_table(client: &Client, table_name: &str, chunk_size: u64) -> Result<Vec<Chunk>> {
    // Identify the primary key column — we need an integer type
    let pk_col = match find_integer_pk_column(client, table_name).await? {
        Some(col) => col,
        None => {
            warn!(
                "Table {} has no suitable integer PK for chunking — skipping snapshot",
                table_name
            );
            return Ok(vec![]);
        }
    };

    // Find MIN and MAX of the PK column (cast to bigint to handle int2/int4 PKs)
    let query = format!(
        "SELECT MIN({pk})::bigint, MAX({pk})::bigint FROM {table}",
        pk = quote_ident(&pk_col),
        table = quote_ident(table_name),
    );
    let row = client
        .query_one(&query, &[])
        .await
        .with_context(|| format!("failed to query MIN/MAX for {}", table_name))?;

    let min_pk: Option<i64> = row.get(0);
    let max_pk: Option<i64> = row.get(1);

    let (min_pk, max_pk) = match (min_pk, max_pk) {
        (Some(min), Some(max)) => (min, max),
        _ => {
            info!("Table {} is empty — no snapshot needed", table_name);
            return Ok(vec![]);
        }
    };

    if min_pk == max_pk {
        // Single row table (or all rows share the same PK value)
        return Ok(vec![Chunk {
            partition_id: 0,
            start_pk: min_pk,
            end_pk: max_pk + 1,
        }]);
    }

    // --- Determine number of chunks ---
    //
    // Primary: use pg_class.reltuples to estimate total rows.
    // This correctly handles composite PKs where the first integer column has
    // a small value range but many rows per value (e.g. TPC-DS store_sales:
    // 2.88B rows but ss_item_sk range of only ~300K → old algo made 6 chunks
    // of 480M rows each, causing OOM).
    //
    // Fallback: reltuples = 0 means the table has never been ANALYZEd.
    // In that case, fall back to PK range division (original behavior).
    let estimated_rows = get_estimated_row_count(client, table_name)
        .await
        .unwrap_or_else(|e| {
            warn!(
                "Failed to get reltuples for {}: {} — falling back to PK range",
                table_name, e
            );
            0
        });

    let total_range = (max_pk - min_pk) as u64 + 1;
    let pk_range_chunks = total_range.div_ceil(chunk_size).max(1);

    let num_chunks = if estimated_rows > 0 {
        // Row-count-based chunking: each chunk targets ~chunk_size rows
        let n = estimated_rows.div_ceil(chunk_size).max(1);
        let n = n.min(MAX_CHUNKS_PER_TABLE);
        // Never fewer chunks than PK-range would produce (safety guarantee:
        // the new algorithm never creates larger chunks than the old one)
        n.max(pk_range_chunks) as i32
    } else {
        // Fallback: reltuples unavailable — use PK range (original behavior)
        pk_range_chunks as i32
    };

    info!(
        "Table {}: pk_col={}, min={}, max={}, pk_range={}, estimated_rows={}, chunk_size={}, num_chunks={}",
        table_name, pk_col, min_pk, max_pk, total_range, estimated_rows, chunk_size, num_chunks
    );

    // --- Build chunk boundaries ---
    //
    // Divide the PK range evenly across all chunks using floating-point
    // to handle cases where num_chunks > total_range gracefully.
    let mut chunks = Vec::with_capacity(num_chunks as usize);
    let pk_range_f64 = (max_pk - min_pk) as f64;
    let step_f64 = pk_range_f64 / num_chunks as f64;

    for i in 0..num_chunks {
        let start = min_pk + (i as f64 * step_f64) as i64;
        let end = if i == num_chunks - 1 {
            max_pk + 1 // last chunk is inclusive of max
        } else {
            min_pk + ((i + 1) as f64 * step_f64) as i64
        };
        // Skip degenerate chunks where start >= end.
        // This happens when num_chunks > total_range (many chunks, small PK range).
        if start >= end && i < num_chunks - 1 {
            continue;
        }
        chunks.push(Chunk {
            partition_id: i,
            start_pk: start,
            end_pk: end,
        });
    }

    debug!(
        "Table {}: generated {} actual chunks (requested {})",
        table_name,
        chunks.len(),
        num_chunks
    );

    Ok(chunks)
}

/// Get the estimated row count from `pg_class.reltuples`.
///
/// Returns 0 if the table has not been ANALYZEd yet (`reltuples` = -1 or 0).
/// This is a cheap catalog lookup — no sequential scan.
async fn get_estimated_row_count(client: &Client, table_name: &str) -> Result<u64> {
    let (schema, table) = if table_name.contains('.') {
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    let row = client
        .query_one(
            "SELECT GREATEST(c.reltuples, 0)::bigint
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = $1 AND n.nspname = $2",
            &[&table, &schema],
        )
        .await
        .with_context(|| format!("failed to get reltuples for {}", table_name))?;

    let estimated: i64 = row.get(0);
    Ok(estimated.max(0) as u64)
}
