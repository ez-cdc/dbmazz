// Copyright 2025
// Licensed under the Elastic License v2.0

//! Divides a table into PK-range chunks suitable for parallel snapshot.
//!
//! Only integer (BIGINT / INT) primary keys are supported for chunking.
//! Tables with non-integer PKs are excluded from snapshot with a warning.

use anyhow::{Context, Result, anyhow};
use tokio_postgres::Client;
use tracing::{debug, info, warn};

/// A PK range chunk: [start_pk, end_pk) — start inclusive, end exclusive.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub partition_id: i32,
    pub start_pk: i64,
    pub end_pk: i64,  // exclusive upper bound
}

/// Divide a table into chunks of approximately `chunk_size` rows.
///
/// Returns an empty Vec if the table is empty or has no suitable integer PK.
/// The caller should treat an empty result as "no snapshot needed" for that table.
pub async fn chunk_table(
    client: &Client,
    table_name: &str,
    chunk_size: u64,
) -> Result<Vec<Chunk>> {
    // Identify the primary key column — we need an integer type
    let pk_col = match find_integer_pk(client, table_name).await? {
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
        pk = pk_col,
        table = table_name
    );
    let row = client.query_one(&query, &[])
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
        // Single row table
        return Ok(vec![Chunk {
            partition_id: 0,
            start_pk: min_pk,
            end_pk: max_pk + 1,
        }]);
    }

    let total_range = (max_pk - min_pk) as u64 + 1;
    let num_chunks = ((total_range + chunk_size - 1) / chunk_size).max(1) as i32;

    debug!(
        "Table {}: pk_col={}, min={}, max={}, range={}, chunk_size={}, num_chunks={}",
        table_name, pk_col, min_pk, max_pk, total_range, chunk_size, num_chunks
    );

    let mut chunks = Vec::with_capacity(num_chunks as usize);
    let step = chunk_size as i64;

    for i in 0..num_chunks {
        let start = min_pk + (i as i64) * step;
        let end = if i == num_chunks - 1 {
            max_pk + 1  // last chunk is inclusive of max
        } else {
            start + step
        };
        chunks.push(Chunk {
            partition_id: i,
            start_pk: start,
            end_pk: end,
        });
    }

    Ok(chunks)
}

/// Find the name of the first integer primary key column of a table.
/// Returns None if no integer PK is found.
async fn find_integer_pk(client: &Client, table_name: &str) -> Result<Option<String>> {
    // Parse schema.table or just table
    let (schema, table) = if table_name.contains('.') {
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    };

    let rows = client.query(
        "SELECT a.attname
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
         JOIN pg_class c ON c.oid = i.indrelid
         JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_type t ON t.oid = a.atttypid
         WHERE i.indisprimary
           AND n.nspname = $1
           AND c.relname = $2
           AND t.typname IN ('int2', 'int4', 'int8')
         ORDER BY a.attnum
         LIMIT 1",
        &[&schema, &table],
    ).await.with_context(|| format!("failed to find integer PK for {}", table_name))?;

    Ok(rows.first().map(|r| r.get::<_, String>(0)))
}
