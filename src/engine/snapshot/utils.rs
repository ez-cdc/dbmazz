// Copyright 2025
// Licensed under the Elastic License v2.0

//! Shared utilities for snapshot modules.

use anyhow::{Context, Result};
use tokio_postgres::Client;

/// Find the name of the first integer primary key column of a table.
/// Returns None if no integer PK is found.
pub async fn find_integer_pk_column(client: &Client, table_name: &str) -> Result<Option<String>> {
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
