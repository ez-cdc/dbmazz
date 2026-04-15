// Copyright 2025
// Licensed under the Elastic License v2.0

//! PostgreSQL target setup: creates raw table, metadata table, and target tables.

use std::collections::HashMap;

use anyhow::{Context, Result};
use tokio_postgres::Client;
use tracing::info;

use super::schema_tracking;
use super::types::pg_oid_to_target_type;
use crate::core::traits::SourceTableSchema;

/// Metadata schema name in the target database
const METADATA_SCHEMA: &str = "_dbmazz";

/// Run full setup for PostgreSQL target.
pub async fn run_setup(
    client: &Client,
    target_schema: &str,
    job_name: &str,
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    // 1. Create metadata schema
    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            METADATA_SCHEMA
        ))
        .await
        .context("Failed to create _dbmazz schema")?;
    info!("  [OK] Schema {} exists", METADATA_SCHEMA);

    // 2. Create raw table
    create_raw_table(client, job_name).await?;

    // 3. Create metadata table
    create_metadata_table(client).await?;

    // 4. Initialize metadata row for this job (if not exists)
    client
        .execute(
            &format!(
                "INSERT INTO {}.\"_metadata\" (job_name) VALUES ($1) ON CONFLICT DO NOTHING",
                METADATA_SCHEMA
            ),
            &[&job_name],
        )
        .await
        .context("Failed to initialize metadata row")?;

    // 5. Create target tables
    for schema in source_schemas {
        create_target_table(client, target_schema, schema).await?;
    }

    Ok(())
}

/// Create the raw staging table for CDC records.
async fn create_raw_table(client: &Client, job_name: &str) -> Result<()> {
    let safe_name = sanitize_identifier(job_name);
    let raw_table = format!("{}._raw_{}", METADATA_SCHEMA, safe_name);

    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {} (
            _uid           UUID    NOT NULL DEFAULT gen_random_uuid(),
            _timestamp     BIGINT  NOT NULL,
            _dst_table     TEXT    NOT NULL,
            _data          JSONB   NOT NULL,
            _record_type   SMALLINT NOT NULL,
            _match_data    JSONB,
            _batch_id      BIGINT,
            _toast_columns TEXT
        )"#,
        raw_table
    );

    client
        .batch_execute(&ddl)
        .await
        .with_context(|| format!("Failed to create raw table {}", raw_table))?;

    // Index on batch_id for normalize queries
    let idx_batch = format!(
        "CREATE INDEX IF NOT EXISTS idx_{}_batch ON {} (_batch_id)",
        safe_name, raw_table
    );
    client
        .batch_execute(&idx_batch)
        .await
        .with_context(|| format!("Failed to create batch index on {}", raw_table))?;

    // Index on destination table for per-table MERGE lookups
    let idx_dst = format!(
        "CREATE INDEX IF NOT EXISTS idx_{}_dst ON {} (_dst_table)",
        safe_name, raw_table
    );
    client
        .batch_execute(&idx_dst)
        .await
        .with_context(|| format!("Failed to create dst_table index on {}", raw_table))?;

    info!("  [OK] Raw table {} ready", raw_table);
    Ok(())
}

/// Create the metadata tracking table.
async fn create_metadata_table(client: &Client) -> Result<()> {
    let ddl = format!(
        r#"CREATE TABLE IF NOT EXISTS {}._metadata (
            job_name           TEXT PRIMARY KEY,
            lsn_offset         BIGINT NOT NULL DEFAULT 0,
            sync_batch_id      BIGINT NOT NULL DEFAULT 0,
            normalize_batch_id BIGINT NOT NULL DEFAULT 0
        )"#,
        METADATA_SCHEMA
    );

    client
        .batch_execute(&ddl)
        .await
        .context("Failed to create metadata table")?;

    info!(
        "  [OK] Metadata table {}.\"_metadata\" ready",
        METADATA_SCHEMA
    );
    Ok(())
}

/// Create a target table based on source schema, if it doesn't exist.
async fn create_target_table(
    client: &Client,
    target_schema: &str,
    source: &SourceTableSchema,
) -> Result<()> {
    // Check if table already exists
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&target_schema, &source.name],
        )
        .await
        .context("Failed to check table existence")?
        .get(0);

    if exists {
        // Ensure metadata columns exist (MERGE needs them)
        client
            .batch_execute(&format!(
                r#"ALTER TABLE "{}"."{}" ADD COLUMN IF NOT EXISTS _dbmazz_synced_at TIMESTAMPTZ;
                   ALTER TABLE "{}"."{}" ADD COLUMN IF NOT EXISTS _dbmazz_op_type SMALLINT"#,
                target_schema, source.name, target_schema, source.name,
            ))
            .await
            .with_context(|| {
                format!(
                    "Failed to add metadata columns to {}.{}",
                    target_schema, source.name
                )
            })?;

        info!(
            "  [OK] Table \"{}\".\"{}\" already exists (metadata columns verified)",
            target_schema, source.name
        );
        return Ok(());
    }

    // Build CREATE TABLE DDL
    let mut col_defs: Vec<String> = Vec::new();

    for col in &source.columns {
        let pg_type = pg_oid_to_target_type(col.pg_type_id);
        let nullable = if col.nullable { "" } else { " NOT NULL" };
        col_defs.push(format!("    \"{}\" {}{}", col.name, pg_type, nullable));
    }

    // Add audit columns
    col_defs.push("    _dbmazz_synced_at TIMESTAMPTZ DEFAULT now()".to_string());
    col_defs.push("    _dbmazz_op_type SMALLINT DEFAULT 0".to_string());

    // Add primary key constraint
    let pk_clause = if source.primary_keys.is_empty() {
        String::new()
    } else {
        let pk_cols: Vec<String> = source
            .primary_keys
            .iter()
            .map(|k| format!("\"{}\"", k))
            .collect();
        format!(",\n    PRIMARY KEY ({})", pk_cols.join(", "))
    };

    let ddl = format!(
        "CREATE TABLE \"{}\".\"{}\" (\n{}{}\n)",
        target_schema,
        source.name,
        col_defs.join(",\n"),
        pk_clause
    );

    client.batch_execute(&ddl).await.with_context(|| {
        format!(
            "Failed to create target table {}.{}",
            target_schema, source.name
        )
    })?;

    info!(
        "  [OK] Created table \"{}\".\"{}\" ({} columns, {} PKs)",
        target_schema,
        source.name,
        source.columns.len(),
        source.primary_keys.len()
    );

    Ok(())
}

/// Bootstrap the schema tracking state at sink startup.
///
/// Creates `_dbmazz._schema_tracking` if missing. If the table has no rows
/// for this `job_name`, this is a first-ever run → seed from `source_schemas`.
/// Otherwise, this is a restart → reconcile the tracked state against the
/// (possibly evolved) source schemas, applying any catch-up `ALTER TABLE`s
/// in a single atomic transaction.
///
/// Returns the in-memory `HashMap<qualified_name, SourceTableSchema>` that
/// reflects what the target now has (post-reconcile). The caller wraps this
/// in `Arc` and a `watch::channel` for the normalizer.
pub async fn initialize_schema_state(
    client: &mut Client,
    target_schema: &str,
    job_name: &str,
    source_schemas: &[SourceTableSchema],
) -> Result<HashMap<String, SourceTableSchema>> {
    // 1. Ensure the tracking table exists (idempotent DDL).
    schema_tracking::create_tracking_table(&*client)
        .await
        .context("initialize_schema_state: failed to create schema tracking table")?;

    // 2. Count existing rows for this job_name to determine first-run vs restart.
    let count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*) FROM {} WHERE job_name = $1",
                schema_tracking::TRACKING_TABLE
            ),
            &[&job_name],
        )
        .await
        .with_context(|| {
            format!(
                "initialize_schema_state: failed to count tracking rows for job '{}'",
                job_name
            )
        })?
        .get(0);

    if count == 0 {
        // 3a. First-ever run: seed the tracking table from source_schemas.
        schema_tracking::seed_initial_state(client, job_name, source_schemas)
            .await
            .context("initialize_schema_state: seed_initial_state failed")?;

        // Build the in-memory map directly from source_schemas — avoids a
        // redundant DB round-trip and is safe because we just inserted exactly
        // these rows.
        let mut map = HashMap::with_capacity(source_schemas.len());
        for schema in source_schemas {
            let qn = format!("{}.{}", schema.schema, schema.name);
            map.insert(qn, schema.clone());
        }

        info!(
            "  [OK] initialize_schema_state: seeded {} tables for job '{}'",
            source_schemas.len(),
            job_name
        );
        Ok(map)
    } else {
        // 3b. Restart path: load the tracked state and reconcile against source.
        let source_pk_lookup: HashMap<String, Vec<String>> = source_schemas
            .iter()
            .map(|s| {
                let qn = format!("{}.{}", s.schema, s.name);
                (qn, s.primary_keys.clone())
            })
            .collect();

        let loaded_cache =
            schema_tracking::load_tracking_state(&*client, job_name, &source_pk_lookup)
                .await
                .context("initialize_schema_state: load_tracking_state failed")?;

        let reconciled = schema_tracking::reconcile_on_startup(
            client,
            target_schema,
            job_name,
            source_schemas,
            loaded_cache,
        )
        .await
        .context("initialize_schema_state: reconcile_on_startup failed")?;

        info!(
            "  [OK] initialize_schema_state: reconciled {} tables for job '{}'",
            reconciled.len(),
            job_name
        );
        Ok(reconciled)
    }
}

/// Sanitize an identifier for use in table names.
/// Replaces non-alphanumeric characters with underscores.
fn sanitize_identifier(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_identifier() {
        assert_eq!(sanitize_identifier("dbmazz_slot"), "dbmazz_slot");
        assert_eq!(sanitize_identifier("my-slot.name"), "my_slot_name");
        assert_eq!(sanitize_identifier("slot 123"), "slot_123");
    }
}
