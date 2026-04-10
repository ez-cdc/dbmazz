// Copyright 2025
// Licensed under the Elastic License v2.0

//! Snowflake DDL setup: creates internal schema, stage, raw table, metadata, and target tables.

use anyhow::{Context, Result};
use tracing::{info, warn};

use super::client::SnowflakeClient;
use super::types::TypeMapper;
use crate::core::traits::SourceTableSchema;

/// Internal schema name in the target database (TRANSIENT, no fail-safe)
const INTERNAL_SCHEMA: &str = "_DBMAZZ";

/// Run full DDL setup for Snowflake target.
pub async fn run_setup(
    client: &SnowflakeClient,
    database: &str,
    target_schema: &str,
    job_name: &str,
    source_schemas: &[SourceTableSchema],
    soft_delete: bool,
) -> Result<()> {
    let safe_job = sanitize_identifier(job_name);
    let type_mapper = TypeMapper::new();

    // 1. Create internal TRANSIENT schema (no fail-safe, cheaper)
    client
        .execute(&format!(
            "CREATE TRANSIENT SCHEMA IF NOT EXISTS {}.{}",
            database, INTERNAL_SCHEMA
        ))
        .await
        .context("Failed to create _DBMAZZ schema")?;
    info!("  [OK] Schema {}.{} exists", database, INTERNAL_SCHEMA);

    // 2. Create internal stage (Parquet format)
    client
        .execute(&format!(
            "CREATE STAGE IF NOT EXISTS {}.{}.STAGE_{} FILE_FORMAT = (TYPE = PARQUET)",
            database, INTERNAL_SCHEMA, safe_job
        ))
        .await
        .context("Failed to create stage")?;
    info!(
        "  [OK] Stage {}.{}.STAGE_{} ready",
        database, INTERNAL_SCHEMA, safe_job
    );

    // 3. Create raw table (TRANSIENT, shared for all tables)
    let raw_table = format!("{}._RAW_{}", INTERNAL_SCHEMA, safe_job);
    client
        .execute(&format!(
            r#"CREATE TRANSIENT TABLE IF NOT EXISTS {}.{} (
                _TIMESTAMP      NUMBER(20,0)  NOT NULL,
                _DST_TABLE      VARCHAR       NOT NULL,
                _DATA           VARIANT       NOT NULL,
                _RECORD_TYPE    NUMBER(3,0)   NOT NULL,
                _MATCH_DATA     VARIANT,
                _BATCH_ID       NUMBER(20,0),
                _TOAST_COLUMNS  VARCHAR
            )"#,
            database, raw_table
        ))
        .await
        .context("Failed to create raw table")?;
    info!("  [OK] Raw table {}.{} ready", database, raw_table);

    // 4. Create metadata table
    client
        .execute(&format!(
            r#"CREATE TABLE IF NOT EXISTS {}.{}._METADATA (
                JOB_NAME           VARCHAR PRIMARY KEY,
                LSN_OFFSET         NUMBER(20,0) NOT NULL DEFAULT 0,
                SYNC_BATCH_ID      NUMBER(20,0) NOT NULL DEFAULT 0,
                NORMALIZE_BATCH_ID NUMBER(20,0) NOT NULL DEFAULT 0
            )"#,
            database, INTERNAL_SCHEMA
        ))
        .await
        .context("Failed to create metadata table")?;

    // 5. Initialize metadata row
    client
        .execute(&format!(
            "INSERT INTO {}.{}._METADATA (JOB_NAME) \
             SELECT '{}' WHERE NOT EXISTS (\
                 SELECT 1 FROM {}.{}._METADATA WHERE JOB_NAME = '{}'\
             )",
            database, INTERNAL_SCHEMA, safe_job, database, INTERNAL_SCHEMA, safe_job
        ))
        .await
        .context("Failed to initialize metadata row")?;
    info!("  [OK] Metadata table ready");

    // 5b. Create batch ID sequence (globally unique across Primary + SnapshotWorkers)
    // Used by flush_staged_files to atomically assign batch_ids that the normalizer
    // reads from the metadata table to find pending batches.
    client
        .execute(&format!(
            "CREATE SEQUENCE IF NOT EXISTS {}.{}.SEQ_BATCH_{} START = 1 INCREMENT = 1",
            database, INTERNAL_SCHEMA, safe_job
        ))
        .await
        .context("Failed to create batch sequence")?;
    info!(
        "  [OK] Sequence {}.{}.SEQ_BATCH_{} ready",
        database, INTERNAL_SCHEMA, safe_job
    );

    // 6. Create target schema if not default
    if target_schema.to_uppercase() != "PUBLIC" {
        client
            .execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                database, target_schema
            ))
            .await
            .context("Failed to create target schema")?;
    }

    // 7. Create target tables
    for schema in source_schemas {
        create_target_table(
            client,
            database,
            target_schema,
            schema,
            soft_delete,
            &type_mapper,
        )
        .await?;
    }

    Ok(())
}

/// Creates a target table based on source schema, with audit columns.
async fn create_target_table(
    client: &SnowflakeClient,
    database: &str,
    target_schema: &str,
    source: &SourceTableSchema,
    soft_delete: bool,
    type_mapper: &TypeMapper,
) -> Result<()> {
    let table_name = source.name.to_uppercase();

    // Build column definitions
    let mut col_defs: Vec<String> = Vec::new();
    for col in &source.columns {
        let sf_type = type_mapper.pg_type_to_snowflake(col.pg_type_id);
        let nullable = if col.nullable { "" } else { " NOT NULL" };
        col_defs.push(format!(
            "    \"{}\" {}{}",
            col.name.to_uppercase(),
            sf_type,
            nullable
        ));
    }

    // Audit columns
    col_defs.push("    \"_DBMAZZ_OP_TYPE\" NUMBER(3,0) DEFAULT 0".to_string());
    if soft_delete {
        col_defs.push("    \"_DBMAZZ_IS_DELETED\" BOOLEAN DEFAULT FALSE".to_string());
    }
    col_defs
        .push("    \"_DBMAZZ_SYNCED_AT\" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()".to_string());
    col_defs.push("    \"_DBMAZZ_CDC_VERSION\" NUMBER(20,0) DEFAULT 0".to_string());

    // Primary key
    let pk_clause = if source.primary_keys.is_empty() {
        String::new()
    } else {
        let pk_cols: Vec<String> = source
            .primary_keys
            .iter()
            .map(|k| format!("\"{}\"", k.to_uppercase()))
            .collect();
        format!(",\n    PRIMARY KEY ({})", pk_cols.join(", "))
    };

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {}.{}.\"{}\" (\n{}{}\n)",
        database,
        target_schema,
        table_name,
        col_defs.join(",\n"),
        pk_clause
    );

    client.execute(&ddl).await.with_context(|| {
        format!(
            "Failed to create target table {}.{}",
            target_schema, table_name
        )
    })?;

    // Add audit columns to existing tables (idempotent).
    // Snowflake does NOT support DEFAULT with expressions (e.g. CURRENT_TIMESTAMP())
    // or even DEFAULT FALSE in ALTER TABLE ADD COLUMN. Only literal constants or
    // no default at all. The MERGE sets these values explicitly, so no default needed.
    let alter_stmts = [
        format!(
            "ALTER TABLE {}.{}.\"{}\" ADD COLUMN IF NOT EXISTS \"_DBMAZZ_OP_TYPE\" NUMBER(3,0)",
            database, target_schema, table_name
        ),
        format!(
            "ALTER TABLE {}.{}.\"{}\" ADD COLUMN IF NOT EXISTS \"_DBMAZZ_SYNCED_AT\" TIMESTAMP_NTZ",
            database, target_schema, table_name
        ),
        format!(
            "ALTER TABLE {}.{}.\"{}\" ADD COLUMN IF NOT EXISTS \"_DBMAZZ_CDC_VERSION\" NUMBER(20,0)",
            database, target_schema, table_name
        ),
    ];

    for stmt in &alter_stmts {
        if let Err(e) = client.execute(stmt).await {
            warn!("ALTER TABLE failed (may be harmless if column exists): {:#}", e);
        }
    }

    if soft_delete {
        if let Err(e) = client
            .execute(&format!(
                "ALTER TABLE {}.{}.\"{}\" ADD COLUMN IF NOT EXISTS \"_DBMAZZ_IS_DELETED\" BOOLEAN",
                database, target_schema, table_name
            ))
            .await
        {
            warn!("ALTER TABLE failed (may be harmless if column exists): {:#}", e);
        }
    }

    info!(
        "  [OK] Target table {}.{}.\"{}\" ready ({} cols, {} PKs)",
        database,
        target_schema,
        table_name,
        source.columns.len(),
        source.primary_keys.len()
    );

    Ok(())
}

fn sanitize_identifier(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_identifier() {
        assert_eq!(sanitize_identifier("dbmazz_slot"), "DBMAZZ_SLOT");
        assert_eq!(sanitize_identifier("my-slot.name"), "MY_SLOT_NAME");
    }
}
