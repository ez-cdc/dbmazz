// Copyright 2025
// Licensed under the Elastic License v2.0

//! SQL Server target setup: creates metadata schema, metadata tables,
//! and target tables for CDC replication.

use anyhow::{Context, Result};
use tiberius::Client;
use tiberius::ToSql;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tracing::info;

use super::types;
use crate::core::traits::SourceTableSchema;

/// Metadata schema name in the target database
const METADATA_SCHEMA: &str = "_dbmazz";

/// Run full setup for SQL Server target.
///
/// Creates:
/// 1. `_dbmazz` schema (idempotent via `sys.schemas` check)
/// 2. `_dbmazz._metadata` tracking table
/// 3. `_dbmazz._DBMAZZ_METADATA` key-value table (verify compatibility)
/// 4. Metadata row for this `job_name` (idempotent)
/// 5. Target tables per `source_schemas`
pub async fn run_setup(
    client: &mut Client<Compat<TcpStream>>,
    target_schema: &str,
    job_name: &str,
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    // 1. Create metadata schema
    let mut sql = String::from("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("') EXEC('CREATE SCHEMA [");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("]')");
    client
        .execute(sql.as_str(), &[])
        .await
        .context("Failed to create _dbmazz schema")?;
    info!("  [OK] Schema [{}] exists", METADATA_SCHEMA);

    // 2. Create metadata tracking table
    create_metadata_table(client).await?;

    // 3. Create _DBMAZZ_METADATA table for verify compatibility
    create_dbmazz_metadata_table(client).await?;

    // 4. Initialize metadata row for this job (if not exists)
    let mut sql = String::from("IF NOT EXISTS (SELECT 1 FROM [");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_metadata] WHERE [job_name] = @P1) INSERT INTO [");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_metadata] ([job_name]) VALUES (@P1)");
    client
        .execute(sql.as_str(), &[&job_name])
        .await
        .context("Failed to initialize metadata row")?;
    info!("  [OK] Metadata row initialized for job '{}'", job_name);

    // 5. Create target tables
    for schema in source_schemas {
        create_target_table(client, target_schema, schema).await?;
    }

    Ok(())
}

/// Create the `_dbmazz._metadata` tracking table.
///
/// Columns:
/// - `job_name` NVARCHAR(256) PRIMARY KEY — CDC job identifier
/// - `lsn_offset` BIGINT DEFAULT 0 — last processed LSN offset
/// - `sync_batch_id` BIGINT DEFAULT 0 — last sync batch ID
async fn create_metadata_table(client: &mut Client<Compat<TcpStream>>) -> Result<()> {
    let mut sql = String::from("IF OBJECT_ID(N'[");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_metadata]', N'U') IS NULL CREATE TABLE [");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_metadata] (\n    [job_name]      NVARCHAR(256) PRIMARY KEY,\n    [lsn_offset]    BIGINT NOT NULL DEFAULT 0,\n    [sync_batch_id] BIGINT NOT NULL DEFAULT 0\n)");

    client
        .execute(sql.as_str(), &[])
        .await
        .context("Failed to create metadata table")?;
    info!(
        "  [OK] Metadata table [{}].[_metadata] ready",
        METADATA_SCHEMA
    );
    Ok(())
}

/// Create the `_dbmazz._DBMAZZ_METADATA` table used by verify for schema
/// evolution compatibility checks.
///
/// A simple key-value store seeded with a `CDC_INIT` row on creation.
async fn create_dbmazz_metadata_table(client: &mut Client<Compat<TcpStream>>) -> Result<()> {
    let mut sql = String::from("IF OBJECT_ID(N'[");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_DBMAZZ_METADATA]', N'U') IS NULL CREATE TABLE [");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_DBMAZZ_METADATA] (\n    [key]   NVARCHAR(256) NOT NULL,\n    [value] NVARCHAR(MAX) NULL\n)");

    client
        .execute(sql.as_str(), &[])
        .await
        .context("Failed to create _DBMAZZ_METADATA table")?;

    // Seed initial row (idempotent)
    let mut sql = String::from("IF NOT EXISTS (SELECT 1 FROM [");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_DBMAZZ_METADATA] WHERE [key] = N'CDC_INIT') INSERT INTO [");
    sql.push_str(METADATA_SCHEMA);
    sql.push_str("].[_DBMAZZ_METADATA] ([key], [value]) VALUES (N'CDC_INIT', N'initialized')");

    client
        .execute(sql.as_str(), &[])
        .await
        .context("Failed to initialize _DBMAZZ_METADATA row")?;

    info!("  [OK] _DBMAZZ_METADATA table ready");
    Ok(())
}

/// Create a target table based on source schema, if it doesn't exist.
///
/// If the table already exists, audit columns are verified and added if
/// missing (`_dbmazz_synced_at DATETIME2`, `_dbmazz_op_type SMALLINT`).
/// If the table does not exist, it is created with all source columns
/// plus audit columns and a PRIMARY KEY constraint.
pub async fn create_target_table(
    client: &mut Client<Compat<TcpStream>>,
    schema: &str,
    source: &SourceTableSchema,
) -> Result<()> {
    // Check if table already exists
    let check_sql = "SELECT 1 FROM sys.tables t \
                      JOIN sys.schemas s ON t.schema_id = s.schema_id \
                      WHERE s.name = @P1 AND t.name = @P2";

    let stream = client
        .query(
            check_sql,
            &[&schema as &dyn ToSql, &source.name as &dyn ToSql],
        )
        .await
        .context("Failed to check table existence")?;
    let rows = stream.into_first_result().await?;
    let exists = !rows.is_empty();

    if exists {
        // Ensure audit columns exist (MERGE needs them)
        let full_name: String = {
            let mut s = String::from("[");
            s.push_str(schema);
            s.push_str("].[");
            s.push_str(&source.name);
            s.push(']');
            s
        };

        let col_check_sql = "SELECT 1 FROM sys.columns \
                              WHERE object_id = OBJECT_ID(@P1) AND name = @P2";

        let stream = client
            .query(
                col_check_sql,
                &[
                    &full_name as &dyn ToSql,
                    &"_dbmazz_synced_at".to_string() as &dyn ToSql,
                ],
            )
            .await
            .context("Failed to check _dbmazz_synced_at column")?;
        let rows = stream.into_first_result().await?;
        if rows.is_empty() {
            let mut alter = String::from("ALTER TABLE ");
            alter.push_str(&full_name);
            alter.push_str(" ADD [_dbmazz_synced_at] DATETIME2 NULL");
            client.execute(alter.as_str(), &[]).await.with_context(|| {
                format!(
                    "Failed to add _dbmazz_synced_at to [{}.{}]",
                    schema, source.name
                )
            })?;
        }

        let stream = client
            .query(
                col_check_sql,
                &[
                    &full_name as &dyn ToSql,
                    &"_dbmazz_op_type".to_string() as &dyn ToSql,
                ],
            )
            .await
            .context("Failed to check _dbmazz_op_type column")?;
        let rows = stream.into_first_result().await?;
        if rows.is_empty() {
            let mut alter = String::from("ALTER TABLE ");
            alter.push_str(&full_name);
            alter.push_str(" ADD [_dbmazz_op_type] SMALLINT NULL");
            client.execute(alter.as_str(), &[]).await.with_context(|| {
                format!(
                    "Failed to add _dbmazz_op_type to [{}.{}]",
                    schema, source.name
                )
            })?;
        }

        info!(
            "  [OK] Table [{}].[{}] already exists (metadata columns verified)",
            schema, source.name
        );
        return Ok(());
    }

    // Build CREATE TABLE DDL
    let mut col_defs: Vec<String> = Vec::new();

    for col in &source.columns {
        let sqlserver_type = types::data_type_to_sqlserver(&col.data_type);
        let nullable = if col.nullable { "NULL" } else { "NOT NULL" };
        col_defs.push(format!(
            "    [{}] {} {}",
            col.name, sqlserver_type, nullable
        ));
    }

    // Add audit columns
    col_defs.push("    [_dbmazz_synced_at] DATETIME2 NULL".to_string());
    col_defs.push("    [_dbmazz_op_type] SMALLINT NULL".to_string());

    // Add primary key constraint
    let pk_clause = if source.primary_keys.is_empty() {
        String::new()
    } else {
        let pk_cols: Vec<String> = source
            .primary_keys
            .iter()
            .map(|k| format!("[{}]", k))
            .collect();
        format!(
            ",\n    CONSTRAINT [PK_{}] PRIMARY KEY ({})",
            source.name,
            pk_cols.join(", ")
        )
    };

    let mut ddl = String::from("CREATE TABLE [");
    ddl.push_str(schema);
    ddl.push_str("].[");
    ddl.push_str(&source.name);
    ddl.push_str("] (\n");
    ddl.push_str(&col_defs.join(",\n"));
    ddl.push_str(&pk_clause);
    ddl.push_str("\n)");

    client
        .execute(ddl.as_str(), &[])
        .await
        .with_context(|| format!("Failed to create target table [{}.{}]", schema, source.name))?;

    info!(
        "  [OK] Created table [{}].[{}] ({} columns, {} PKs)",
        schema,
        source.name,
        source.columns.len(),
        source.primary_keys.len()
    );

    Ok(())
}
