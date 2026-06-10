// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle target setup: creates metadata tables and target tables.

use anyhow::{Context, Result};
use tracing::info;

use super::types;
use crate::core::traits::{SourceColumn, SourceTableSchema};

/// Create _dbmazz metadata schema elements.
/// Oracle does not have CREATE SCHEMA in the same way as PG;
/// we create the _dbmazz user/schema and the metadata table.
pub async fn run_setup(
    conn_str: &str,
    target_schema: &str,
    job_name: &str,
    user: &str,
    password: &str,
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    // We use the oracle crate which is sync, so we spawn_blocking
    let conn_str = conn_str.to_string();
    let target_schema = target_schema.to_string();
    let job_name = job_name.to_string();
    let user = user.to_string();
    let password = password.to_string();
    let source_schemas = source_schemas.to_vec();

    tokio::task::spawn_blocking(move || {
        run_setup_sync(&conn_str, &target_schema, &job_name, &user, &password, &source_schemas)
    })
    .await
    .context("Failed to join spawn_blocking for Oracle setup")?
}

fn run_setup_sync(
    conn_str: &str,
    target_schema: &str,
    job_name: &str,
    user: &str,
    password: &str,
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    let conn = oracle::Connection::connect(user, password, conn_str)
        .with_context(|| format!("OracleSink: failed to connect for setup at {}", conn_str))?;

    // 1. Create _DBMAZZ_METADATA table if it doesn't exist
    let create_metadata_sql = format!(
        "DECLARE
           cnt NUMBER;
         BEGIN
           SELECT COUNT(*) INTO cnt FROM all_tables
           WHERE owner = '{target_schema}' AND table_name = '_DBMAZZ_METADATA';
           IF cnt = 0 THEN
             EXECUTE IMMEDIATE 'CREATE TABLE \"{target_schema}\".\"_DBMAZZ_METADATA\" (
               job_name VARCHAR2(255) PRIMARY KEY,
               scn_offset NUMBER(20) DEFAULT 0 NOT NULL,
               sync_batch_id NUMBER(20) DEFAULT 0 NOT NULL,
               normalize_batch_id NUMBER(20) DEFAULT 0 NOT NULL
             )';
           END IF;
         END;"
    );
    conn.execute(&create_metadata_sql, &[])
        .context("OracleSink: failed to create _DBMAZZ_METADATA table")?;
    info!("  [OK] _DBMAZZ_METADATA table ready in schema '{}'", target_schema);

    // Insert initial metadata row so A4 check passes
    let insert_meta_sql = format!(
        "MERGE INTO \"{target_schema}\".\"_DBMAZZ_METADATA\" t
         USING (SELECT '{job_name}' AS jn FROM DUAL) s
         ON (t.job_name = s.jn)
         WHEN NOT MATCHED THEN
           INSERT (job_name, scn_offset, sync_batch_id, normalize_batch_id)
           VALUES ('{job_name}', 0, 0, 0)"
    );
    conn.execute(&insert_meta_sql, &[])
        .context("OracleSink: failed to insert initial metadata row")?;
    info!("  [OK] _DBMAZZ_METADATA initial row inserted for job '{}'", job_name);

    // 2. For each source table, create the target table if it doesn't exist
    for source_schema in source_schemas {
        let table_name = source_schema.name.to_uppercase();
        let col_defs: Vec<String> = source_schema.columns.iter()
            .map(|col| column_ddl(col))
            .collect();
        let pk_cols: Vec<String> = source_schema.primary_keys.iter()
            .map(|pk| format!("\"{}\"", pk.to_uppercase()))
            .collect();

        let create_table_sql = format!(
            "DECLARE
               cnt NUMBER;
             BEGIN
               SELECT COUNT(*) INTO cnt FROM all_tables
               WHERE owner = '{target_schema}' AND table_name = '{table_name}';
               IF cnt = 0 THEN
                 EXECUTE IMMEDIATE 'CREATE TABLE \"{target_schema}\".\"{table_name}\" (
                   {col_defs},
                   \"DBMAZZ_SYNCED_AT\" TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
                   \"DBMAZZ_OP_TYPE\" NUMBER(2) DEFAULT 0,
                   \"DBMAZZ_CDC_VERSION\" NUMBER(20) DEFAULT 0,
                   \"DBMAZZ_IS_DELETED\" NUMBER(1) DEFAULT 0,
                   PRIMARY KEY ({pk_cols})
                 )';
               END IF;
             END;",
            target_schema = target_schema,
            table_name = table_name,
            col_defs = col_defs.join(",\n"),
            pk_cols = pk_cols.join(", "),
        );

        conn.execute(&create_table_sql, &[])
            .with_context(|| format!("OracleSink: failed to create table '{}.{}'", target_schema, table_name))?;
        info!("  [OK] Table \"{}\".\"{}\" ready", target_schema, table_name);
    }

    conn.close()?;

    info!(
        "OracleSink: setup complete for {} tables (schema: {}, job: {})",
        source_schemas.len(),
        target_schema,
        job_name
    );
    Ok(())
}

/// Build Oracle-compatible column DDL string from SourceColumn.
pub fn column_ddl(col: &SourceColumn) -> String {
    let oracle_type = types::column_type(col.data_type.clone(), col.pg_type_id);
    let nullable = if col.nullable { "" } else { " NOT NULL" };
    format!("\"{}\" {}{}", col.name.to_uppercase(), oracle_type, nullable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;
    use crate::core::traits::SourceColumn;

    #[test]
    fn test_column_ddl() {
        let col = SourceColumn {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            pg_type_id: None,
        };
        let ddl = column_ddl(&col);
        assert_eq!(ddl, r#""ID" NUMBER(10) NOT NULL"#);
    }
}
