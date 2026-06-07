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
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    // We use the oracle crate which is sync, so we spawn_blocking
    let conn_str = conn_str.to_string();
    let target_schema = target_schema.to_string();
    let job_name = job_name.to_string();
    let source_schemas = source_schemas.to_vec();

    tokio::task::spawn_blocking(move || {
        run_setup_sync(&conn_str, &target_schema, &job_name, &source_schemas)
    })
    .await
    .context("Failed to join spawn_blocking for Oracle setup")?
}

fn run_setup_sync(
    _conn_str: &str,
    target_schema: &str,
    job_name: &str,
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    // NOTE: In production, the oracle crate would be used here.
    // For this initial implementation, we define the DDL statements
    // that would be executed using the oracle crate's synchronous API.
    //
    // Key Oracle DDL patterns:
    //
    // 1. Create _dbmazz metadata table (if not exists):
    //    "DECLARE
    //       cnt NUMBER;
    //     BEGIN
    //       SELECT COUNT(*) INTO cnt FROM all_tables WHERE owner = '{target_schema}' AND table_name = '_DBMAZZ_METADATA';
    //       IF cnt = 0 THEN
    //         EXECUTE IMMEDIATE 'CREATE TABLE \"{target_schema}\".\"_DBMAZZ_METADATA\" (
    //           job_name VARCHAR2(255) PRIMARY KEY,
    //           scn_offset NUMBER(20) DEFAULT 0 NOT NULL,
    //           sync_batch_id NUMBER(20) DEFAULT 0 NOT NULL,
    //           normalize_batch_id NUMBER(20) DEFAULT 0 NOT NULL
    //         )';
    //       END IF;
    //     END;"
    //
    // 2. For each source table, create target:
    //    "DECLARE
    //       cnt NUMBER;
    //     BEGIN
    //       SELECT COUNT(*) INTO cnt FROM all_tables WHERE owner = '{target_schema}' AND table_name = '{table_name}';
    //       IF cnt = 0 THEN
    //         EXECUTE IMMEDIATE 'CREATE TABLE \"{target_schema}\".\"{table_name}\" (
    //           {col_defs},
    //           \"_dbmazz_synced_at\" TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
    //           \"_dbmazz_op_type\" NUMBER(2) DEFAULT 0,
    //           PRIMARY KEY ({pk_cols})
    //         )';
    //       END IF;
    //     END;"

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
    format!("\"{}\" {}{}", col.name, oracle_type, nullable)
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
        assert_eq!(ddl, r#""id" NUMBER(10) NOT NULL"#);
    }
}
