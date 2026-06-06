// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle Schema Setup
//!
//! This module handles schema setup and management for Oracle tables,
//! including:
//! - Connection validation
//! - Table existence verification
//! - CDC audit column creation
//! - Target table creation

use anyhow::{anyhow, Result};
use tracing::info;

use super::client::OracleClient;
use super::config::OracleSinkConfig;
use super::types::TypeMapper;
use crate::core::traits::SourceTableSchema;
use crate::utils::validate_sql_identifier;

/// CDC audit columns that must exist in all replicated Oracle tables.
///
/// Column names are UPPERCASE because Oracle stores object names in uppercase
/// by default, and case-insensitive queries via `UPPER()` match this convention.
const AUDIT_COLUMNS: &[(&str, &str)] = &[
    (
        "DBMazz_OP_TYPE",
        "NUMBER(1) DEFAULT 0",
    ),
    (
        "DBMazz_IS_DELETED",
        "NUMBER(1) DEFAULT 0",
    ),
    (
        "DBMazz_SYNCED_AT",
        "TIMESTAMP(6) DEFAULT SYSTIMESTAMP",
    ),
    (
        "DBMazz_CDC_VERSION",
        "NUMBER(19) DEFAULT 0",
    ),
];

/// Oracle schema setup and management.
///
/// Handles DDL operations via the Oracle client including:
/// - Connection validation
/// - Table existence checks
/// - Audit column management
pub struct OracleSetup {
    /// Oracle client for DDL operations
    client: OracleClient,
    /// Type mapper for schema generation
    type_mapper: TypeMapper,
    /// Target schema
    schema: String,
}

impl OracleSetup {
    /// Creates a new Oracle setup instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Oracle sink configuration
    ///
    /// # Returns
    ///
    /// A new `OracleSetup` instance with initialized Oracle client
    pub fn new(config: &OracleSinkConfig) -> Self {
        let client = OracleClient::new(config);
        let type_mapper = TypeMapper::new();
        Self {
            client,
            type_mapper,
            schema: config.schema.clone(),
        }
    }

    /// Runs the complete setup process.
    ///
    /// This includes:
    /// 1. Verifying connection to Oracle
    /// 2. Creating target tables if they don't exist
    /// 3. Adding audit columns to existing tables
    ///
    /// # Arguments
    ///
    /// * `source_schemas` - List of source table schemas to replicate
    pub async fn run(&self, source_schemas: &[SourceTableSchema]) -> Result<()> {
        info!("Oracle Setup:");

        // 1. Verify connectivity
        self.verify_connection().await?;

        // 2. Create or verify target tables
        for schema in source_schemas {
            self.ensure_target_table(schema).await?;
        }

        info!("[OK] Oracle setup complete");
        Ok(())
    }

    /// Verifies connectivity to Oracle.
    async fn verify_connection(&self) -> Result<()> {
        self.client.verify_connection().await?;
        info!("  [OK] Oracle connection OK");
        Ok(())
    }

    /// Ensures a target table exists with all required columns.
    ///
    /// If the table does not exist, it is created with source columns,
    /// audit columns, and primary key constraint. If the table already
    /// exists, only missing audit columns are added.
    async fn ensure_target_table(&self, source: &SourceTableSchema) -> Result<()> {
        let table_name = &source.name;

        // Validate identifiers to prevent SQL injection
        validate_sql_identifier(table_name)
            .map_err(|e| anyhow!("Invalid table name '{}': {}", table_name, e))?;
        validate_sql_identifier(&self.schema)
            .map_err(|e| anyhow!("Invalid schema name '{}': {}", self.schema, e))?;

        let exists = self.client.table_exists(table_name).await?;

        if !exists {
            // Create the table with all columns plus audit columns
            let sql = self.generate_create_table_sql(source);
            info!("  Creating table \"{}\".\"{}\"", self.schema, table_name);
            self.client.execute_ddl(&sql).await?;
            info!("  [OK] Table \"{}\" created", table_name);
        } else {
            // Table exists — ensure audit columns are present
            info!("  Table \"{}\" already exists, ensuring audit columns", table_name);
            self.ensure_audit_columns(&[table_name.to_string()]).await?;
            info!("  [OK] Table \"{}\" audit columns verified", table_name);
        }

        Ok(())
    }

    /// Generates CREATE TABLE SQL from a source schema.
    ///
    /// Includes source columns, audit columns, and a PRIMARY KEY constraint
    /// if the source schema defines primary keys.
    fn generate_create_table_sql(&self, source: &SourceTableSchema) -> String {
        let mut sql = String::with_capacity(1024);
        sql.push_str(&format!(
            "CREATE TABLE \"{}\".\"{}\" (\n",
            self.schema, source.name
        ));

        let mut column_defs: Vec<String> = Vec::new();

        // Add source columns
        for col in &source.columns {
            let oracle_type = self.type_mapper.to_oracle_type(&col.data_type);
            let nullable = if col.nullable { "" } else { " NOT NULL" };
            column_defs.push(format!(
                "    \"{}\" {}{}",
                col.name, oracle_type, nullable
            ));
        }

        // Add audit columns
        for (name, def) in AUDIT_COLUMNS {
            column_defs.push(format!("    \"{}\" {}", name, def));
        }

        // Add PRIMARY KEY constraint if PKs exist
        if !source.primary_keys.is_empty() {
            let pk_cols: Vec<String> = source
                .primary_keys
                .iter()
                .map(|pk| format!("\"{}\"", pk))
                .collect();
            column_defs.push(format!(
                "    CONSTRAINT \"pk_{}\" PRIMARY KEY ({})",
                source.name,
                pk_cols.join(", ")
            ));
        }

        sql.push_str(&column_defs.join(",\n"));
        sql.push_str("\n)");

        info!(
            "OracleSetup: generated CREATE TABLE for \"{}\".\"{}\"",
            self.schema, source.name
        );

        sql
    }

    /// Ensures all tables have the required CDC audit columns.
    async fn ensure_audit_columns(&self, tables: &[String]) -> Result<()> {
        for table in tables {
            let table_name = table.split('.').next_back().unwrap_or(table);
            self.ensure_audit_columns_for_table(table_name).await?;
        }
        Ok(())
    }

    /// Adds audit columns to a specific table if they don't exist.
    ///
    /// Queries the existing columns for the table and adds any audit column
    /// that is missing. Uses `UPPER()` on column names for case-insensitive
    /// comparison since Oracle stores object names in uppercase by default.
    async fn ensure_audit_columns_for_table(&self, table: &str) -> Result<()> {
        // Validate identifiers to prevent SQL injection
        validate_sql_identifier(table)
            .map_err(|e| anyhow!("Invalid table name '{}': {}", table, e))?;
        validate_sql_identifier(&self.schema)
            .map_err(|e| anyhow!("Invalid schema name '{}': {}", self.schema, e))?;

        // Get existing columns (Oracle returns them in UPPERCASE)
        let existing_columns = self.client.get_table_columns(table).await?;
        let existing_upper: Vec<String> = existing_columns
            .iter()
            .map(|c| c.to_uppercase())
            .collect();

        for (col_name, col_def) in AUDIT_COLUMNS {
            // Validate column name (although these are constants, it's good practice)
            validate_sql_identifier(col_name)
                .map_err(|e| anyhow!("Invalid column name '{}': {}", col_name, e))?;

            if !existing_upper.contains(&col_name.to_uppercase()) {
                info!("  Adding audit column {} to \"{}\".\"{}\"", col_name, self.schema, table);

                let sql = format!(
                    "ALTER TABLE \"{}\".\"{}\" ADD (\"{}\" {})",
                    self.schema, table, col_name, col_def
                );

                self.client.execute_ddl(&sql).await.map_err(|e| {
                    anyhow!("Failed to add column {} to \"{}\".\"{}\": {}", col_name, self.schema, table, e)
                })?;

                info!("  [OK] Column {} added to \"{}\".\"{}\"", col_name, self.schema, table);
            } else {
                info!("  [OK] Column {} already exists in \"{}\".\"{}\"", col_name, self.schema, table);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;
    use crate::core::traits::SourceColumn;

    fn test_config() -> OracleSinkConfig {
        OracleSinkConfig {
            host: "localhost".to_string(),
            port: 1521,
            service_name: "ORCLCDB".to_string(),
            user: "cdc_user".to_string(),
            password: "cdc_pass".to_string(),
            schema: "CDC_SCHEMA".to_string(),
            timeout_secs: 30,
            batch_size: 1000,
        }
    }

    fn test_schema() -> SourceTableSchema {
        SourceTableSchema {
            schema: "public".to_string(),
            name: "orders".to_string(),
            columns: vec![
                SourceColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    pg_type_id: None,
                },
                SourceColumn {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    pg_type_id: None,
                },
                SourceColumn {
                    name: "amount".to_string(),
                    data_type: DataType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    nullable: true,
                    pg_type_id: None,
                },
            ],
            primary_keys: vec!["id".to_string()],
        }
    }

    #[test]
    fn test_generate_create_table_sql() {
        let setup = OracleSetup::new(&test_config());
        let schema = test_schema();
        let sql = setup.generate_create_table_sql(&schema);

        assert!(sql.contains("CREATE TABLE \"CDC_SCHEMA\".\"orders\""));
        assert!(sql.contains("\"id\" NUMBER(10) NOT NULL"));
        assert!(sql.contains("\"name\" VARCHAR2(4000)"));
        assert!(sql.contains("\"amount\" NUMBER(10,2)"));
        assert!(sql.contains("CONSTRAINT \"pk_orders\" PRIMARY KEY (\"id\")"));

        // Audit columns
        assert!(sql.contains("\"DBMazz_OP_TYPE\" NUMBER(1) DEFAULT 0"));
        assert!(sql.contains("\"DBMazz_IS_DELETED\" NUMBER(1) DEFAULT 0"));
        assert!(sql.contains("\"DBMazz_SYNCED_AT\" TIMESTAMP(6) DEFAULT SYSTIMESTAMP"));
        assert!(sql.contains("\"DBMazz_CDC_VERSION\" NUMBER(19) DEFAULT 0"));
    }

    #[test]
    fn test_audit_columns_definition() {
        assert_eq!(AUDIT_COLUMNS.len(), 4);
        let col_names: Vec<&str> = AUDIT_COLUMNS.iter().map(|(name, _)| *name).collect();
        assert!(col_names.contains(&"DBMazz_OP_TYPE"));
        assert!(col_names.contains(&"DBMazz_IS_DELETED"));
        assert!(col_names.contains(&"DBMazz_SYNCED_AT"));
        assert!(col_names.contains(&"DBMazz_CDC_VERSION"));
    }
}
