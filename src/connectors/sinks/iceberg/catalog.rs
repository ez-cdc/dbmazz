// Copyright 2025
// Licensed under the Elastic License v2.0

//! Iceberg catalog integration.
//!
//! Supports REST catalog (recommended) and Hadoop catalog fallback
//! for table metadata management.

use anyhow::{Context, Result};
use iceberg::spec::types::Type as IcebergType;
use iceberg::table::Table;
use iceberg::Catalog as IcebergCatalogTrait;

use crate::core::traits::SourceTableSchema;
use crate::core::record::DataType;

use super::config::IcebergSinkConfig;

/// Abstracted Iceberg catalog operations.
pub struct IcebergCatalog {
    catalog: Box<dyn IcebergCatalogTrait>,
    warehouse: String,
}

impl std::fmt::Debug for IcebergCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCatalog")
            .field("warehouse", &self.warehouse)
            .finish()
    }
}

impl IcebergCatalog {
    /// Create a new Iceberg catalog from configuration.
    pub async fn new(config: &IcebergSinkConfig) -> Result<Self> {
        let catalog: Box<dyn IcebergCatalogTrait> = if !config.catalog_uri.is_empty() {
            // REST catalog
            let rest_catalog = iceberg::catalog::rest::RestCatalog::new(
                iceberg::catalog::rest::RestCatalogConfig::builder()
                    .uri(&config.catalog_uri)
                    .warehouse(&config.warehouse)
                    .build(),
            );
            Box::new(rest_catalog)
        } else {
            // Hadoop catalog (store metadata in S3 under warehouse path)
            // For Hadoop catalog we need a file I/O implementation
            // Using S3FileIO from iceberg-rust
            let s3_file_io = iceberg::io::S3FileIO::new(&config.warehouse)?;
            let hadoop_catalog = iceberg::catalog::hadoop::HadoopCatalog::new(
                s3_file_io,
                &config.warehouse,
            );
            Box::new(hadoop_catalog)
        };

        Ok(Self {
            catalog,
            warehouse: config.warehouse.clone(),
        })
    }

    /// Derive a fully-qualified table name from a source table schema.
    pub fn table_name(source: &SourceTableSchema) -> String {
        format!("{}.{}", source.schema, source.name)
    }

    /// Check if a table exists in the catalog.
    pub async fn table_exists(&self, namespace: &str, table: &str) -> Result<bool> {
        Ok(self.catalog.table_exists(namespace, table).await?)
    }

    /// Create a new Iceberg table from source schema.
    pub async fn create_table(&self, source: &SourceTableSchema) -> Result<Table> {
        use iceberg::spec::{NestedField, Schema};
        use iceberg::table::TableCreation;

        let mut fields = Vec::new();
        let mut field_id: i32 = 1;

        for col in &source.columns {
            let ice_type = super::types::cdc_to_iceberg_type(&col.data_type)
                .with_context(|| format!("Failed to map type for column '{}'", col.name))?;
            fields.push(
                NestedField::optional(field_id, &col.name, ice_type)
                    .map_err(|e| anyhow::anyhow!("Invalid field '{}': {}", col.name, e))?,
            );
            field_id += 1;
        }

        let schema = Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build Iceberg schema: {}", e))?;

        // Sort order by field_id 1 (first column) ascending
        let sort_order = iceberg::spec::SortOrder::builder()
            .with_sort_field(
                iceberg::spec::SortField::builder()
                    .source_column_id(1)
                    .direction(iceberg::spec::SortDirection::Ascending)
                    .build(),
            )
            .build();

        let table_creation = TableCreation::builder()
            .name(&source.name)
            .schema(schema)
            .sort_order(sort_order)
            .build();

        let table = self.catalog
            .create_table(&source.schema, table_creation)
            .await
            .with_context(|| format!("Failed to create table '{}.{}'", source.schema, source.name))?;

        Ok(table)
    }

    /// Load an existing table from the catalog.
    pub async fn load_table(&self, namespace: &str, table: &str) -> Result<Table> {
        let tbl = self.catalog
            .load_table(namespace, table)
            .await
            .with_context(|| format!("Failed to load table '{}.{}'", namespace, table))?;
        Ok(tbl)
    }

    /// Get the current schema of a table as a list of SourceColumn.
    pub async fn current_schema(&self, namespace: &str, table: &str) -> Result<Vec<SourceColumn>> {
        let tbl = self.load_table(namespace, table).await?;
        let metadata = tbl.metadata();

        // Get the current schema from table metadata
        let schema = metadata.current_table_schema()
            .ok_or_else(|| anyhow::anyhow!("No current schema found for '{}.{}'", namespace, table))?;

        let mut columns = Vec::new();
        for field in schema.fields() {
            let dt = iceberg_type_to_cdc(&field.field_type);
            columns.push(SourceColumn {
                name: field.name.clone(),
                data_type: dt,
                nullable: field.required,
                pg_type_id: None,
            });
        }

        Ok(columns)
    }

    /// Add a column to an existing Iceberg table.
    pub async fn add_column(
        &self,
        namespace: &str,
        table: &str,
        column_name: &str,
        data_type: &DataType,
    ) -> Result<()> {
        let mut tbl = self.load_table(namespace, table).await?;
        let ice_type = super::types::cdc_to_iceberg_type(data_type)?;

        tbl.alter(
            iceberg::transaction::Transaction::new()
                .add_column(
                    iceberg::spec::table::AlterAddColumn {
                        name: column_name.to_string(),
                        doc: None,
                        field_type: Some(ice_type),
                        required: false,
                        default_value: None,
                        comment: None,
                    },
                )
                .map_err(|e| anyhow::anyhow!("Invalid ADD COLUMN: {}", e))?
        ).await
        .with_context(|| format!("Failed to add column '{}' to '{}.{}'", column_name, namespace, table))?;

        Ok(())
    }
}

/// Convert an Iceberg type back to a CDC DataType (for schema comparison).
fn iceberg_type_to_cdc(t: &IcebergType) -> DataType {
    use iceberg::spec::types::PrimitiveType;
    match t {
        IcebergType::Primitive(p) => match p {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { .. } => DataType::Decimal(38, 0), // approximate
            PrimitiveType::Date => DataType::Date,
            PrimitiveType::Time => DataType::Time,
            PrimitiveType::TimestampTz | PrimitiveType::Timestamp => DataType::Timestamp,
            PrimitiveType::String => DataType::String,
            PrimitiveType::Uuid => DataType::Uuid,
            PrimitiveType::Binary => DataType::Bytes,
            _ => DataType::String,
        },
        _ => DataType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name() {
        let source = SourceTableSchema {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![],
            primary_keys: vec![],
        };
        assert_eq!(IcebergCatalog::table_name(&source), "public.users");
    }

    #[test]
    fn test_iceberg_type_to_cdc_roundtrip() {
        use iceberg::spec::types::{PrimitiveType, Type as IcebergType};

        assert_eq!(
            iceberg_type_to_cdc(&IcebergType::Primitive(PrimitiveType::Boolean)),
            DataType::Boolean
        );
        assert_eq!(
            iceberg_type_to_cdc(&IcebergType::Primitive(PrimitiveType::Int)),
            DataType::Int32
        );
        assert_eq!(
            iceberg_type_to_cdc(&IcebergType::Primitive(PrimitiveType::String)),
            DataType::String
        );
    }
}
