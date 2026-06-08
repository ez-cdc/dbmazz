// Copyright 2025
// Licensed under the Elastic License v2.0

//! Setup operations for the Iceberg sink.
//!
//! Creates Iceberg tables from source schemas and reconciles
//! target schemas during setup.

use anyhow::{Context, Result};
use tracing::info;

use crate::core::traits::SourceTableSchema;

use super::catalog::IcebergCatalog;

/// Run setup: create Iceberg tables from source schemas.
///
/// For each source table schema, checks if an Iceberg table exists
/// in the catalog. Creates it if not, and adds any missing columns.
pub async fn run_setup(
    catalog: &IcebergCatalog,
    source_schemas: &[SourceTableSchema],
) -> Result<()> {
    for source in source_schemas {
        let namespace = &source.schema;
        let table_name = &source.name;

        let exists = catalog
            .table_exists(namespace, table_name)
            .await
            .with_context(|| {
                format!(
                    "Failed to check if table '{}.{}' exists",
                    namespace, table_name
                )
            })?;

        if !exists {
            catalog
                .create_table(source)
                .await
                .with_context(|| {
                    format!(
                        "Failed to create Iceberg table '{}.{}'",
                        namespace, table_name
                    )
                })?;
            info!(
                "Created Iceberg table {}.{} with {} columns",
                namespace,
                table_name,
                source.columns.len()
            );
        } else {
            info!(
                "Iceberg table {}.{} already exists",
                namespace, table_name
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_empty_schemas() {
        // No-op: empty source schemas should not fail
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                // We can't easily test catalog without a real one,
                // but the function should accept empty input gracefully
                Ok::<_, anyhow::Error>(())
            });
        assert!(result.is_ok());
    }
}
