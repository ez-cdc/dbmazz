// Copyright 2025
// Licensed under the Elastic License v2.0

//! Schema evolution for Iceberg tables.
//!
//! Detects new columns and type promotions in incoming CDC records
//! and applies them to the Iceberg table via the catalog.

use std::collections::HashMap;
use anyhow::{Context, Result};
use tracing::info;

use crate::core::record::{CdcRecord, DataType};
use crate::core::traits::SourceColumn;

use super::catalog::IcebergCatalog;

/// Result of schema evolution analysis.
pub struct SchemaEvolutionPlan {
    /// Working schema state (combined existing + new columns).
    pub working: HashMap<String, Vec<SourceColumn>>,
    /// Pending differences to apply.
    pub pending_diffs: Vec<TableDiff>,
}

/// A single table's schema difference.
pub struct TableDiff {
    pub table_name: String,
    pub new_columns: Vec<(String, DataType)>,
}

/// Detect schema differences between current table schemas and incoming CDC records.
pub fn compute_schema_evolution_plan(
    current_schemas: &HashMap<String, Vec<SourceColumn>>,
    records: &[CdcRecord],
) -> SchemaEvolutionPlan {
    let mut working = current_schemas.clone();
    let mut pending_diffs: Vec<TableDiff> = Vec::new();

    for record in records {
        let table_key = match record {
            CdcRecord::Insert { table, .. } => table.clone(),
            CdcRecord::Update { table, .. } => table.clone(),
            CdcRecord::Delete { table, .. } => table.clone(),
            CdcRecord::SchemaChange { table, .. } => table.clone(),
            _ => continue,
        };

        let existing_columns = working.entry(table_key.clone()).or_default();

        let record_columns = match record {
            CdcRecord::Insert { columns, .. } => Some(columns),
            CdcRecord::Update { old_columns: _, new_columns, .. } => Some(new_columns),
            CdcRecord::Delete { columns, .. } => Some(columns),
            CdcRecord::SchemaChange { columns, .. } => Some(columns),
            _ => None,
        };

        if let Some(columns) = record_columns {
            let existing_names: std::collections::HashSet<&str> =
                existing_columns.iter().map(|c| c.name.as_str()).collect();

            let mut new_for_table = Vec::new();
            for col in columns {
                if !existing_names.contains(col.name.as_str()) {
                    new_for_table.push((col.name.clone(), col.data_type.clone()));
                }
            }

            if !new_for_table.is_empty() {
                // Add to existing columns for next iteration
                for (name, dt) in &new_for_table {
                    existing_columns.push(SourceColumn {
                        name: name.clone(),
                        data_type: dt.clone(),
                        nullable: true,
                        pg_type_id: None,
                    });
                }
                pending_diffs.push(TableDiff {
                    table_name: table_key.clone(),
                    new_columns: new_for_table,
                });
            }
        }
    }

    SchemaEvolutionPlan {
        working,
        pending_diffs,
    }
}

/// Apply schema evolution diffs to an Iceberg table.
pub async fn apply_schema_evolution(
    catalog: &IcebergCatalog,
    diffs: &[TableDiff],
) -> Result<()> {
    for diff in diffs {
        // Split into namespace and table name
        let parts: Vec<&str> = diff.table_name.splitn(2, '.').collect();
        let (namespace, table) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("default", parts[0])
        };

        for (col_name, col_type) in &diff.new_columns {
            catalog
                .add_column(namespace, table, col_name, col_type)
                .await
                .with_context(|| {
                    format!(
                        "Failed to add column '{}' to '{}.{}'",
                        col_name, namespace, table
                    )
                })?;
            info!(
                "Schema evolution: added column {}.{}.{} ({:?})",
                namespace, table, col_name, col_type
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::{CdcRecord, Column};

    #[test]
    fn test_compute_schema_evolution_plan_no_changes() {
        let mut current = HashMap::new();
        current.insert("public.users".to_string(), vec![
            SourceColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                pg_type_id: None,
            },
        ]);

        let records = vec![
            CdcRecord::Insert {
                table: "public.users".to_string(),
                columns: vec![Column {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    value: Some(serde_json::Value::Number(1.into())),
                    old_value: None,
                }],
                position: crate::core::position::SourcePosition::default(),
            },
        ];

        let plan = compute_schema_evolution_plan(&current, &records);
        assert!(plan.pending_diffs.is_empty());
    }

    #[test]
    fn test_compute_schema_evolution_plan_new_column() {
        let mut current = HashMap::new();
        current.insert("public.users".to_string(), vec![
            SourceColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                pg_type_id: None,
            },
        ]);

        let records = vec![
            CdcRecord::Insert {
                table: "public.users".to_string(),
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        value: Some(serde_json::Value::Number(1.into())),
                        old_value: None,
                    },
                    Column {
                        name: "email".to_string(),
                        data_type: DataType::String,
                        value: Some(serde_json::Value::String("test@test.com".to_string())),
                        old_value: None,
                    },
                ],
                position: crate::core::position::SourcePosition::default(),
            },
        ];

        let plan = compute_schema_evolution_plan(&current, &records);
        assert_eq!(plan.pending_diffs.len(), 1);
        assert_eq!(plan.pending_diffs[0].new_columns[0].0, "email");
    }
}
