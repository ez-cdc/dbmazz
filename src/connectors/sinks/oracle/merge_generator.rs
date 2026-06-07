// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle MERGE statement generator.
//!
//! Generates MERGE INTO statements for upsert (INSERT/UPDATE) and
//! DELETE statements for CDC deletes on Oracle target tables.

use crate::core::traits::SourceTableSchema;

/// Generate a MERGE INTO statement for Oracle upsert.
///
/// Oracle MERGE syntax:
/// ```
/// MERGE INTO "schema"."table" dst
/// USING (SELECT col1, col2 FROM DUAL WHERE ...) src
/// ON (dst.pk = src.pk)
/// WHEN MATCHED THEN UPDATE SET col1 = src.col1, col2 = src.col2
/// WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (src.col1, src.col2)
/// ```
pub fn generate_merge(
    schema: &str,
    source_schema: &SourceTableSchema,
) -> String {
    let dst_table = format!("\"{}\".\"{}\"", schema, source_schema.name);
    
    let col_names: Vec<&str> = source_schema.columns.iter()
        .map(|c| c.name.as_str())
        .collect();
    
    let pk_condition: Vec<String> = source_schema.primary_keys.iter()
        .map(|pk| format!("dst.\"{}\" = src.\"{}\"", pk, pk))
        .collect();

    let update_set: Vec<String> = col_names.iter()
        .filter(|c| !source_schema.primary_keys.contains(&c.to_string()))
        .map(|c| format!("\"{}\" = src.\"{}\"", c, c))
        .collect();

    let insert_cols: Vec<String> = col_names.iter()
        .map(|c| format!("\"{}\"", c))
        .collect();
    
    let insert_vals: Vec<String> = col_names.iter()
        .map(|c| format!("src.\"{}\"", c))
        .collect();

    // Build the DUAL-based MERGE
    let mut sql = String::with_capacity(1024);
    sql.push_str(&format!("MERGE INTO {} dst\n", dst_table));
    sql.push_str("USING (\n");
    sql.push_str("    SELECT ");
    sql.push_str(&col_names.iter()
        .map(|c| format!(":{} AS \"{}\"", c, c))
        .collect::<Vec<_>>()
        .join(", "));
    sql.push_str(" FROM DUAL\n");
    sql.push_str(") src\n");
    sql.push_str("ON (");
    sql.push_str(&pk_condition.join(" AND "));
    sql.push_str(")\n");

    if !update_set.is_empty() {
        sql.push_str("WHEN MATCHED THEN UPDATE SET\n    ");
        sql.push_str(&update_set.join(",\n    "));
        sql.push_str(",\n    \"_dbmazz_synced_at\" = SYSTIMESTAMP,\n");
        sql.push_str("    \"_dbmazz_op_type\" = 0\n");
    }
    
    sql.push_str("WHEN NOT MATCHED THEN INSERT (\n    ");
    sql.push_str(&insert_cols.join(",\n    "));
    sql.push_str(",\n    \"_dbmazz_synced_at\",\n    \"_dbmazz_op_type\"\n) VALUES (\n    ");
    sql.push_str(&insert_vals.join(",\n    "));
    sql.push_str(",\n    SYSTIMESTAMP,\n    0\n)\n");

    sql
}

/// Generate a DELETE statement.
pub fn generate_delete(
    schema: &str,
    table_name: &str,
) -> String {
    format!("DELETE FROM \"{}\".\"{}\" WHERE ", schema, table_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;
    use crate::core::traits::{SourceColumn, SourceTableSchema};

    fn test_schema() -> SourceTableSchema {
        SourceTableSchema {
            schema: "PUBLIC".to_string(),
            name: "users".to_string(),
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
            ],
            primary_keys: vec!["id".to_string()],
        }
    }

    #[test]
    fn test_generate_merge() {
        let schema = test_schema();
        let sql = generate_merge("PUBLIC", &schema);
        assert!(sql.contains("MERGE INTO \"PUBLIC\".\"users\" dst"));
        assert!(sql.contains(":id AS \"id\""));
        assert!(sql.contains(":name AS \"name\""));
    }
}
