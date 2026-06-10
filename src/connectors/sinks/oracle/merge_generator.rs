// Copyright 2025
// Licensed under the Elastic License v2.0

//! Oracle MERGE statement generator.
//!
//! Generates MERGE INTO statements for upsert (INSERT/UPDATE) and
//! DELETE statements for CDC deletes on Oracle target tables.
//!
//! All identifiers are safely double-quoted via `quote_ident()` to
//! prevent SQL injection — Oracle identifiers are case-insensitive
//! unless quoted, and quoting eliminates any risk of keyword injection.

use crate::core::traits::SourceTableSchema;

/// Double-quote an Oracle identifier, doubling any embedded `"`.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Fully-qualified table reference: `"schema"."table"`.
fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

/// Generate a MERGE INTO statement for Oracle upsert.
///
/// Oracle MERGE syntax:
/// ```ignore
/// MERGE INTO "schema"."table" dst
/// USING (SELECT col1, col2 FROM DUAL WHERE ...) src
/// ON (dst.pk = src.pk)
/// WHEN MATCHED THEN UPDATE SET col1 = src.col1, col2 = src.col2
/// WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (src.col1, src.col2)
/// ```
pub fn generate_merge(
    schema: &str,
    source_schema: &SourceTableSchema,
    unchanged_cols: &[String],
) -> String {
    let dst_table = qualified(schema, &source_schema.name.to_uppercase());

    let col_names: Vec<String> = source_schema.columns.iter()
        .map(|c| c.name.to_uppercase())
        .collect();

    let pk_condition: Vec<String> = source_schema.primary_keys.iter()
        .map(|pk| {
            let q = quote_ident(&pk.to_uppercase());
            format!("dst.{q} = src.{q}")
        })
        .collect();

    // Exclude both PKs and unchanged columns from the UPDATE SET clause.
    // Unchanged columns (Value::Unchanged from PG TOAST) must NOT be written
    // back, otherwise they'd overwrite the existing value with NULL.
    let update_set: Vec<String> = col_names.iter()
        .filter(|c| {
            !source_schema.primary_keys.iter().any(|pk| pk.to_uppercase() == **c)
                && !unchanged_cols.iter().any(|uc| uc.to_uppercase() == **c)
        })
        .map(|c| {
            let q = quote_ident(c);
            format!("{q} = src.{q}")
        })
        .collect();

    let insert_cols: Vec<String> = col_names.iter()
        .map(|c| quote_ident(c))
        .collect();

    let insert_vals: Vec<String> = col_names.iter()
        .map(|c| format!("src.{}", quote_ident(c)))
        .collect();

    // Build the DUAL-based MERGE with a placeholder for inline values
    let mut stmt = String::with_capacity(1024);
    stmt.push_str("MERGE INTO ");
    stmt.push_str(&dst_table);
    stmt.push_str(" dst\n");
    stmt.push_str("USING (\n");
    stmt.push_str("    __USING_SELECT__\n");
    stmt.push_str(") src\n");
    stmt.push_str("ON (");
    stmt.push_str(&pk_condition.join(" AND "));
    stmt.push_str(")\n");

    if !update_set.is_empty() {
        stmt.push_str("WHEN MATCHED THEN UPDATE SET\n    ");
        stmt.push_str(&update_set.join(",\n    "));
        stmt.push_str(",\n    \"DBMAZZ_SYNCED_AT\" = SYSTIMESTAMP,\n");
        stmt.push_str("    \"DBMAZZ_OP_TYPE\" = 0\n");
    }

    stmt.push_str("WHEN NOT MATCHED THEN INSERT (\n    ");
    stmt.push_str(&insert_cols.join(",\n    "));
    stmt.push_str(",\n    \"DBMAZZ_SYNCED_AT\",\n    \"DBMAZZ_OP_TYPE\"\n) VALUES (\n    ");
    stmt.push_str(&insert_vals.join(",\n    "));
    stmt.push_str(",\n    SYSTIMESTAMP,\n    0\n)\n");

    stmt
}

/// Generate a DELETE statement.
pub fn generate_delete(
    schema: &str,
    table_name: &str,
) -> String {
    let mut stmt = String::from("DELETE FROM ");
    stmt.push_str(&qualified(schema, &table_name.to_uppercase()));
    stmt.push_str(" WHERE ");
    stmt
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
                    data_type: DataType::Text,
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
        let unchanged: Vec<String> = vec![];
        let sql = generate_merge("PUBLIC", &schema, &unchanged);
        assert!(sql.contains(r#""PUBLIC"."USERS""#));
        assert!(sql.contains(r#""ID""#));
        assert!(sql.contains(r#""NAME""#));
        assert!(sql.contains("__USING_SELECT__"));
        // Without unchanged cols, NAME should be in UPDATE SET
        assert!(sql.contains(r#""NAME" = src."NAME""#));
    }

    #[test]
    fn test_generate_merge_with_unchanged() {
        let schema = test_schema();
        let unchanged: Vec<String> = vec!["name".to_string()];
        let sql = generate_merge("PUBLIC", &schema, &unchanged);
        assert!(sql.contains(r#""PUBLIC"."USERS""#));
        // NAME excluded from UPDATE SET because it's unchanged
        assert!(!sql.contains(r#""NAME" = src."NAME""#));
        // When ALL non-PK columns are unchanged, the UPDATE SET is omitted
        // (the MERGE still INSERTs on NOT MATCHED; the update is a no-op)
        assert!(!sql.contains("WHEN MATCHED THEN UPDATE"));
        // INSERT branch is still present
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"));
    }

    #[test]
    fn test_generate_delete() {
        let sql = generate_delete("PUBLIC", "users");
        assert!(sql.starts_with(r#"DELETE FROM "PUBLIC"."USERS" WHERE "#));
    }
}
