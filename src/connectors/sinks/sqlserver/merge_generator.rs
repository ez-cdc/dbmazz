// Copyright 2025
// Licensed under the Elastic License v2.0

//! Dynamic MERGE SQL generator for SQL Server.
//!
//! Generates parameterized T-SQL MERGE statements with @P1, @P2, ...
//! placeholders. Unlike the PostgreSQL sink (which uses a raw table + CTE
//! pattern), SQL Server uses a direct MERGE with parameterized queries.
//!
//! # Key behaviors
//!
//! - **generate_merge** — builds a full T-SQL MERGE with UPDATE/INSERT branches
//! - **generate_delete** — builds a simple `DELETE ... WHERE pk = @P1` statement
//! - `Value::Unchanged` columns (PG TOAST) are excluded from the UPDATE SET
//!   clause but included in the INSERT branch
//! - All identifiers use SQL Server bracket quoting: `[column_name]`

use crate::core::record::DataType;

/// Column metadata for MERGE generation.
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
}

/// Wrap a SQL Server identifier in brackets.
///
/// SQL Server uses `[identifier]` quoting instead of PostgreSQL's
/// `"identifier"` or MySQL's backticks.
fn quote_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('[');
    out.push_str(name);
    out.push(']');
    out
}

/// Generate a T-SQL MERGE statement with parameterized queries.
///
/// SQL Server uses `@P1`, `@P2`, etc. as sequential parameter placeholders.
/// Columns listed in `unchanged_cols` are excluded from the `UPDATE SET`
/// clause to avoid overwriting TOAST columns with NULL.
///
/// # Arguments
///
/// * `table` — target table name (already quoted, e.g. `[orders]`)
/// * `schema` — target schema (already quoted, e.g. `[dbo]`)
/// * `columns` — column definitions (name + data_type metadata)
/// * `pk_cols` — primary key column names (unquoted, matched against `columns`)
/// * `unchanged_cols` — columns with `Value::Unchanged` to exclude from UPDATE SET
///
/// # Returns
///
/// A tuple `(sql_string, param_count)` where `param_count` is the number of
/// `@P` parameters used in the statement.
pub fn generate_merge(
    table: &str,
    schema: &str,
    columns: &[ColumnInfo],
    pk_cols: &[String],
    unchanged_cols: &[String],
) -> (String, usize) {
    let mut sql = String::with_capacity(1024);
    let mut param_idx: usize = 1;

    // ── MERGE INTO [schema].[table] AS target ──
    sql.push_str("MERGE INTO ");
    sql.push_str(schema);
    sql.push('.');
    sql.push_str(table);
    sql.push_str(" AS target\n");

    // ── USING (VALUES (@P1, @P2, ...)) AS source (col1, col2, ...) ──
    sql.push_str("USING (VALUES (");
    let mut param_placeholders = Vec::with_capacity(columns.len());
    let mut quoted_cols = Vec::with_capacity(columns.len());

    for col in columns {
        // Build @P{N} parameter placeholder
        let mut p = String::from("@P");
        p.push_str(&param_idx.to_string());
        param_placeholders.push(p);
        param_idx += 1;

        quoted_cols.push(quote_identifier(&col.name));
    }

    sql.push_str(&param_placeholders.join(", "));
    sql.push_str(")) AS source (");
    sql.push_str(&quoted_cols.join(", "));
    sql.push_str(")\n");

    // ── ON target.[pk1] = source.[pk1] AND ... ──
    sql.push_str("ON ");
    let mut on_clauses = Vec::with_capacity(pk_cols.len());
    for pk in pk_cols {
        let pk_q = quote_identifier(pk);
        let mut clause = String::new();
        clause.push_str("target.");
        clause.push_str(&pk_q);
        clause.push_str(" = source.");
        clause.push_str(&pk_q);
        on_clauses.push(clause);
    }
    sql.push_str(&on_clauses.join(" AND "));
    sql.push('\n');

    // ── WHEN MATCHED THEN UPDATE SET ──
    // Exclude PK columns (they are in the ON clause) and unchanged columns
    // (Value::Unchanged from PG TOAST — must not overwrite with NULL).
    sql.push_str("WHEN MATCHED THEN\n");
    sql.push_str("    UPDATE SET\n");

    let mut set_parts = Vec::new();
    for col in columns {
        if pk_cols.contains(&col.name) {
            continue;
        }
        if unchanged_cols.contains(&col.name) {
            continue;
        }
        let col_q = quote_identifier(&col.name);
        let mut set = String::new();
        set.push_str("        ");
        set.push_str(&col_q);
        set.push_str(" = source.");
        set.push_str(&col_q);
        set_parts.push(set);
    }
    sql.push_str(&set_parts.join(",\n"));
    sql.push('\n');

    // ── WHEN NOT MATCHED THEN INSERT (...) VALUES (source.col, ...) ──
    // All columns (including PKs and unchanged) go into the INSERT.
    sql.push_str("WHEN NOT MATCHED THEN\n");
    sql.push_str("    INSERT (");
    sql.push_str(&quoted_cols.join(", "));
    sql.push_str(")\n");
    sql.push_str("    VALUES (");
    let source_vals: Vec<String> = quoted_cols
        .iter()
        .map(|c| {
            let mut sv = String::from("source.");
            sv.push_str(c);
            sv
        })
        .collect();
    sql.push_str(&source_vals.join(", "));
    sql.push_str(");\n");

    let param_count = param_idx - 1;
    (sql, param_count)
}

/// Generate a T-SQL DELETE statement with parameterized PK WHERE clause.
///
/// Example output:
/// ```sql
/// DELETE FROM [dbo].[orders] WHERE [id] = @P1;
/// ```
///
/// # Arguments
///
/// * `table` — target table name (already quoted, e.g. `[orders]`)
/// * `schema` — target schema (already quoted, e.g. `[dbo]`)
/// * `pk_cols` — primary key column names (unquoted)
pub fn generate_delete(table: &str, schema: &str, pk_cols: &[String]) -> String {
    let mut sql = String::with_capacity(256);

    sql.push_str("DELETE FROM ");
    sql.push_str(schema);
    sql.push('.');
    sql.push_str(table);
    sql.push_str(" WHERE ");

    let mut conditions = Vec::with_capacity(pk_cols.len());
    for (param_idx, pk) in (1..).zip(pk_cols.iter()) {
        let pk_q = quote_identifier(pk);
        let mut cond = String::new();
        cond.push_str(&pk_q);
        cond.push_str(" = @P");
        cond.push_str(&param_idx.to_string());
        conditions.push(cond);
    }
    sql.push_str(&conditions.join(" AND "));
    sql.push(';');

    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DataType;

    fn test_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::String,
            },
            ColumnInfo {
                name: "amount".to_string(),
                data_type: DataType::Decimal {
                    precision: 38,
                    scale: 9,
                },
            },
        ]
    }

    fn test_pk_cols() -> Vec<String> {
        vec!["id".to_string()]
    }

    #[test]
    fn test_generate_merge_basic() {
        let (sql, param_count) =
            generate_merge("[orders]", "[dbo]", &test_columns(), &test_pk_cols(), &[]);

        assert_eq!(param_count, 3, "Expected 3 params, got {}", param_count);
        assert!(sql.contains("MERGE INTO [dbo].[orders] AS target"));
        assert!(sql.contains("USING (VALUES (@P1, @P2, @P3)) AS source ([id], [name], [amount])"));
        assert!(sql.contains("ON target.[id] = source.[id]"));
        assert!(sql.contains("WHEN MATCHED THEN"));
        assert!(sql.contains("UPDATE SET"));
        assert!(sql.contains("[name] = source.[name]"));
        assert!(sql.contains("[amount] = source.[amount]"));
        assert!(sql.contains("WHEN NOT MATCHED THEN"));
        assert!(sql.contains("INSERT ([id], [name], [amount])"));
        assert!(sql.contains("VALUES (source.[id], source.[name], source.[amount])"));
        // PK column must NOT appear in SET (only ON clause has it)
        assert!(
            !sql.contains("        [id] = source.[id]"),
            "PK column should not be in UPDATE SET"
        );
    }

    #[test]
    fn test_generate_merge_with_unchanged() {
        let (sql, _) = generate_merge(
            "[orders]",
            "[dbo]",
            &test_columns(),
            &test_pk_cols(),
            &["amount".to_string()],
        );

        // name is still updated
        assert!(sql.contains("[name] = source.[name]"));
        // amount is unchanged → excluded from SET
        assert!(
            !sql.contains("[amount] = source.[amount]"),
            "Unchanged column should be excluded from UPDATE SET"
        );
        // INSERT still includes all columns (PK + unchanged + normal)
        assert!(sql.contains("INSERT ([id], [name], [amount])"));
    }

    #[test]
    fn test_generate_merge_all_unchanged() {
        let cols = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
            },
            ColumnInfo {
                name: "payload".to_string(),
                data_type: DataType::Text,
            },
        ];
        let (sql, _) = generate_merge(
            "[blobs]",
            "[dbo]",
            &cols,
            &test_pk_cols(),
            &["payload".to_string()],
        );

        // Only PK exists in non-unchanged non-PK set — the SET clause should
        // still be valid (even if empty, the UPDATE SET header is present).
        assert!(sql.contains("UPDATE SET"));
        assert!(!sql.contains("[payload] = source.[payload]"));
    }

    #[test]
    fn test_generate_merge_composite_pk() {
        let cols = vec![
            ColumnInfo {
                name: "order_id".to_string(),
                data_type: DataType::Int32,
            },
            ColumnInfo {
                name: "item_id".to_string(),
                data_type: DataType::Int32,
            },
            ColumnInfo {
                name: "quantity".to_string(),
                data_type: DataType::Int32,
            },
        ];
        let pks = vec!["order_id".to_string(), "item_id".to_string()];

        let (sql, param_count) = generate_merge("[order_items]", "[dbo]", &cols, &pks, &[]);

        assert_eq!(param_count, 3);
        assert!(sql.contains(
            "ON target.[order_id] = source.[order_id] AND target.[item_id] = source.[item_id]"
        ));
        // PKs excluded from SET (ON clause has regular indentation, SET has 8 spaces)
        assert!(
            !sql.contains("        [order_id] = source.[order_id]"),
            "PK order_id should not be in SET"
        );
        assert!(
            !sql.contains("        [item_id] = source.[item_id]"),
            "PK item_id should not be in SET"
        );
        assert!(sql.contains("[quantity] = source.[quantity]"));
    }

    #[test]
    fn test_generate_delete_single_pk() {
        let sql = generate_delete("[orders]", "[dbo]", &test_pk_cols());

        assert!(sql.starts_with("DELETE FROM"));
        assert!(sql.contains("[dbo].[orders]"));
        assert!(sql.contains("WHERE [id] = @P1"));
        assert!(sql.ends_with(';'));
    }

    #[test]
    fn test_generate_delete_composite_pk() {
        let pks = vec!["order_id".to_string(), "item_id".to_string()];
        let sql = generate_delete("[order_items]", "[dbo]", &pks);

        assert!(sql.contains("WHERE [order_id] = @P1 AND [item_id] = @P2"));
    }

    #[test]
    fn test_quote_identifier_brackets() {
        assert_eq!(quote_identifier("hello"), "[hello]");
        assert_eq!(quote_identifier("column_name"), "[column_name]");
        assert_eq!(quote_identifier("OrderId"), "[OrderId]");
    }
}
