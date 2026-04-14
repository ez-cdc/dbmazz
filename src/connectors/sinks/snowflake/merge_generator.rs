// Copyright 2025
// Licensed under the Elastic License v2.0

//! Dynamic MERGE SQL generator for Snowflake.
//!
//! Generates a MERGE statement that applies raw table records to the target table:
//! - Deduplicates by PK using ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _TIMESTAMP DESC)
//! - Handles INSERT (NOT MATCHED), DELETE/soft-delete (MATCHED), UPDATE (MATCHED)
//! - Generates one WHEN MATCHED clause per unique TOAST column combination
//!
//! Key differences from PostgresSink MERGE:
//! - Column extraction: `_DATA:"col"::TYPE` (VARIANT path) instead of `(_data->>'col')::type`
//! - Identifiers: UPPERCASE + double-quoted
//! - Transaction: implicit (MERGE is atomic in Snowflake)
//! - Delete: configurable soft/hard

use super::types::TypeMapper;
use crate::core::traits::SourceTableSchema;

/// Generate a complete MERGE SQL statement for a single batch.
///
/// # Arguments
/// * `database` — Snowflake database
/// * `raw_table` — full name of the raw table (e.g., `_DBMAZZ._RAW_JOB`)
/// * `target_schema` — target table schema (e.g., `PUBLIC`)
/// * `source_schema` — source table schema with column definitions and PKs
/// * `toast_combinations` — unique TOAST column combinations found in this batch
/// * `batch_id` — the specific batch to process
/// * `soft_delete` — if true, DELETE becomes UPDATE SET _DBMAZZ_IS_DELETED = TRUE
pub fn generate_merge(
    database: &str,
    raw_table: &str,
    target_schema: &str,
    source_schema: &SourceTableSchema,
    toast_combinations: &[String],
    batch_id: i64,
    soft_delete: bool,
) -> String {
    let type_mapper = TypeMapper::new();
    let table_upper = source_schema.name.to_uppercase();
    let dst_table = format!("{}.{}.\"{}\"", database, target_schema, table_upper);
    // _DST_TABLE in the raw table is the source-side qualified name (e.g. "public.orders")
    // emitted by TableRef::qualified_name() in parquet_writer. Use the source schema,
    // NOT the Snowflake target schema, because Snowflake VARCHAR comparisons are
    // case-sensitive and source schema is typically lowercase.
    let dst_qualified = format!("{}.{}", source_schema.schema, source_schema.name);

    // Build column extraction expressions from VARIANT: _DATA:"col"::TYPE AS "COL"
    let col_extracts: Vec<String> = source_schema
        .columns
        .iter()
        .map(|col| {
            let expr = type_mapper.variant_extract_expr(&col.name, col.pg_type_id);
            format!("{} AS \"{}\"", expr, col.name.to_uppercase())
        })
        .collect();

    // PK columns for PARTITION BY
    let pk_partition: Vec<String> = source_schema
        .primary_keys
        .iter()
        .map(|pk| {
            let col = source_schema
                .columns
                .iter()
                .find(|c| &c.name == pk)
                .expect("PK column must exist in schema");
            type_mapper.variant_extract_expr(&col.name, col.pg_type_id)
        })
        .collect();

    // PK ON clause
    let pk_on: Vec<String> = source_schema
        .primary_keys
        .iter()
        .map(|pk| {
            let upper = pk.to_uppercase();
            format!("TARGET.\"{}\" = SOURCE.\"{}\"", upper, upper)
        })
        .collect();

    // All column names (UPPERCASE)
    let all_col_names: Vec<String> = source_schema
        .columns
        .iter()
        .map(|c| c.name.to_uppercase())
        .collect();

    // Non-PK column names
    let pk_upper: Vec<String> = source_schema
        .primary_keys
        .iter()
        .map(|k| k.to_uppercase())
        .collect();
    let non_pk_cols: Vec<&str> = all_col_names
        .iter()
        .filter(|c| !pk_upper.contains(c))
        .map(|c| c.as_str())
        .collect();

    // --- Build SQL ---
    let mut sql = String::with_capacity(4096);

    // MERGE INTO ... USING (CTEs)
    sql.push_str(&format!("MERGE INTO {} AS TARGET\n", dst_table));
    sql.push_str("USING (\n");

    // CTE 1: RANKED — dedup by PK, keep most recent
    sql.push_str("    WITH RANKED AS (\n");
    sql.push_str("        SELECT\n");
    sql.push_str("            _DATA, _RECORD_TYPE, _TOAST_COLUMNS, _TIMESTAMP,\n");
    sql.push_str("            ROW_NUMBER() OVER (\n");
    sql.push_str("                PARTITION BY\n");
    sql.push_str("                    ");
    sql.push_str(&pk_partition.join(", "));
    sql.push('\n');
    sql.push_str("                ORDER BY _TIMESTAMP DESC\n");
    sql.push_str("            ) AS _RANK\n");
    sql.push_str(&format!("        FROM {}.{}\n", database, raw_table));
    sql.push_str(&format!("        WHERE _BATCH_ID = {}\n", batch_id));
    sql.push_str(&format!("          AND _DST_TABLE = '{}'\n", dst_qualified));
    sql.push_str("    ),\n");

    // CTE 2: FLATTENED — extract columns with type casting
    sql.push_str("    FLATTENED AS (\n");
    sql.push_str("        SELECT\n");
    sql.push_str("            _RECORD_TYPE,\n");
    sql.push_str("            _TOAST_COLUMNS,\n");
    sql.push_str("            _TIMESTAMP,\n");
    for (i, extract) in col_extracts.iter().enumerate() {
        sql.push_str("            ");
        sql.push_str(extract);
        if i < col_extracts.len() - 1 {
            sql.push(',');
        }
        sql.push('\n');
    }
    sql.push_str("        FROM RANKED\n");
    sql.push_str("        WHERE _RANK = 1\n");
    sql.push_str("    )\n");

    sql.push_str("    SELECT * FROM FLATTENED\n");
    sql.push_str(") AS SOURCE\n");

    // ON clause (PK match)
    sql.push_str("ON ");
    sql.push_str(&pk_on.join(" AND "));
    sql.push('\n');

    // WHEN NOT MATCHED AND not DELETE → INSERT
    sql.push_str("\nWHEN NOT MATCHED AND SOURCE._RECORD_TYPE != 2 THEN\n");
    sql.push_str("    INSERT (");
    let insert_cols: Vec<String> = all_col_names
        .iter()
        .map(|c| format!("\"{}\"", c))
        .chain(vec![
            "\"_DBMAZZ_SYNCED_AT\"".to_string(),
            "\"_DBMAZZ_OP_TYPE\"".to_string(),
        ])
        .chain(if soft_delete {
            vec!["\"_DBMAZZ_IS_DELETED\"".to_string()]
        } else {
            vec![]
        })
        .collect();
    sql.push_str(&insert_cols.join(", "));
    sql.push_str(")\n");
    sql.push_str("    VALUES (");
    let insert_vals: Vec<String> = all_col_names
        .iter()
        .map(|c| format!("SOURCE.\"{}\"", c))
        .chain(vec![
            "CURRENT_TIMESTAMP()".to_string(),
            "SOURCE._RECORD_TYPE".to_string(),
        ])
        .chain(if soft_delete {
            vec!["FALSE".to_string()]
        } else {
            vec![]
        })
        .collect();
    sql.push_str(&insert_vals.join(", "));
    sql.push_str(")\n");

    // DELETE handling
    if soft_delete {
        // Soft delete: UPDATE SET _DBMAZZ_IS_DELETED = TRUE
        sql.push_str("\nWHEN MATCHED AND SOURCE._RECORD_TYPE = 2 THEN\n");
        sql.push_str("    UPDATE SET\n");
        sql.push_str("        \"_DBMAZZ_IS_DELETED\" = TRUE,\n");
        sql.push_str("        \"_DBMAZZ_SYNCED_AT\" = CURRENT_TIMESTAMP(),\n");
        sql.push_str("        \"_DBMAZZ_OP_TYPE\" = 2\n");

        // Soft delete: INSERT + DELETE in same batch edge case
        sql.push_str("\nWHEN NOT MATCHED AND SOURCE._RECORD_TYPE = 2 THEN\n");
        sql.push_str("    INSERT (");
        sql.push_str(&insert_cols.join(", "));
        sql.push_str(")\n");
        sql.push_str("    VALUES (");
        let delete_insert_vals: Vec<String> = all_col_names
            .iter()
            .map(|c| format!("SOURCE.\"{}\"", c))
            .chain(vec![
                "CURRENT_TIMESTAMP()".to_string(),
                "2".to_string(),
                "TRUE".to_string(),
            ])
            .collect();
        sql.push_str(&delete_insert_vals.join(", "));
        sql.push_str(")\n");
    } else {
        // Hard delete
        sql.push_str("\nWHEN MATCHED AND SOURCE._RECORD_TYPE = 2 THEN\n");
        sql.push_str("    DELETE\n");
    }

    // WHEN MATCHED AND UPDATE — one clause per TOAST combination
    for toast in toast_combinations {
        let update_cols: Vec<&str> = if toast.is_empty() {
            non_pk_cols.clone()
        } else {
            let unchanged: Vec<String> =
                toast.split(',').map(|s| s.trim().to_uppercase()).collect();
            non_pk_cols
                .iter()
                .filter(|c| !unchanged.contains(&c.to_string()))
                .copied()
                .collect()
        };

        sql.push_str("\nWHEN MATCHED AND SOURCE._RECORD_TYPE != 2");
        if toast.is_empty() {
            sql.push_str(" AND SOURCE._TOAST_COLUMNS = ''");
        } else {
            sql.push_str(&format!(" AND SOURCE._TOAST_COLUMNS = '{}'", toast));
        }
        sql.push_str(" THEN\n");
        sql.push_str("    UPDATE SET\n");

        let mut set_parts: Vec<String> = update_cols
            .iter()
            .map(|c| format!("        \"{}\" = SOURCE.\"{}\"", c, c))
            .collect();
        set_parts.push("        \"_DBMAZZ_SYNCED_AT\" = CURRENT_TIMESTAMP()".to_string());
        set_parts.push("        \"_DBMAZZ_OP_TYPE\" = SOURCE._RECORD_TYPE".to_string());
        if soft_delete {
            set_parts.push("        \"_DBMAZZ_IS_DELETED\" = FALSE".to_string());
        }
        sql.push_str(&set_parts.join(",\n"));
        sql.push('\n');
    }

    sql.push_str(";\n");
    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::{SourceColumn, SourceTableSchema};
    use crate::core::DataType;

    fn test_schema() -> SourceTableSchema {
        SourceTableSchema {
            schema: "public".to_string(),
            name: "orders".to_string(),
            columns: vec![
                SourceColumn {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    pg_type_id: 23,
                },
                SourceColumn {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    pg_type_id: 25,
                },
                SourceColumn {
                    name: "amount".to_string(),
                    data_type: DataType::Decimal {
                        precision: 38,
                        scale: 9,
                    },
                    nullable: true,
                    pg_type_id: 1700,
                },
            ],
            primary_keys: vec!["id".to_string()],
        }
    }

    #[test]
    fn test_generate_merge_basic_hard_delete() {
        let schema = test_schema();
        let sql = generate_merge(
            "MY_DB",
            "_DBMAZZ._RAW_JOB",
            "PUBLIC",
            &schema,
            &["".to_string()],
            1,
            false,
        );

        assert!(sql.contains("MERGE INTO MY_DB.PUBLIC.\"ORDERS\" AS TARGET"));
        assert!(sql.contains("ROW_NUMBER() OVER"));
        assert!(sql.contains("_BATCH_ID = 1"));
        // _DST_TABLE must match what parquet_writer stores (source schema, not target):
        // TableRef::qualified_name() emits "public.orders" — lowercase, PG schema.
        assert!(sql.contains("_DST_TABLE = 'public.orders'"));
        assert!(sql.contains("TARGET.\"ID\" = SOURCE.\"ID\""));
        assert!(sql.contains("WHEN NOT MATCHED AND SOURCE._RECORD_TYPE != 2 THEN"));
        assert!(sql.contains("WHEN MATCHED AND SOURCE._RECORD_TYPE = 2 THEN"));
        assert!(sql.contains("DELETE"));
        assert!(sql.contains("\"NAME\" = SOURCE.\"NAME\""));
        assert!(sql.contains("\"AMOUNT\" = SOURCE.\"AMOUNT\""));
        assert!(sql.contains("\"_DBMAZZ_SYNCED_AT\" = CURRENT_TIMESTAMP()"));
        assert!(!sql.contains("_DBMAZZ_IS_DELETED"));
    }

    #[test]
    fn test_generate_merge_soft_delete() {
        let schema = test_schema();
        let sql = generate_merge(
            "MY_DB",
            "_DBMAZZ._RAW_JOB",
            "PUBLIC",
            &schema,
            &["".to_string()],
            1,
            true,
        );

        // Soft delete: should NOT contain hard DELETE
        assert!(!sql.contains("\n    DELETE\n"));
        // Should contain soft delete UPDATE
        assert!(sql.contains("\"_DBMAZZ_IS_DELETED\" = TRUE"));
        // Should reset is_deleted on update
        assert!(sql.contains("\"_DBMAZZ_IS_DELETED\" = FALSE"));
        // INSERT+DELETE edge case: should insert with _DBMAZZ_IS_DELETED = TRUE
        assert!(sql.contains("WHEN NOT MATCHED AND SOURCE._RECORD_TYPE = 2 THEN"));
    }

    #[test]
    fn test_generate_merge_with_toast() {
        let schema = test_schema();
        let sql = generate_merge(
            "MY_DB",
            "_DBMAZZ._RAW_JOB",
            "PUBLIC",
            &schema,
            &["".to_string(), "amount".to_string()],
            1,
            false,
        );

        // Two UPDATE clauses
        let update_count = sql
            .matches("WHEN MATCHED AND SOURCE._RECORD_TYPE != 2")
            .count();
        assert_eq!(update_count, 2);

        assert!(sql.contains("AND SOURCE._TOAST_COLUMNS = ''"));
        assert!(sql.contains("AND SOURCE._TOAST_COLUMNS = 'amount'"));
    }

    #[test]
    fn test_generate_merge_composite_pk() {
        let schema = SourceTableSchema {
            schema: "public".to_string(),
            name: "order_items".to_string(),
            columns: vec![
                SourceColumn {
                    name: "order_id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    pg_type_id: 23,
                },
                SourceColumn {
                    name: "item_id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    pg_type_id: 23,
                },
                SourceColumn {
                    name: "quantity".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    pg_type_id: 23,
                },
            ],
            primary_keys: vec!["order_id".to_string(), "item_id".to_string()],
        };

        let sql = generate_merge(
            "MY_DB",
            "_DBMAZZ._RAW_JOB",
            "PUBLIC",
            &schema,
            &["".to_string()],
            1,
            false,
        );

        // Composite PK
        assert!(sql.contains(
            "TARGET.\"ORDER_ID\" = SOURCE.\"ORDER_ID\" AND TARGET.\"ITEM_ID\" = SOURCE.\"ITEM_ID\""
        ));
    }

    #[test]
    fn test_variant_extraction_in_merge() {
        let schema = test_schema();
        let sql = generate_merge(
            "MY_DB",
            "_DBMAZZ._RAW_JOB",
            "PUBLIC",
            &schema,
            &["".to_string()],
            1,
            false,
        );

        // INT column: direct cast
        assert!(sql.contains("_DATA:\"id\"::INT AS \"ID\""));
        // TEXT column: VARCHAR cast
        assert!(sql.contains("_DATA:\"name\"::VARCHAR AS \"NAME\""));
        // NUMERIC column: TRY_CAST
        assert!(sql.contains("TRY_CAST((_DATA:\"amount\")::VARCHAR AS NUMBER(38,9)) AS \"AMOUNT\""));
    }
}
