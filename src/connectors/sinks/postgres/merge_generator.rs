// Copyright 2025
// Licensed under the Elastic License v2.0

//! Dynamic MERGE SQL generator for PostgreSQL >= 15.
//!
//! Generates a MERGE statement that applies raw table records to the target table:
//! - Deduplicates by PK using ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _timestamp DESC)
//! - Handles INSERT (NOT MATCHED), DELETE (MATCHED), UPDATE (MATCHED)
//! - Generates one WHEN MATCHED clause per unique TOAST column combination

use crate::core::traits::SourceTableSchema;
use super::types::pg_oid_to_target_type;

/// Generate a complete MERGE SQL statement for a single batch.
///
/// # Arguments
/// * `raw_table` — full name of the raw table (e.g., `_dbmazz._raw_dbmazz_slot`)
/// * `target_schema` — target table schema (e.g., `public`)
/// * `source_schema` — source table schema with column definitions and PKs
/// * `toast_combinations` — unique TOAST column combinations found in this batch
///   (e.g., `["", "big_col", "big_col,other_col"]`)
/// * `batch_id` — the specific batch to process
pub fn generate_merge(
    raw_table: &str,
    target_schema: &str,
    source_schema: &SourceTableSchema,
    toast_combinations: &[String],
    batch_id: i64,
) -> String {
    let dst_table = format!("\"{}\".\"{}\"", target_schema, source_schema.name);
    let dst_qualified = format!("{}.{}", target_schema, source_schema.name);

    // Build column extraction expressions: (_data->>'col')::type AS "col"
    let col_extracts: Vec<String> = source_schema
        .columns
        .iter()
        .map(|col| {
            let pg_type = pg_oid_to_target_type(col.pg_type_id);
            format!("(_data->>'{name}')::{pg_type} AS \"{name}\"", name = col.name)
        })
        .collect();

    // PK columns for PARTITION BY and ON clause
    let pk_partition: Vec<String> = source_schema
        .primary_keys
        .iter()
        .map(|pk| {
            let col = source_schema
                .columns
                .iter()
                .find(|c| &c.name == pk)
                .expect("PK column must exist in schema");
            let pg_type = pg_oid_to_target_type(col.pg_type_id);
            format!("(_data->>'{pk}')::{pg_type}", pk = pk)
        })
        .collect();

    let pk_on: Vec<String> = source_schema
        .primary_keys
        .iter()
        .map(|pk| format!("dst.\"{}\" = src.\"{}\"", pk, pk))
        .collect();

    // All non-PK column names (for UPDATE SET)
    let non_pk_cols: Vec<&str> = source_schema
        .columns
        .iter()
        .filter(|c| !source_schema.primary_keys.contains(&c.name))
        .map(|c| c.name.as_str())
        .collect();

    // All column names (for INSERT)
    let all_col_names: Vec<&str> = source_schema.columns.iter().map(|c| c.name.as_str()).collect();

    // --- Build the SQL ---

    let mut sql = String::with_capacity(2048);

    // CTE: dedup by PK, keep most recent by _timestamp.
    // ROW_NUMBER() guarantees exactly one row per PK (no ties, unlike RANK()).
    sql.push_str("WITH src_rank AS (\n");
    sql.push_str("    SELECT _data, _record_type, _toast_columns,\n");
    sql.push_str("        ROW_NUMBER() OVER (\n");
    sql.push_str("            PARTITION BY ");
    sql.push_str(&pk_partition.join(", "));
    sql.push('\n');
    sql.push_str("            ORDER BY _timestamp DESC\n");
    sql.push_str("        ) AS _rank\n");
    sql.push_str(&format!("    FROM {}\n", raw_table));
    sql.push_str(&format!(
        "    WHERE _batch_id = {}\n",
        batch_id
    ));
    sql.push_str(&format!(
        "      AND _dst_table = '{}'\n",
        dst_qualified
    ));
    sql.push_str(")\n");

    // MERGE INTO
    sql.push_str(&format!("MERGE INTO {} AS dst\n", dst_table));
    sql.push_str("USING (\n");
    sql.push_str("    SELECT\n");
    for extract in &col_extracts {
        sql.push_str("        ");
        sql.push_str(extract);
        sql.push_str(",\n");
    }
    sql.push_str("        _record_type,\n");
    sql.push_str("        _toast_columns\n");
    sql.push_str("    FROM src_rank\n");
    sql.push_str("    WHERE _rank = 1\n");
    sql.push_str(") AS src\n");
    sql.push_str("ON ");
    sql.push_str(&pk_on.join(" AND "));
    sql.push('\n');

    // WHEN NOT MATCHED AND not DELETE → INSERT
    sql.push_str("WHEN NOT MATCHED AND src._record_type != 2 THEN\n");
    sql.push_str("    INSERT (");
    let insert_cols: Vec<String> = all_col_names
        .iter()
        .map(|c| format!("\"{}\"", c))
        .chain(std::iter::once("\"_dbmazz_synced_at\"".to_string()))
        .chain(std::iter::once("\"_dbmazz_op_type\"".to_string()))
        .collect();
    sql.push_str(&insert_cols.join(", "));
    sql.push_str(")\n");
    sql.push_str("    VALUES (");
    let insert_vals: Vec<String> = all_col_names
        .iter()
        .map(|c| format!("src.\"{}\"", c))
        .chain(std::iter::once("now()".to_string()))
        .chain(std::iter::once("src._record_type".to_string()))
        .collect();
    sql.push_str(&insert_vals.join(", "));
    sql.push_str(")\n");

    // WHEN MATCHED AND DELETE → DELETE
    sql.push_str("WHEN MATCHED AND src._record_type = 2 THEN\n");
    sql.push_str("    DELETE\n");

    // WHEN MATCHED AND UPDATE — one clause per TOAST combination
    for toast in toast_combinations {
        let update_cols: Vec<&str> = if toast.is_empty() {
            // No TOAST — update all non-PK columns
            non_pk_cols.clone()
        } else {
            // TOAST — exclude unchanged columns
            let unchanged: Vec<&str> = toast.split(',').map(|s| s.trim()).collect();
            non_pk_cols
                .iter()
                .filter(|c| !unchanged.contains(c))
                .copied()
                .collect()
        };

        sql.push_str("WHEN MATCHED AND src._record_type != 2");
        if toast.is_empty() {
            sql.push_str(" AND src._toast_columns = ''");
        } else {
            sql.push_str(&format!(" AND src._toast_columns = '{}'", toast));
        }
        sql.push_str(" THEN\n");
        sql.push_str("    UPDATE SET\n");

        let mut set_parts: Vec<String> = update_cols
            .iter()
            .map(|c| format!("        \"{}\" = src.\"{}\"", c, c))
            .collect();
        set_parts.push("        \"_dbmazz_synced_at\" = now()".to_string());
        set_parts.push("        \"_dbmazz_op_type\" = src._record_type".to_string());
        sql.push_str(&set_parts.join(",\n"));
        sql.push('\n');
    }

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
                    pg_type_id: 23, // integer
                },
                SourceColumn {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    pg_type_id: 25, // text
                },
                SourceColumn {
                    name: "amount".to_string(),
                    data_type: DataType::Decimal {
                        precision: 38,
                        scale: 9,
                    },
                    nullable: true,
                    pg_type_id: 1700, // numeric
                },
            ],
            primary_keys: vec!["id".to_string()],
        }
    }

    #[test]
    fn test_generate_merge_basic() {
        let schema = test_schema();
        let sql = generate_merge(
            "_dbmazz._raw_job",
            "public",
            &schema,
            &["".to_string()],
            1,
        );

        // Should contain MERGE INTO
        assert!(sql.contains("MERGE INTO \"public\".\"orders\" AS dst"));
        // Should use ROW_NUMBER() (not RANK())
        assert!(sql.contains("ROW_NUMBER() OVER"));
        assert!(!sql.contains("RANK()"));
        // Should contain PK partition
        assert!(sql.contains("PARTITION BY (_data->>'id')::integer"));
        // Should filter by single batch_id
        assert!(sql.contains("_batch_id = 1"));
        // Should contain ON clause
        assert!(sql.contains("ON dst.\"id\" = src.\"id\""));
        // Should contain INSERT
        assert!(sql.contains("WHEN NOT MATCHED AND src._record_type != 2 THEN"));
        // Should contain DELETE
        assert!(sql.contains("WHEN MATCHED AND src._record_type = 2 THEN"));
        assert!(sql.contains("DELETE"));
        // Should contain UPDATE with all non-PK cols
        assert!(sql.contains("\"name\" = src.\"name\""));
        assert!(sql.contains("\"amount\" = src.\"amount\""));
        // Should contain audit columns
        assert!(sql.contains("\"_dbmazz_synced_at\" = now()"));
    }

    #[test]
    fn test_generate_merge_with_toast() {
        let schema = test_schema();
        let sql = generate_merge(
            "_dbmazz._raw_job",
            "public",
            &schema,
            &["".to_string(), "amount".to_string()],
            1,
        );

        // Should have two WHEN MATCHED UPDATE clauses
        let update_count = sql.matches("WHEN MATCHED AND src._record_type != 2").count();
        assert_eq!(update_count, 2, "Should have 2 UPDATE clauses (one per TOAST combination)");

        // First clause: no TOAST, updates all
        assert!(sql.contains("AND src._toast_columns = ''"));
        // Second clause: amount is TOAST, should NOT update amount
        assert!(sql.contains("AND src._toast_columns = 'amount'"));
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
            "_dbmazz._raw_job",
            "public",
            &schema,
            &["".to_string()],
            1,
        );

        // Composite PK in PARTITION BY
        assert!(sql.contains("PARTITION BY (_data->>'order_id')::integer, (_data->>'item_id')::integer"));
        // Composite PK in ON clause
        assert!(sql.contains("dst.\"order_id\" = src.\"order_id\" AND dst.\"item_id\" = src.\"item_id\""));
    }

    #[test]
    fn test_generate_merge_single_batch() {
        let schema = test_schema();
        let sql = generate_merge(
            "_dbmazz._raw_slot",
            "public",
            &schema,
            &["".to_string()],
            42,
        );

        // Should filter by exact batch_id, not a range
        assert!(sql.contains("_batch_id = 42"));
        assert!(!sql.contains("_batch_id >"));
        assert!(!sql.contains("_batch_id <="));
    }
}
