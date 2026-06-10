// Copyright 2026
// Licensed under the Elastic License v2.0

//! Source schema → Iceberg schema mapping.
//!
//! Builds Iceberg table schemas (with stable field-ids) from
//! `SourceTableSchema`, including the three CDC metadata columns the
//! changelog model adds to every table.

use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::TableIdent;

use crate::core::record::DataType;
use crate::core::traits::SourceTableSchema;

/// CDC operation marker column: `I` / `U` / `D`.
pub const CDC_OP_COL: &str = "_cdc_op";
/// Source position column (PG: LSN; MySQL: binlog position — see README).
pub const CDC_POSITION_COL: &str = "_cdc_position";
/// Sink-side write timestamp column (timestamptz, UTC).
pub const CDC_TS_COL: &str = "_cdc_ts";

/// Iceberg table name for a source table: `<schema>__<table>`.
///
/// Flat naming inside the configured namespace avoids multi-level
/// namespace support differences between catalog servers.
pub fn iceberg_table_name(source_schema: &str, table: &str) -> String {
    if source_schema.is_empty() {
        table.to_string()
    } else {
        format!("{}__{}", source_schema, table)
    }
}

/// `TableIdent` for a `TableRef::qualified_name()` (`schema.table` or `table`).
pub fn table_ident(namespace: &str, qualified_name: &str) -> Result<TableIdent> {
    let (schema, table) = match qualified_name.split_once('.') {
        Some((s, t)) => (s, t),
        None => ("", qualified_name),
    };
    let ns = iceberg::NamespaceIdent::from_vec(vec![namespace.to_string()])
        .context("Failed to build namespace ident")?;
    Ok(TableIdent::new(ns, iceberg_table_name(schema, table)))
}

/// Map a CDC `DataType` to an Iceberg type. Exhaustive — a new `DataType`
/// variant is a compile error here, never a silent fallback.
pub fn cdc_type_to_iceberg(dt: &DataType) -> Result<Type> {
    let t = match dt {
        DataType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        DataType::Int16 | DataType::Int32 => Type::Primitive(PrimitiveType::Int),
        DataType::Int64 => Type::Primitive(PrimitiveType::Long),
        // u64 does not fit in Iceberg `long`; decimal(20,0) holds the full
        // range exactly (consistent with NUMERIC(20,0) in the PG/Snowflake sinks).
        DataType::UInt64 => Type::Primitive(PrimitiveType::Decimal {
            precision: 20,
            scale: 0,
        }),
        DataType::Float32 => Type::Primitive(PrimitiveType::Float),
        DataType::Float64 => Type::Primitive(PrimitiveType::Double),
        DataType::Decimal { precision, scale } => {
            // Iceberg requires 1 <= precision <= 38.
            let p = (*precision).clamp(1, 38);
            let s = (*scale).min(p);
            Type::Primitive(PrimitiveType::Decimal {
                precision: p as u32,
                scale: s as u32,
            })
        }
        DataType::String | DataType::Text | DataType::Json | DataType::Jsonb => {
            Type::Primitive(PrimitiveType::String)
        }
        DataType::Uuid => Type::Primitive(PrimitiveType::Uuid),
        DataType::Date => Type::Primitive(PrimitiveType::Date),
        DataType::Time => Type::Primitive(PrimitiveType::Time),
        DataType::Timestamp => Type::Primitive(PrimitiveType::Timestamp),
        DataType::TimestampTz => Type::Primitive(PrimitiveType::Timestamptz),
        DataType::Bytes => Type::Primitive(PrimitiveType::Binary),
    };
    Ok(t)
}

/// Build the Iceberg schema for a source table: source columns first
/// (field-ids 1..N), then the three CDC metadata columns.
///
/// All source columns are optional regardless of source nullability: a
/// changelog row may legitimately miss columns (e.g. DELETE images under
/// partial replica identity), and the changelog model does not enforce
/// source constraints. Metadata columns are required — the sink always
/// populates them.
pub fn build_iceberg_schema(source: &SourceTableSchema) -> Result<Schema> {
    let mut fields: Vec<Arc<NestedField>> = Vec::with_capacity(source.columns.len() + 3);

    for (i, col) in source.columns.iter().enumerate() {
        let iceberg_type = cdc_type_to_iceberg(&col.data_type).with_context(|| {
            format!(
                "Unmappable type for column '{}.{}.{}'",
                source.schema, source.name, col.name
            )
        })?;
        fields.push(Arc::new(NestedField::optional(
            (i + 1) as i32,
            &col.name,
            iceberg_type,
        )));
    }

    let base = source.columns.len() as i32;
    fields.push(Arc::new(NestedField::required(
        base + 1,
        CDC_OP_COL,
        Type::Primitive(PrimitiveType::String),
    )));
    fields.push(Arc::new(NestedField::required(
        base + 2,
        CDC_POSITION_COL,
        Type::Primitive(PrimitiveType::Long),
    )));
    fields.push(Arc::new(NestedField::required(
        base + 3,
        CDC_TS_COL,
        Type::Primitive(PrimitiveType::Timestamptz),
    )));

    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Iceberg schema: {}", e))
}

/// Validate that every source column exists in an existing Iceberg table
/// schema. Extra (stale) columns in the table are fine; missing ones are
/// a hard error, since rows would silently lose data.
pub fn validate_existing_schema(source: &SourceTableSchema, table_schema: &Schema) -> Result<()> {
    let mut missing = Vec::new();
    for col in &source.columns {
        if table_schema.field_by_name(&col.name).is_none() {
            missing.push(col.name.clone());
        }
    }
    for meta in [CDC_OP_COL, CDC_POSITION_COL, CDC_TS_COL] {
        if table_schema.field_by_name(meta).is_none() {
            missing.push(meta.to_string());
        }
    }
    if !missing.is_empty() {
        anyhow::bail!(
            "Existing Iceberg table for '{}.{}' is missing column(s) {:?}. \
             Drop the table or migrate it before pointing dbmazz at it.",
            source.schema,
            source.name,
            missing
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::SourceColumn;

    fn col(name: &str, dt: DataType) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            data_type: dt,
            nullable: false,
            pg_type_id: None,
        }
    }

    fn all_types_schema() -> SourceTableSchema {
        SourceTableSchema {
            schema: "public".to_string(),
            name: "every_type".to_string(),
            columns: vec![
                col("c_bool", DataType::Boolean),
                col("c_i16", DataType::Int16),
                col("c_i32", DataType::Int32),
                col("c_i64", DataType::Int64),
                col("c_u64", DataType::UInt64),
                col("c_f32", DataType::Float32),
                col("c_f64", DataType::Float64),
                col(
                    "c_dec",
                    DataType::Decimal {
                        precision: 38,
                        scale: 9,
                    },
                ),
                col("c_str", DataType::String),
                col("c_text", DataType::Text),
                col("c_bytes", DataType::Bytes),
                col("c_json", DataType::Json),
                col("c_jsonb", DataType::Jsonb),
                col("c_uuid", DataType::Uuid),
                col("c_date", DataType::Date),
                col("c_time", DataType::Time),
                col("c_ts", DataType::Timestamp),
                col("c_tstz", DataType::TimestampTz),
            ],
            primary_keys: vec!["c_i64".to_string()],
        }
    }

    #[test]
    fn every_data_type_maps() {
        let schema = build_iceberg_schema(&all_types_schema()).unwrap();
        // 18 source columns + 3 metadata columns
        assert_eq!(schema.as_struct().fields().len(), 21);
    }

    #[test]
    fn uint64_maps_to_decimal_20_0() {
        let t = cdc_type_to_iceberg(&DataType::UInt64).unwrap();
        assert!(matches!(
            t,
            Type::Primitive(PrimitiveType::Decimal {
                precision: 20,
                scale: 0
            })
        ));
    }

    #[test]
    fn decimal_clamps_to_iceberg_limits() {
        let t = cdc_type_to_iceberg(&DataType::Decimal {
            precision: 0,
            scale: 0,
        })
        .unwrap();
        assert!(matches!(
            t,
            Type::Primitive(PrimitiveType::Decimal {
                precision: 1,
                scale: 0
            })
        ));
    }

    #[test]
    fn field_ids_are_stable_and_sequential() {
        let schema = build_iceberg_schema(&all_types_schema()).unwrap();
        let fields = schema.as_struct().fields();
        for (i, f) in fields.iter().enumerate() {
            assert_eq!(f.id, (i + 1) as i32);
        }
        assert_eq!(fields[fields.len() - 3].name, CDC_OP_COL);
        assert_eq!(fields[fields.len() - 2].name, CDC_POSITION_COL);
        assert_eq!(fields[fields.len() - 1].name, CDC_TS_COL);
    }

    #[test]
    fn table_naming_is_flat() {
        assert_eq!(iceberg_table_name("public", "users"), "public__users");
        assert_eq!(iceberg_table_name("", "users"), "users");
        let ident = table_ident("analytics", "public.users").unwrap();
        assert_eq!(ident.name(), "public__users");
    }

    #[test]
    fn validate_existing_schema_detects_missing_columns() {
        let source = all_types_schema();
        let full = build_iceberg_schema(&source).unwrap();
        assert!(validate_existing_schema(&source, &full).is_ok());

        let mut smaller = source.clone();
        smaller.columns.push(col("brand_new", DataType::String));
        let err = validate_existing_schema(&smaller, &full).unwrap_err();
        assert!(err.to_string().contains("brand_new"));
    }
}
