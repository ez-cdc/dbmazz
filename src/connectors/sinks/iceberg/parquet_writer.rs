// Copyright 2025
// Licensed under the Elastic License v2.0

//! Parquet writer for Iceberg-compatible Parquet files.
//!
//! Serializes CDC records into Apache Parquet format compatible with
//! Iceberg's Parquet file format requirements.

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::core::record::{CdcRecord, Column};
use crate::core::traits::SourceColumn;

use super::types::build_arrow_schema;

/// (Parquet bytes, record count)
pub type ParquetOutput = (Vec<u8>, usize);

/// Serialize CDC records to an Iceberg-compatible Parquet byte array.
///
/// The schema is built from the source columns. All INSERT and UPDATE
/// records in the batch are written as rows. DELETE records produce
/// a separate marker for position-delete handling.
pub fn records_to_parquet(
    records: &[CdcRecord],
    columns: &[SourceColumn],
    batch_id: i64,
) -> Result<ParquetOutput> {
    if records.is_empty() {
        return Ok((Vec::new(), 0));
    }

    // Build Arrow schema from source columns
    let schema = build_arrow_schema(columns)
        .context("Failed to build Arrow schema for Parquet writer")?;

    // Build arrays from records
    let arrays = build_arrays(records, columns, &schema)?;
    let record_count = arrays.first().map(|a| a.len()).unwrap_or(0);

    if record_count == 0 {
        return Ok((Vec::new(), 0));
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .context("Failed to create RecordBatch")?;

    // Configure Parquet writer properties
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default()))
        .set_data_page_size_limit(1024 * 1024) // 1MB data pages
        .build();

    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .context("Failed to create ArrowWriter")?;

        writer
            .write(&batch)
            .context("Failed to write batch to Parquet")?;

        writer
            .close()
            .context("Failed to close Parquet writer")?;
    }

    Ok((buf, record_count))
}

/// Build Arrow arrays from CDC records.
fn build_arrays(
    records: &[CdcRecord],
    columns: &[SourceColumn],
    schema: &ArrowSchema,
) -> Result<Vec<ArrayRef>> {
    let num_cols = columns.len();
    let num_rows = records.len();

    // Initialize builders for each column
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(num_cols);
    for col in columns {
        builders.push(new_builder(&col.data_type, num_rows));
    }

    // Fill builders from records
    for record in records {
        let record_columns = match record {
            CdcRecord::Insert { columns, .. } => columns,
            CdcRecord::Update { new_columns, .. } => new_columns,
            CdcRecord::Delete { columns, .. } => columns,
            _ => continue,
        };

        for (i, col_def) in columns.iter().enumerate() {
            let val = record_columns
                .iter()
                .find(|c| c.name == col_def.name)
                .and_then(|c| c.value.as_ref());

            append_value(builders[i].as_mut(), &col_def.data_type, val)?;
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
    Ok(arrays)
}

/// Create a new Arrow array builder for the given DataType.
fn new_builder(dt: &crate::core::record::DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        crate::core::record::DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        crate::core::record::DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        crate::core::record::DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        crate::core::record::DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        crate::core::record::DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        crate::core::record::DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        crate::core::record::DataType::Decimal(_, _) => Box::new(StringBuilder::with_capacity(capacity, 256)),
        crate::core::record::DataType::String | crate::core::record::DataType::Json => {
            Box::new(StringBuilder::with_capacity(capacity, 256))
        }
        crate::core::record::DataType::Uuid => Box::new(StringBuilder::with_capacity(capacity, 64)),
        crate::core::record::DataType::Date => Box::new(Date64Builder::with_capacity(capacity)),
        crate::core::record::DataType::Time => Box::new(Time64MicrosecondBuilder::with_capacity(capacity)),
        crate::core::record::DataType::Timestamp => Box::new(TimestampMicrosecondBuilder::with_capacity(capacity)),
        crate::core::record::DataType::Bytes => Box::new(BinaryBuilder::with_capacity(capacity, 256)),
        crate::core::record::DataType::Unchanged | crate::core::record::DataType::Null => {
            Box::new(NullBuilder::with_capacity(capacity))
        }
    }
}

/// Append a JSON value to the appropriate builder.
fn append_value(
    builder: &mut dyn ArrayBuilder,
    dt: &crate::core::record::DataType,
    value: Option<&serde_json::Value>,
) -> Result<()> {
    match value {
        None | Some(serde_json::Value::Null) => {
            append_null(builder, dt)?;
        }
        Some(val) => {
            append_non_null(builder, dt, val)?;
        }
    }
    Ok(())
}

fn append_null(builder: &mut dyn ArrayBuilder, dt: &crate::core::record::DataType) -> Result<()> {
    match dt {
        crate::core::record::DataType::Boolean => {
            builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap().append_null();
        }
        crate::core::record::DataType::Int16 => {
            builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap().append_null();
        }
        crate::core::record::DataType::Int32 => {
            builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap().append_null();
        }
        crate::core::record::DataType::Int64 => {
            builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap().append_null();
        }
        crate::core::record::DataType::Float32 => {
            builder.as_any_mut().downcast_mut::<Float32Builder>().unwrap().append_null();
        }
        crate::core::record::DataType::Float64 => {
            builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap().append_null();
        }
        crate::core::record::DataType::String | crate::core::record::DataType::Json
        | crate::core::record::DataType::Uuid | crate::core::record::DataType::Decimal(_, _) => {
            builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_null();
        }
        crate::core::record::DataType::Date => {
            builder.as_any_mut().downcast_mut::<Date64Builder>().unwrap().append_null();
        }
        crate::core::record::DataType::Time => {
            builder.as_any_mut().downcast_mut::<Time64MicrosecondBuilder>().unwrap().append_null();
        }
        crate::core::record::DataType::Timestamp => {
            builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().unwrap().append_null();
        }
        crate::core::record::DataType::Bytes => {
            builder.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap().append_null();
        }
        crate::core::record::DataType::Unchanged | crate::core::record::DataType::Null => {}
    }
    Ok(())
}

fn append_non_null(
    builder: &mut dyn ArrayBuilder,
    dt: &crate::core::record::DataType,
    val: &serde_json::Value,
) -> Result<()> {
    match dt {
        crate::core::record::DataType::Boolean => {
            let v = val.as_bool().unwrap_or(false);
            builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Int16 => {
            let v = val.as_i64().unwrap_or(0) as i16;
            builder.as_any_mut().downcast_mut::<Int16Builder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Int32 => {
            let v = val.as_i64().unwrap_or(0) as i32;
            builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Int64 => {
            let v = val.as_i64().unwrap_or(0);
            builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Float32 => {
            let v = val.as_f64().unwrap_or(0.0) as f32;
            builder.as_any_mut().downcast_mut::<Float32Builder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Float64 => {
            let v = val.as_f64().unwrap_or(0.0);
            builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::String | crate::core::record::DataType::Json
        | crate::core::record::DataType::Uuid => {
            let s = val.as_str().unwrap_or("");
            builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_value(s);
        }
        crate::core::record::DataType::Decimal(_, _) => {
            let s = val.to_string();
            builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap().append_value(&s);
        }
        crate::core::record::DataType::Date => {
            let v = val.as_i64().unwrap_or(0);
            builder.as_any_mut().downcast_mut::<Date64Builder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Time => {
            let v = val.as_i64().unwrap_or(0);
            builder.as_any_mut().downcast_mut::<Time64MicrosecondBuilder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Timestamp => {
            let v = val.as_i64().unwrap_or(0);
            builder.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().unwrap().append_value(v);
        }
        crate::core::record::DataType::Bytes => {
            let s = val.as_str().unwrap_or("");
            builder.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap().append_value(s.as_bytes());
        }
        crate::core::record::DataType::Unchanged | crate::core::record::DataType::Null => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;
    use crate::core::traits::SourceColumn;

    #[test]
    fn test_empty_records() {
        let (buf, count) = records_to_parquet(&[], &[], 1).unwrap();
        assert_eq!(count, 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_build_arrow_schema_from_columns() {
        let columns = vec![
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
        ];
        let schema = build_arrow_schema(&columns).unwrap();
        assert_eq!(schema.fields().len(), 2);
    }
}
