// Copyright 2025
// Licensed under the Elastic License v2.0

//! Type mappings between CDC DataTypes and Apache Iceberg types.
//!
//! Maps dbmazz internal types to Iceberg primitive types and Arrow
//! schemas for Parquet serialization.

use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};

use crate::core::record::DataType;
use crate::core::traits::SourceColumn;

/// Maps a CDC DataType to an Apache Iceberg type.
pub fn cdc_to_iceberg_type(dt: &DataType) -> Result<iceberg::spec::types::Type> {
    use iceberg::spec::types::Type as IcebergType;
    match dt {
        DataType::Boolean => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Boolean)),
        DataType::Int16 | DataType::Int32 => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Int)),
        DataType::Int64 => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Long)),
        DataType::Float32 => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Float)),
        DataType::Float64 => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Double)),
        DataType::Decimal(precision, scale) => {
            Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Decimal {
                precision: *precision,
                scale: *scale,
            }))
        }
        DataType::String | DataType::Json => {
            Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::String))
        }
        DataType::Uuid => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Uuid)),
        DataType::Date => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Date)),
        DataType::Time => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Time)),
        DataType::Timestamp => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::TimestampTz)),
        DataType::Bytes => Ok(IcebergType::Primitive(iceberg::spec::types::PrimitiveType::Binary)),
        DataType::Unchanged | DataType::Null => {
            Err(anyhow!("Cannot map Unchanged/Null to an Iceberg type"))
        }
    }
}

/// Maps a CDC DataType to an Arrow DataType for Parquet serialization.
pub fn cdc_to_arrow_type(dt: &DataType) -> Result<ArrowType> {
    match dt {
        DataType::Boolean => Ok(ArrowType::Boolean),
        DataType::Int16 => Ok(ArrowType::Int16),
        DataType::Int32 => Ok(ArrowType::Int32),
        DataType::Int64 => Ok(ArrowType::Int64),
        DataType::Float32 => Ok(ArrowType::Float32),
        DataType::Float64 => Ok(ArrowType::Float64),
        DataType::Decimal(p, s) => {
            // Arrow decimal: precision, scale
            Ok(ArrowType::Decimal256(*p as u8, *s as i8))
        }
        DataType::String | DataType::Json => Ok(ArrowType::Utf8),
        DataType::Uuid => Ok(ArrowType::Utf8), // UUID stored as string in Parquet
        DataType::Date => Ok(ArrowType::Date64),
        DataType::Time => Ok(ArrowType::Time64(arrow::datatypes::TimeUnit::Microsecond)),
        DataType::Timestamp => Ok(ArrowType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        DataType::Bytes => Ok(ArrowType::Binary),
        DataType::Unchanged | DataType::Null => {
            Err(anyhow!("Cannot map Unchanged/Null to an Arrow type"))
        }
    }
}

/// Builds an Arrow schema from source column definitions.
pub fn build_arrow_schema(columns: &[SourceColumn]) -> Result<ArrowSchema> {
    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let arrow_type = cdc_to_arrow_type(&col.data_type)
            .map_err(|e| anyhow!("Column '{}': {}", col.name, e))?;
        fields.push(ArrowField::new(&col.name, arrow_type, col.nullable));
    }
    Ok(ArrowSchema::new(fields))
}

/// Returns a human-readable string representation of an Iceberg type.
pub fn iceberg_type_to_string(t: &iceberg::spec::types::Type) -> String {
    // Simple display helper
    format!("{:?}", t)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;

    #[test]
    fn test_cdc_to_iceberg_basic_types() {
        use iceberg::spec::types::{PrimitiveType, Type as IcebergType};

        assert!(matches!(
            cdc_to_iceberg_type(&DataType::Boolean).unwrap(),
            IcebergType::Primitive(PrimitiveType::Boolean)
        ));
        assert!(matches!(
            cdc_to_iceberg_type(&DataType::Int32).unwrap(),
            IcebergType::Primitive(PrimitiveType::Int)
        ));
        assert!(matches!(
            cdc_to_iceberg_type(&DataType::Int64).unwrap(),
            IcebergType::Primitive(PrimitiveType::Long)
        ));
        assert!(matches!(
            cdc_to_iceberg_type(&DataType::Float32).unwrap(),
            IcebergType::Primitive(PrimitiveType::Float)
        ));
        assert!(matches!(
            cdc_to_iceberg_type(&DataType::Float64).unwrap(),
            IcebergType::Primitive(PrimitiveType::Double)
        ));
        assert!(matches!(
            cdc_to_iceberg_type(&DataType::String).unwrap(),
            IcebergType::Primitive(PrimitiveType::String)
        ));
        assert!(matches!(
            cdc_to_iceberg_type(&DataType::Date).unwrap(),
            IcebergType::Primitive(PrimitiveType::Date)
        ));
        assert!(matches!(
            cdc_to_iceberg_type(&DataType::Timestamp).unwrap(),
            IcebergType::Primitive(PrimitiveType::TimestampTz)
        ));
    }

    #[test]
    fn test_cdc_to_iceberg_decimal() {
        use iceberg::spec::types::{PrimitiveType, Type as IcebergType};
        let t = cdc_to_iceberg_type(&DataType::Decimal(10, 2)).unwrap();
        match t {
            IcebergType::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                assert_eq!(precision, 10);
                assert_eq!(scale, 2);
            }
            _ => panic!("Expected Decimal"),
        }
    }

    #[test]
    fn test_cdc_to_arrow_basic_types() {
        assert!(matches!(cdc_to_arrow_type(&DataType::Boolean).unwrap(), ArrowType::Boolean));
        assert!(matches!(cdc_to_arrow_type(&DataType::Int32).unwrap(), ArrowType::Int32));
        assert!(matches!(cdc_to_arrow_type(&DataType::Int64).unwrap(), ArrowType::Int64));
        assert!(matches!(cdc_to_arrow_type(&DataType::String).unwrap(), ArrowType::Utf8));
        assert!(matches!(cdc_to_arrow_type(&DataType::Timestamp).unwrap(), ArrowType::Timestamp(..)));
    }

    #[test]
    fn test_build_arrow_schema() {
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
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }
}
