// Copyright 2025
// Licensed under the Elastic License v2.0

//! Adapter layer for bridging legacy and new sink interfaces
//!
//! This module provides `NewSinkAdapter` which wraps a `Box<dyn core::Sink>`
//! and implements the legacy `sink::Sink` trait. This allows gradual migration
//! to the new connector system while maintaining backward compatibility.
//!
//! # Architecture
//!
//! ```text
//! Pipeline (legacy)
//!     |
//!     | push_batch(CdcMessage[])
//!     v
//! NewSinkAdapter
//!     |
//!     | convert to CdcRecord[]
//!     v
//! Box<dyn core::Sink>
//!     |
//!     | write_batch(CdcRecord[])
//!     v
//! StarRocksSink (new)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use dbmazz::connectors::sinks::create_sink;
//! use dbmazz::sink::adapter::NewSinkAdapter;
//!
//! let new_sink = create_sink(&config)?;
//! let adapter = NewSinkAdapter::new(new_sink);
//!
//! // Now `adapter` implements legacy sink::Sink trait
//! pipeline.set_sink(Box::new(adapter));
//! ```

use anyhow::Result;
use async_trait::async_trait;

use crate::core::{
    CdcRecord, ColumnDef, ColumnValue, DataType, Sink as CoreSink, SinkCapabilities,
    SourcePosition, TableRef, Value,
};
use crate::pipeline::schema_cache::{SchemaCache, SchemaDelta, TableSchema};
use crate::source::parser::{CdcMessage, TupleData};

/// Adapter that wraps a new `core::Sink` to implement the legacy `sink::Sink` trait.
///
/// This enables the existing Pipeline to work with new trait-based connectors
/// without requiring immediate changes to the pipeline itself.
pub struct NewSinkAdapter {
    inner: Box<dyn CoreSink>,
}

impl NewSinkAdapter {
    /// Create a new adapter wrapping a core::Sink
    pub fn new(sink: Box<dyn CoreSink>) -> Self {
        Self { inner: sink }
    }

    /// Get the capabilities of the underlying sink
    pub fn capabilities(&self) -> SinkCapabilities {
        self.inner.capabilities()
    }

    /// Validate the connection to the underlying sink
    pub async fn validate_connection(&self) -> Result<()> {
        self.inner.validate_connection().await
    }

    /// Verify HTTP connection (for StarRocks compatibility)
    pub async fn verify_http_connection(&self) -> Result<()> {
        self.inner.validate_connection().await
    }

    /// Convert a legacy CdcMessage batch to CdcRecord batch
    fn convert_batch(
        &self,
        batch: &[CdcMessage],
        schema_cache: &SchemaCache,
        lsn: u64,
    ) -> Vec<CdcRecord> {
        let mut records = Vec::with_capacity(batch.len());
        let position = SourcePosition::Lsn(lsn);

        for msg in batch {
            if let Some(record) = self.convert_message(msg, schema_cache, &position) {
                records.push(record);
            }
        }

        records
    }

    /// Convert a single CdcMessage to CdcRecord
    fn convert_message(
        &self,
        msg: &CdcMessage,
        schema_cache: &SchemaCache,
        position: &SourcePosition,
    ) -> Option<CdcRecord> {
        match msg {
            CdcMessage::Begin {
                xid, ..
            } => Some(CdcRecord::Begin { xid: *xid as u64 }),

            CdcMessage::Commit { end_lsn, .. } => Some(CdcRecord::Commit {
                xid: 0,
                position: SourcePosition::Lsn(*end_lsn),
            }),

            CdcMessage::Relation {
                namespace,
                name,
                columns,
                ..
            } => {
                let column_defs = columns
                    .iter()
                    .map(|c| ColumnDef::new(c.name.clone(), pg_type_to_data_type(c.type_id), true))
                    .collect();

                Some(CdcRecord::SchemaChange {
                    table: TableRef::new(Some(namespace.clone()), name.clone()),
                    columns: column_defs,
                    position: position.clone(),
                })
            }

            CdcMessage::Insert { relation_id, tuple } => {
                let schema = schema_cache.get(*relation_id)?;
                let columns = self.tuple_to_column_values(tuple, schema);

                Some(CdcRecord::Insert {
                    table: TableRef::new(Some("public".to_string()), schema.name.clone()),
                    columns,
                    position: position.clone(),
                })
            }

            CdcMessage::Update {
                relation_id,
                old_tuple,
                new_tuple,
            } => {
                let schema = schema_cache.get(*relation_id)?;
                let new_columns = self.tuple_to_column_values(new_tuple, schema);
                let old_columns = old_tuple.as_ref().map(|t| self.tuple_to_column_values(t, schema));

                Some(CdcRecord::Update {
                    table: TableRef::new(Some("public".to_string()), schema.name.clone()),
                    old_columns,
                    new_columns,
                    position: position.clone(),
                })
            }

            CdcMessage::Delete {
                relation_id,
                old_tuple,
            } => {
                let schema = schema_cache.get(*relation_id)?;
                let old = old_tuple.as_ref()?;
                let columns = self.tuple_to_column_values(old, schema);

                Some(CdcRecord::Delete {
                    table: TableRef::new(Some("public".to_string()), schema.name.clone()),
                    columns,
                    position: position.clone(),
                })
            }

            CdcMessage::KeepAlive { wal_end, .. } => Some(CdcRecord::Heartbeat {
                position: SourcePosition::Lsn(*wal_end),
            }),

            CdcMessage::Unknown => None,
            CdcMessage::LogicalMessage { .. } => None, // watermark messages — skip
        }
    }

    /// Convert a Tuple to ColumnValue vector using schema info
    fn tuple_to_column_values(
        &self,
        tuple: &crate::source::parser::Tuple,
        schema: &TableSchema,
    ) -> Vec<ColumnValue> {
        schema
            .columns
            .iter()
            .zip(tuple.cols.iter())
            .map(|(col, data)| {
                let value = match data {
                    TupleData::Null => Value::Null,
                    TupleData::Toast => Value::Unchanged,
                    TupleData::Text(bytes) => {
                        let text = String::from_utf8_lossy(bytes);
                        self.convert_pg_value(&text, col.type_id)
                    }
                };
                ColumnValue::new(col.name.clone(), value)
            })
            .collect()
    }

    /// Convert a PostgreSQL text value to a generic Value based on type OID
    fn convert_pg_value(&self, text: &str, pg_type_id: u32) -> Value {
        use crate::connectors::sources::postgres::types::{
            normalize_timestamptz, parse_pg_array, strip_money_symbol,
        };

        match pg_type_id {
            // Boolean
            16 => {
                let is_true = matches!(text.to_lowercase().as_str(), "t" | "true" | "1");
                Value::Bool(is_true)
            }
            // Integer types (INT2, INT4, INT8)
            21 | 23 | 20 => text.parse::<i64>().map(Value::Int64).unwrap_or_else(|_| Value::String(text.to_string())),
            // Float types (FLOAT4, FLOAT8)
            700 | 701 => text.parse::<f64>().map(Value::Float64).unwrap_or_else(|_| Value::String(text.to_string())),
            // Money - strip currency symbol
            790 => Value::Decimal(strip_money_symbol(text)),
            // NUMERIC/DECIMAL - keep as string for precision
            1700 => Value::Decimal(text.to_string()),
            // Timestamp (no TZ) - keep as-is
            1114 => Value::String(text.to_string()),
            // TimestampTZ - normalize to UTC
            1184 => Value::String(normalize_timestamptz(text)),
            // JSON/JSONB
            114 | 3802 => Value::Json(text.to_string()),
            // UUID
            2950 => Value::Uuid(text.to_string()),
            // Integer arrays
            1005 | 1007 | 1016 => Value::Json(parse_pg_array(text, "int")),
            // Float arrays
            1021 | 1022 => Value::Json(parse_pg_array(text, "float")),
            // Text/varchar arrays
            1009 | 1015 => Value::Json(parse_pg_array(text, "text")),
            // Default: string
            _ => Value::String(text.to_string()),
        }
    }
}

/// Convert PostgreSQL type OID to generic DataType
fn pg_type_to_data_type(pg_type_id: u32) -> DataType {
    match pg_type_id {
        16 => DataType::Boolean,
        21 => DataType::Int16,
        23 => DataType::Int32,
        20 => DataType::Int64,
        700 => DataType::Float32,
        701 => DataType::Float64,
        1700 => DataType::Decimal {
            precision: 38,
            scale: 9,
        },
        1114 => DataType::Timestamp,
        1184 => DataType::TimestampTz,
        25 | 1043 | 1042 => DataType::String,
        114 | 3802 => DataType::Jsonb,
        2950 => DataType::Uuid,
        17 => DataType::Bytes,
        _ => DataType::String,
    }
}

#[async_trait]
impl crate::sink::Sink for NewSinkAdapter {
    async fn push_batch(
        &mut self,
        batch: &[CdcMessage],
        schema_cache: &SchemaCache,
        lsn: u64,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // Convert legacy CdcMessage to new CdcRecord format
        let records = self.convert_batch(batch, schema_cache, lsn);

        if records.is_empty() {
            return Ok(());
        }

        // Write using the new sink
        let _result = self.inner.write_batch(records).await?;

        Ok(())
    }

    async fn apply_schema_delta(&self, _delta: &SchemaDelta) -> Result<()> {
        // Schema evolution in the new sink is handled via SchemaChange records
        // The new sink doesn't have a separate apply_schema_delta method.
        // For now, we just return Ok - the schema changes are embedded in the record stream.
        //
        // TODO: If we need DDL support, we could:
        // 1. Add an optional schema_evolution method to core::Sink
        // 2. Or handle DDL in the connector's write_batch when it sees SchemaChange records
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_to_data_type() {
        assert_eq!(pg_type_to_data_type(16), DataType::Boolean);
        assert_eq!(pg_type_to_data_type(21), DataType::Int16);
        assert_eq!(pg_type_to_data_type(23), DataType::Int32);
        assert_eq!(pg_type_to_data_type(20), DataType::Int64);
        assert_eq!(pg_type_to_data_type(700), DataType::Float32);
        assert_eq!(pg_type_to_data_type(701), DataType::Float64);
        assert_eq!(pg_type_to_data_type(1114), DataType::Timestamp);
        assert_eq!(pg_type_to_data_type(25), DataType::String);
        assert_eq!(pg_type_to_data_type(3802), DataType::Jsonb);
        assert_eq!(pg_type_to_data_type(2950), DataType::Uuid);
        // Unknown type defaults to String
        assert_eq!(pg_type_to_data_type(99999), DataType::String);
    }

    #[test]
    fn test_convert_pg_value_bool() {
        let adapter = NewSinkAdapter {
            inner: Box::new(MockSink),
        };

        assert!(matches!(adapter.convert_pg_value("t", 16), Value::Bool(true)));
        assert!(matches!(adapter.convert_pg_value("true", 16), Value::Bool(true)));
        assert!(matches!(adapter.convert_pg_value("1", 16), Value::Bool(true)));
        assert!(matches!(adapter.convert_pg_value("f", 16), Value::Bool(false)));
        assert!(matches!(adapter.convert_pg_value("false", 16), Value::Bool(false)));
    }

    #[test]
    fn test_convert_pg_value_int() {
        let adapter = NewSinkAdapter {
            inner: Box::new(MockSink),
        };

        assert!(matches!(adapter.convert_pg_value("42", 23), Value::Int64(42)));
        assert!(matches!(adapter.convert_pg_value("-100", 20), Value::Int64(-100)));
        // Invalid int falls back to string
        assert!(matches!(adapter.convert_pg_value("not_a_number", 23), Value::String(_)));
    }

    #[test]
    fn test_convert_pg_value_float() {
        let adapter = NewSinkAdapter {
            inner: Box::new(MockSink),
        };

        if let Value::Float64(f) = adapter.convert_pg_value("3.5", 701) {
            assert!((f - 3.5).abs() < 0.001);
        } else {
            panic!("Expected Float64");
        }
    }

    // Mock sink for testing
    struct MockSink;

    #[async_trait]
    impl CoreSink for MockSink {
        fn name(&self) -> &'static str {
            "mock"
        }

        fn capabilities(&self) -> SinkCapabilities {
            SinkCapabilities {
                supports_upsert: true,
                supports_delete: true,
                supports_schema_evolution: false,
                supports_transactions: false,
                loading_model: crate::core::LoadingModel::Streaming,
                min_batch_size: None,
                max_batch_size: None,
                optimal_flush_interval_ms: 1000,
            }
        }

        async fn validate_connection(&self) -> Result<()> {
            Ok(())
        }

        async fn write_batch(&mut self, _records: Vec<CdcRecord>) -> Result<crate::core::SinkResult> {
            Ok(crate::core::SinkResult {
                records_written: 0,
                bytes_written: 0,
                last_position: None,
            })
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }
}
