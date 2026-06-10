// Copyright 2026
// Licensed under the Elastic License v2.0

//! CdcRecord → Arrow RecordBatch conversion for the Iceberg sink.
//!
//! Conversion is driven by the *table's* Arrow schema (derived from the
//! Iceberg schema, field-ids included), never by per-record inference.
//! Type mismatches are hard errors naming table, column, and both types —
//! values are never silently coerced to defaults.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowType, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime};

use crate::core::record::{CdcRecord, ColumnValue, Value};
use crate::core::SourcePosition;

use super::schema::{CDC_OP_COL, CDC_POSITION_COL, CDC_TS_COL};

/// Result of converting one table's records.
#[derive(Debug)]
pub struct ConvertedBatch {
    pub batch: RecordBatch,
    /// TOAST `Value::Unchanged` occurrences written as NULL.
    pub unchanged_as_null: usize,
    /// Record column names absent from the table schema (schema drift),
    /// whose values were skipped.
    pub skipped_columns: Vec<String>,
}

/// Scalar position for `_cdc_position`. PG LSNs are globally ordered;
/// MySQL binlog positions are per-file (see sink README for the caveat).
fn position_to_i64(pos: &SourcePosition) -> i64 {
    match pos {
        SourcePosition::Lsn(lsn) => *lsn as i64,
        SourcePosition::Offset(off) => *off,
        SourcePosition::MysqlBinlog { position, .. } => *position as i64,
        SourcePosition::GtidSet(_) | SourcePosition::FilePosition { .. } => 0,
    }
}

fn op_and_columns(record: &CdcRecord) -> Option<(&'static str, &[ColumnValue], &SourcePosition)> {
    match record {
        CdcRecord::Insert {
            columns, position, ..
        } => Some(("I", columns, position)),
        CdcRecord::Update {
            new_columns,
            position,
            ..
        } => Some(("U", new_columns, position)),
        CdcRecord::Delete {
            columns, position, ..
        } => Some(("D", columns, position)),
        _ => None,
    }
}

/// Convert data records (Insert/Update/Delete) into a RecordBatch matching
/// `arrow_schema`. Non-data records must be filtered out by the caller.
pub fn records_to_batch(
    table_name: &str,
    arrow_schema: &Arc<ArrowSchema>,
    records: &[CdcRecord],
    write_ts_micros: i64,
) -> Result<ConvertedBatch> {
    let fields = arrow_schema.fields();
    let mut builders: Vec<ColumnBuilder> = Vec::with_capacity(fields.len());
    for field in fields {
        builders.push(ColumnBuilder::for_type(field.data_type()).with_context(|| {
            format!(
                "Unsupported Arrow type for column '{}.{}'",
                table_name,
                field.name()
            )
        })?);
    }

    // Column name → schema index, for record-value lookup and drift detection.
    let index_by_name: HashMap<&str, usize> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();

    let mut unchanged_as_null = 0usize;
    let mut skipped: Vec<String> = Vec::new();
    let mut row_count = 0usize;

    for record in records {
        let Some((op, columns, position)) = op_and_columns(record) else {
            continue;
        };
        row_count += 1;

        // Values for this row, positioned by schema index.
        let mut row: Vec<Option<&Value>> = vec![None; fields.len()];
        for cv in columns {
            match index_by_name.get(cv.name.as_str()) {
                Some(&i) => row[i] = Some(&cv.value),
                None => {
                    if !skipped.contains(&cv.name) {
                        skipped.push(cv.name.clone());
                    }
                }
            }
        }

        for (i, field) in fields.iter().enumerate() {
            let builder = &mut builders[i];
            match field.name().as_str() {
                CDC_OP_COL => {
                    builder.append(table_name, field.name(), &Value::String(op.to_string()))?
                }
                CDC_POSITION_COL => builder.append(
                    table_name,
                    field.name(),
                    &Value::Int64(position_to_i64(position)),
                )?,
                CDC_TS_COL => {
                    builder.append(table_name, field.name(), &Value::Timestamp(write_ts_micros))?
                }
                _ => match row[i] {
                    None | Some(Value::Null) => builder.append_null(),
                    Some(Value::Unchanged) => {
                        unchanged_as_null += 1;
                        builder.append_null();
                    }
                    Some(value) => builder.append(table_name, field.name(), value)?,
                },
            }
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
        .with_context(|| format!("Failed to build RecordBatch for '{}'", table_name))?;
    debug_assert_eq!(batch.num_rows(), row_count);

    Ok(ConvertedBatch {
        batch,
        unchanged_as_null,
        skipped_columns: skipped,
    })
}

/// Typed Arrow builder for one column. The variant is chosen from the
/// Arrow field type so the finished array always matches the schema.
enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
    Utf8(StringBuilder),
    Date32(Date32Builder),
    Time64(Time64MicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    FixedBinary16(FixedSizeBinaryBuilder),
    LargeBinary(LargeBinaryBuilder),
}

impl ColumnBuilder {
    fn for_type(dt: &ArrowType) -> Result<Self> {
        let b = match dt {
            ArrowType::Boolean => Self::Bool(BooleanBuilder::new()),
            ArrowType::Int32 => Self::Int32(Int32Builder::new()),
            ArrowType::Int64 => Self::Int64(Int64Builder::new()),
            ArrowType::Float32 => Self::Float32(Float32Builder::new()),
            ArrowType::Float64 => Self::Float64(Float64Builder::new()),
            ArrowType::Decimal128(p, s) => Self::Decimal {
                builder: Decimal128Builder::new().with_data_type(dt.clone()),
                precision: *p,
                scale: *s,
            },
            ArrowType::Utf8 => Self::Utf8(StringBuilder::new()),
            ArrowType::Date32 => Self::Date32(Date32Builder::new()),
            ArrowType::Time64(TimeUnit::Microsecond) => {
                Self::Time64(Time64MicrosecondBuilder::new())
            }
            ArrowType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::Timestamp(TimestampMicrosecondBuilder::new().with_data_type(dt.clone()))
            }
            ArrowType::FixedSizeBinary(16) => Self::FixedBinary16(FixedSizeBinaryBuilder::new(16)),
            ArrowType::LargeBinary => Self::LargeBinary(LargeBinaryBuilder::new()),
            other => bail!(
                "Arrow type {:?} is not supported by the Iceberg sink",
                other
            ),
        };
        Ok(b)
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Decimal { builder, .. } => builder.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Time64(b) => b.append_null(),
            Self::Timestamp(b) => b.append_null(),
            Self::FixedBinary16(b) => b.append_null(),
            Self::LargeBinary(b) => b.append_null(),
        }
    }

    fn append(&mut self, table: &str, column: &str, value: &Value) -> Result<()> {
        let mismatch = |expected: &str| -> anyhow::Error {
            anyhow::anyhow!(
                "Type mismatch in '{}.{}': expected {}, got {}",
                table,
                column,
                expected,
                value_variant_name(value)
            )
        };

        // The snapshot path delivers every column as text (`Value::String`
        // from PG's text protocol), while the WAL path delivers typed
        // values. Each arm therefore also accepts a string form, parsed
        // strictly — a parse failure is a hard error, never a default.
        match self {
            Self::Bool(b) => match value {
                Value::Bool(v) => b.append_value(*v),
                Value::String(s) => match s.as_str() {
                    "t" | "true" | "TRUE" | "1" => b.append_value(true),
                    "f" | "false" | "FALSE" | "0" => b.append_value(false),
                    _ => {
                        anyhow::bail!("Invalid boolean '{}' for '{}.{}'", s, table, column)
                    }
                },
                _ => return Err(mismatch("boolean")),
            },
            Self::Int32(b) => {
                let v = match value {
                    Value::Int64(v) => *v,
                    Value::String(s) => s.parse::<i64>().with_context(|| {
                        format!("Invalid integer '{}' for '{}.{}'", s, table, column)
                    })?,
                    _ => return Err(mismatch("int32")),
                };
                let v = i32::try_from(v).map_err(|_| {
                    anyhow::anyhow!("Value {} out of i32 range for '{}.{}'", v, table, column)
                })?;
                b.append_value(v);
            }
            Self::Int64(b) => match value {
                Value::Int64(v) => b.append_value(*v),
                Value::UInt64(v) => {
                    let v = i64::try_from(*v).map_err(|_| {
                        anyhow::anyhow!(
                            "Value {} out of i64 range for '{}.{}' (column should be decimal(20,0))",
                            v,
                            table,
                            column
                        )
                    })?;
                    b.append_value(v);
                }
                Value::String(s) => {
                    let v = s.parse::<i64>().with_context(|| {
                        format!("Invalid integer '{}' for '{}.{}'", s, table, column)
                    })?;
                    b.append_value(v);
                }
                _ => return Err(mismatch("int64")),
            },
            Self::Float32(b) => match value {
                // FLOAT4 sources arrive as Value::Float64; the round-trip
                // through f64 is exact for genuine f32 source values.
                Value::Float64(v) => b.append_value(*v as f32),
                Value::String(s) => {
                    let v = s.parse::<f32>().with_context(|| {
                        format!("Invalid float '{}' for '{}.{}'", s, table, column)
                    })?;
                    b.append_value(v);
                }
                _ => return Err(mismatch("float32")),
            },
            Self::Float64(b) => match value {
                Value::Float64(v) => b.append_value(*v),
                Value::String(s) => {
                    let v = s.parse::<f64>().with_context(|| {
                        format!("Invalid float '{}' for '{}.{}'", s, table, column)
                    })?;
                    b.append_value(v);
                }
                _ => return Err(mismatch("float64")),
            },
            Self::Decimal {
                builder,
                precision,
                scale,
            } => {
                let mantissa = match value {
                    Value::Decimal(s) | Value::String(s) => {
                        decimal_str_to_i128(s, *precision, *scale).with_context(|| {
                            format!("Invalid decimal '{}' for '{}.{}'", s, table, column)
                        })?
                    }
                    Value::Int64(v) => scale_int_to_i128(*v as i128, *precision, *scale)
                        .with_context(|| {
                            format!("Integer {} overflows '{}.{}'", v, table, column)
                        })?,
                    Value::UInt64(v) => scale_int_to_i128(*v as i128, *precision, *scale)
                        .with_context(|| {
                            format!("Integer {} overflows '{}.{}'", v, table, column)
                        })?,
                    _ => return Err(mismatch("decimal")),
                };
                builder.append_value(mantissa);
            }
            Self::Utf8(b) => match value {
                Value::String(s) => b.append_value(s),
                Value::Json(s) => b.append_value(s),
                _ => return Err(mismatch("string")),
            },
            Self::Date32(b) => {
                let days = match value {
                    Value::String(s) => parse_date_days(s).with_context(|| {
                        format!("Invalid date '{}' for '{}.{}'", s, table, column)
                    })?,
                    Value::Timestamp(micros) => micros_to_days(*micros),
                    _ => return Err(mismatch("date")),
                };
                b.append_value(days);
            }
            Self::Time64(b) => {
                let micros = match value {
                    Value::String(s) => parse_time_micros(s).with_context(|| {
                        format!("Invalid time '{}' for '{}.{}'", s, table, column)
                    })?,
                    Value::Timestamp(micros) => *micros,
                    _ => return Err(mismatch("time")),
                };
                b.append_value(micros);
            }
            Self::Timestamp(b) => {
                let micros = match value {
                    Value::Timestamp(micros) => *micros,
                    Value::String(s) => parse_timestamp_micros(s).with_context(|| {
                        format!("Invalid timestamp '{}' for '{}.{}'", s, table, column)
                    })?,
                    _ => return Err(mismatch("timestamp")),
                };
                b.append_value(micros);
            }
            Self::FixedBinary16(b) => {
                let bytes = match value {
                    Value::Uuid(s) | Value::String(s) => *uuid::Uuid::parse_str(s)
                        .with_context(|| {
                            format!("Invalid UUID '{}' for '{}.{}'", s, table, column)
                        })?
                        .as_bytes(),
                    _ => return Err(mismatch("uuid")),
                };
                b.append_value(bytes)
                    .context("FixedSizeBinary(16) append failed")?;
            }
            Self::LargeBinary(b) => match value {
                Value::Bytes(bytes) => b.append_value(bytes),
                // PG bytea arrives as text in `\xDEADBEEF` hex form.
                Value::String(s) => {
                    if let Some(hex_part) = s.strip_prefix("\\x") {
                        let decoded = hex::decode(hex_part).with_context(|| {
                            format!("Invalid bytea hex for '{}.{}'", table, column)
                        })?;
                        b.append_value(&decoded);
                    } else {
                        b.append_value(s.as_bytes());
                    }
                }
                _ => return Err(mismatch("binary")),
            },
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Decimal { mut builder, .. } => Arc::new(builder.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::Time64(mut b) => Arc::new(b.finish()),
            Self::Timestamp(mut b) => Arc::new(b.finish()),
            Self::FixedBinary16(mut b) => Arc::new(b.finish()),
            Self::LargeBinary(mut b) => Arc::new(b.finish()),
        }
    }
}

fn value_variant_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::UInt64(_) => "UInt64",
        Value::Float64(_) => "Float64",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::Json(_) => "Json",
        Value::Timestamp(_) => "Timestamp",
        Value::Decimal(_) => "Decimal",
        Value::Uuid(_) => "Uuid",
        Value::Unchanged => "Unchanged",
    }
}

/// Parse a decimal string into an i128 mantissa at the given scale.
/// More fractional digits than `scale` is an error (strict — no silent
/// rounding); fewer are zero-padded.
fn decimal_str_to_i128(s: &str, precision: u8, scale: i8) -> Result<i128> {
    let s = s.trim();
    let (negative, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };

    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        bail!("empty decimal");
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        bail!("non-numeric decimal");
    }

    let scale = scale.max(0) as usize;
    if frac_part.len() > scale {
        // Trailing zeros beyond the scale are harmless; anything else
        // would silently lose precision.
        let (keep, excess) = frac_part.split_at(scale);
        if excess.chars().any(|c| c != '0') {
            bail!(
                "decimal has {} fractional digits but column scale is {}",
                frac_part.len(),
                scale
            );
        }
        return assemble_decimal(negative, int_part, keep, scale, precision);
    }
    assemble_decimal(negative, int_part, frac_part, scale, precision)
}

fn assemble_decimal(
    negative: bool,
    int_part: &str,
    frac_part: &str,
    scale: usize,
    precision: u8,
) -> Result<i128> {
    let mut mantissa: i128 = if int_part.is_empty() {
        0
    } else {
        int_part.parse::<i128>().context("integer part overflow")?
    };
    for c in frac_part.chars() {
        mantissa = mantissa
            .checked_mul(10)
            .and_then(|m| m.checked_add((c as u8 - b'0') as i128))
            .context("decimal overflow")?;
    }
    for _ in frac_part.len()..scale {
        mantissa = mantissa.checked_mul(10).context("decimal overflow")?;
    }
    let max = 10i128
        .checked_pow(precision as u32)
        .context("precision too large")?
        - 1;
    if mantissa > max {
        bail!("decimal exceeds precision {}", precision);
    }
    Ok(if negative { -mantissa } else { mantissa })
}

fn scale_int_to_i128(v: i128, precision: u8, scale: i8) -> Result<i128> {
    let mut mantissa = v;
    for _ in 0..scale.max(0) {
        mantissa = mantissa.checked_mul(10).context("decimal overflow")?;
    }
    let max = 10i128
        .checked_pow(precision as u32)
        .context("precision too large")?
        - 1;
    if mantissa.abs() > max {
        bail!("integer exceeds decimal precision {}", precision);
    }
    Ok(mantissa)
}

const EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// Parse `YYYY-MM-DD` (ignoring any trailing time component, as emitted by
/// the MySQL DATE path) into days since the Unix epoch.
fn parse_date_days(s: &str) -> Result<i32> {
    let date_part = s.get(0..10).unwrap_or(s);
    let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d").context("expected YYYY-MM-DD")?;
    Ok(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
}

fn micros_to_days(micros: i64) -> i32 {
    (micros / 86_400_000_000).try_into().unwrap_or(0)
}

/// Parse `HH:MM:SS[.ffffff]`, tolerating the MySQL `D H:MM:SS` day-prefixed
/// form, into microseconds since midnight.
fn parse_time_micros(s: &str) -> Result<i64> {
    let s = s.trim();
    // MySQL TIME can carry a day count ("0 10:30:00"); fold it in.
    let (days, time_part) = match s.split_once(' ') {
        Some((d, rest)) if d.chars().all(|c| c.is_ascii_digit() || c == '-') => {
            (d.parse::<i64>().unwrap_or(0), rest)
        }
        _ => (0, s),
    };
    if days < 0 || s.starts_with('-') {
        bail!("negative TIME values cannot be represented as time-of-day");
    }
    let t = NaiveTime::parse_from_str(time_part, "%H:%M:%S%.f")
        .context("expected HH:MM:SS[.ffffff]")?;
    use chrono::Timelike;
    let micros_of_day =
        t.num_seconds_from_midnight() as i64 * 1_000_000 + (t.nanosecond() / 1_000) as i64;
    Ok(days * 86_400_000_000 + micros_of_day)
}

/// Parse a PG-style timestamp string into epoch microseconds.
///
/// Accepts the WAL-path form (`YYYY-MM-DD HH:MM:SS[.ffffff]`, naive or
/// UTC-normalized) and the snapshot-path timestamptz form with an offset
/// suffix (`… +00` / `…+05:30`), as emitted by PG's text protocol.
fn parse_timestamp_micros(s: &str) -> Result<i64> {
    let s = s.trim();
    let normalized = s.replacen('T', " ", 1);

    if let Ok(dt) = NaiveDateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    // `%#z` parses both short (`+00`) and full (`+00:00`) offsets.
    chrono::DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%.f%#z")
        .map(|dt| dt.timestamp_micros())
        .context("expected YYYY-MM-DD HH:MM:SS[.ffffff][±TZ]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::record::DataType;
    use crate::core::record::{ColumnValue, TableRef};
    use crate::core::traits::{SourceColumn, SourceTableSchema};

    fn arrow_schema_for(columns: Vec<SourceColumn>) -> Arc<ArrowSchema> {
        let source = SourceTableSchema {
            schema: "public".to_string(),
            name: "t".to_string(),
            columns,
            primary_keys: vec![],
        };
        let iceberg_schema = super::super::schema::build_iceberg_schema(&source).unwrap();
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).unwrap())
    }

    fn col(name: &str, dt: DataType) -> SourceColumn {
        SourceColumn {
            name: name.to_string(),
            data_type: dt,
            nullable: true,
            pg_type_id: None,
        }
    }

    fn insert(columns: Vec<ColumnValue>) -> CdcRecord {
        CdcRecord::Insert {
            table: TableRef::new(Some("public".to_string()), "t".to_string()),
            columns,
            position: SourcePosition::Lsn(42),
        }
    }

    fn cv(name: &str, value: Value) -> ColumnValue {
        ColumnValue {
            name: name.to_string(),
            value,
        }
    }

    #[test]
    fn uint64_above_i64_max_survives_in_decimal_column() {
        let schema = arrow_schema_for(vec![col("big", DataType::UInt64)]);
        let records = vec![insert(vec![cv("big", Value::UInt64(u64::MAX))])];
        let out = records_to_batch("public.t", &schema, &records, 0).unwrap();
        assert_eq!(out.batch.num_rows(), 1);
        let arr = out
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), u64::MAX as i128);
    }

    #[test]
    fn timestamp_column_accepts_pg_string_and_mysql_micros() {
        let schema = arrow_schema_for(vec![
            col("ts", DataType::Timestamp),
            col("tstz", DataType::TimestampTz),
        ]);
        let records = vec![
            insert(vec![
                cv(
                    "ts",
                    Value::String("2026-01-15 10:30:00.123456".to_string()),
                ),
                cv("tstz", Value::String("2026-01-15 10:30:00".to_string())),
            ]),
            insert(vec![
                cv("ts", Value::Timestamp(1_700_000_000_000_000)),
                cv("tstz", Value::Timestamp(1_700_000_000_000_000)),
            ]),
        ];
        let out = records_to_batch("public.t", &schema, &records, 0).unwrap();
        assert_eq!(out.batch.num_rows(), 2);
    }

    #[test]
    fn type_mismatch_is_a_hard_error_naming_the_column() {
        let schema = arrow_schema_for(vec![col("n", DataType::Int64)]);
        let records = vec![insert(vec![cv("n", Value::Bool(true))])];
        let err = records_to_batch("public.t", &schema, &records, 0).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("public.t"), "missing table in: {}", msg);
        assert!(msg.contains(".n'"), "missing column in: {}", msg);
        assert!(msg.contains("Bool"), "missing actual type in: {}", msg);
    }

    #[test]
    fn unparseable_string_is_a_hard_error_not_a_default() {
        // Snapshot rows arrive as text; parse failures must fail the batch.
        let schema = arrow_schema_for(vec![col("n", DataType::Int64)]);
        let records = vec![insert(vec![cv("n", Value::String("oops".to_string()))])];
        let err = records_to_batch("public.t", &schema, &records, 0).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("oops"), "missing value in: {}", msg);
        assert!(msg.contains(".n'"), "missing column in: {}", msg);
    }

    #[test]
    fn snapshot_text_values_are_parsed_into_typed_columns() {
        // The snapshot path emits every column as Value::String.
        let schema = arrow_schema_for(vec![
            col("i", DataType::Int64),
            col("f", DataType::Float64),
            col("b", DataType::Boolean),
            col(
                "d",
                DataType::Decimal {
                    precision: 38,
                    scale: 9,
                },
            ),
        ]);
        let records = vec![insert(vec![
            cv("i", Value::String("42".to_string())),
            cv("f", Value::String("1.5".to_string())),
            cv("b", Value::String("t".to_string())),
            cv("d", Value::String("100.50".to_string())),
        ])];
        let out = records_to_batch("public.t", &schema, &records, 0).unwrap();
        assert_eq!(out.batch.num_rows(), 1);
        let ints = out
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(ints.value(0), 42);
    }

    #[test]
    fn unchanged_toast_becomes_null_and_is_counted() {
        let schema = arrow_schema_for(vec![col("payload", DataType::Text)]);
        let records = vec![insert(vec![cv("payload", Value::Unchanged)])];
        let out = records_to_batch("public.t", &schema, &records, 0).unwrap();
        assert_eq!(out.unchanged_as_null, 1);
        assert!(out.batch.column(0).is_null(0));
    }

    #[test]
    fn unknown_record_columns_are_reported_as_skipped() {
        let schema = arrow_schema_for(vec![col("a", DataType::Int64)]);
        let records = vec![insert(vec![
            cv("a", Value::Int64(1)),
            cv("added_later", Value::Int64(2)),
        ])];
        let out = records_to_batch("public.t", &schema, &records, 0).unwrap();
        assert_eq!(out.skipped_columns, vec!["added_later".to_string()]);
    }

    #[test]
    fn metadata_columns_are_populated() {
        let schema = arrow_schema_for(vec![col("a", DataType::Int64)]);
        let records = vec![
            insert(vec![cv("a", Value::Int64(1))]),
            CdcRecord::Delete {
                table: TableRef::new(Some("public".to_string()), "t".to_string()),
                columns: vec![cv("a", Value::Int64(1))],
                position: SourcePosition::Lsn(99),
            },
        ];
        let out = records_to_batch("public.t", &schema, &records, 1_000).unwrap();
        let batch_schema = out.batch.schema();
        let names: Vec<&str> = batch_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let op_idx = names.iter().position(|n| *n == CDC_OP_COL).unwrap();
        let pos_idx = names.iter().position(|n| *n == CDC_POSITION_COL).unwrap();

        let ops = out
            .batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(ops.value(0), "I");
        assert_eq!(ops.value(1), "D");

        let positions = out
            .batch
            .column(pos_idx)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(positions.value(0), 42);
        assert_eq!(positions.value(1), 99);
    }

    #[test]
    fn uuid_date_time_bytea_conversions() {
        let schema = arrow_schema_for(vec![
            col("u", DataType::Uuid),
            col("d", DataType::Date),
            col("t", DataType::Time),
            col("b", DataType::Bytes),
        ]);
        let records = vec![insert(vec![
            cv(
                "u",
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".to_string()),
            ),
            cv("d", Value::String("2026-06-09".to_string())),
            cv("t", Value::String("10:30:00.5".to_string())),
            cv("b", Value::String("\\x4142".to_string())),
        ])];
        let out = records_to_batch("public.t", &schema, &records, 0).unwrap();
        assert_eq!(out.batch.num_rows(), 1);

        let bin = out
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(bin.value(0), b"AB");
    }

    #[test]
    fn decimal_strictness() {
        assert_eq!(decimal_str_to_i128("123.456", 10, 3).unwrap(), 123_456);
        assert_eq!(decimal_str_to_i128("-1.5", 10, 2).unwrap(), -150);
        assert_eq!(decimal_str_to_i128("7", 10, 2).unwrap(), 700);
        // trailing zeros beyond scale are fine
        assert_eq!(decimal_str_to_i128("1.2300", 10, 2).unwrap(), 123);
        // real precision loss is an error
        assert!(decimal_str_to_i128("1.234", 10, 2).is_err());
        // precision overflow is an error
        assert!(decimal_str_to_i128("1000", 3, 0).is_err());
    }

    #[test]
    fn date_helpers() {
        assert_eq!(parse_date_days("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date_days("1970-01-02").unwrap(), 1);
        assert_eq!(parse_date_days("1969-12-31").unwrap(), -1);
        // MySQL DATE path carries a zeroed time component
        assert_eq!(parse_date_days("1970-01-02 00:00:00").unwrap(), 1);
        assert!(parse_date_days("not-a-date").is_err());
    }

    #[test]
    fn time_helpers() {
        assert_eq!(parse_time_micros("00:00:01").unwrap(), 1_000_000);
        assert_eq!(parse_time_micros("0 00:00:01").unwrap(), 1_000_000);
        assert_eq!(parse_time_micros("1 00:00:00").unwrap(), 86_400_000_000);
        assert!(parse_time_micros("-1 05:00:00").is_err());
    }

    #[test]
    fn timestamp_helpers() {
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(
            parse_timestamp_micros("1970-01-01 00:00:00.000001").unwrap(),
            1
        );
        assert_eq!(
            parse_timestamp_micros("1970-01-01T00:00:01").unwrap(),
            1_000_000
        );
        // snapshot-path timestamptz with short/full offset suffix
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00+00").unwrap(), 0);
        assert_eq!(
            parse_timestamp_micros("1970-01-01 05:30:00.000001+05:30").unwrap(),
            1
        );
        assert!(parse_timestamp_micros("garbage").is_err());
    }
}
