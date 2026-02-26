# Contributing Connectors to dbmazz

This guide explains how to add new source and sink connectors to dbmazz. The connector architecture is designed to be pluggable, making it straightforward to add support for new databases and systems.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Adding a New Source Connector](#adding-a-new-source-connector)
- [Adding a New Sink Connector](#adding-a-new-sink-connector)
- [Checklist for New Connectors](#checklist-for-new-connectors)
- [Testing Requirements](#testing-requirements)
- [Documentation Requirements](#documentation-requirements)

## Overview

dbmazz uses a trait-based architecture for connectors:

- **Source Connectors**: Implement the `Source` trait from `crate::core::traits`
- **Sink Connectors**: Implement the `Sink` trait from `crate::core::traits`
- **Normalized Data**: All connectors exchange data using `CdcRecord` from `crate::core::record`
- **Factory Pattern**: Connectors are instantiated via factory functions that handle configuration

The connector system provides complete isolation between:
- Protocol-specific logic (how to read/write)
- Type mappings (database-specific types to generic DataType)
- Configuration and validation
- Connection management

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Source Connectors                            │
│  (Postgres, MySQL, Oracle, etc.)                                │
│                                                                  │
│  Each implements: Source trait                                  │
│  Each emits: CdcRecord                                          │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ CdcRecord (normalized format)
                 │
                 ▼
        ┌─────────────────┐
        │   Pipeline      │
        │                 │
        │ - Batching      │
        │ - Schema cache  │
        │ - Checkpointing │
        └─────────┬───────┘
                  │
                  │ CdcRecord batches
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Sink Connectors                              │
│  (StarRocks, ClickHouse, Snowflake, etc.)                       │
│                                                                  │
│  Each implements: Sink trait                                    │
│  Each accepts: Vec<CdcRecord>                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Adding a New Source Connector

### Step 1: Create the Directory Structure

Create a new directory under `src/connectors/sources/`:

```bash
cd src/connectors/sources/
mkdir mynewsource
cd mynewsource
```

### Step 2: Create Required Files

Create the following files (use the PostgreSQL source as a reference):

```
mynewsource/
├── mod.rs           # Main implementation (Source trait)
├── config.rs        # Configuration and validation
├── parser.rs        # Protocol/message parsing logic (if applicable)
├── replication.rs   # Connection and streaming logic (if applicable)
├── setup.rs         # Initial setup (create slots, streams, etc.)
├── types.rs         # Type mapping from source DB to core::DataType
└── README.md        # Connector documentation
```

### Step 3: Implement the Source Trait

In `mod.rs`, create your source struct and implement the `Source` trait:

```rust
// mynewsource/mod.rs
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::core::{CdcRecord, Source, SourcePosition};

mod config;
mod parser;
mod setup;
mod types;

pub use config::MyNewSourceConfig;

/// My New Source connector for CDC
pub struct MyNewSource {
    // Connection details
    connection_url: String,

    // Tables to replicate
    tables: Vec<String>,

    // Channel for emitting CdcRecord events
    record_tx: mpsc::Sender<CdcRecord>,

    // Current position for checkpointing
    current_position: Option<SourcePosition>,

    // Add other fields as needed (connections, state, etc.)
}

impl MyNewSource {
    /// Create a new instance
    pub async fn new(
        connection_url: &str,
        tables: Vec<String>,
        record_tx: mpsc::Sender<CdcRecord>,
    ) -> Result<Self> {
        Ok(Self {
            connection_url: connection_url.to_string(),
            tables,
            record_tx,
            current_position: None,
        })
    }

    /// Internal helper to emit CdcRecord to the channel
    async fn emit_record(&self, record: CdcRecord) -> Result<()> {
        self.record_tx
            .send(record)
            .await
            .context("Failed to send CdcRecord")?;
        Ok(())
    }
}

#[async_trait]
impl Source for MyNewSource {
    fn name(&self) -> &'static str {
        "mynewsource"
    }

    async fn validate(&self) -> Result<()> {
        // Validate connection and configuration
        // Check version, permissions, table existence, etc.
        todo!("Implement validation")
    }

    async fn start(&mut self) -> Result<()> {
        // Start the CDC stream
        // This typically involves:
        // 1. Connecting to the source
        // 2. Setting up replication/change stream
        // 3. Starting to emit CdcRecord events to record_tx
        todo!("Implement start")
    }

    async fn stop(&mut self) -> Result<()> {
        // Gracefully stop the source
        // Clean up connections, streams, etc.
        todo!("Implement stop")
    }

    fn current_position(&self) -> Option<SourcePosition> {
        // Return the current position for checkpointing
        self.current_position.clone()
    }
}
```

### Step 4: Implement Type Mappings

In `types.rs`, map source-specific types to `core::DataType`:

```rust
// mynewsource/types.rs
use crate::core::{DataType, ColumnDef, ColumnValue, Value};

/// Maps source type IDs to generic DataType
pub fn source_type_to_data_type(type_id: u32) -> DataType {
    match type_id {
        1 => DataType::Boolean,
        2 => DataType::Int32,
        3 => DataType::Int64,
        4 => DataType::Float64,
        5 => DataType::String,
        // ... add all type mappings
        _ => DataType::String, // Fallback
    }
}

/// Convert source-specific column definitions to ColumnDef
pub fn columns_to_defs(source_columns: &[SourceColumn]) -> Vec<ColumnDef> {
    source_columns
        .iter()
        .map(|col| ColumnDef::new(
            col.name.clone(),
            source_type_to_data_type(col.type_id),
            col.nullable,
        ))
        .collect()
}

/// Convert source-specific values to ColumnValue
pub fn values_to_column_values(
    values: &[SourceValue],
    columns: &[SourceColumn],
) -> Vec<ColumnValue> {
    values
        .iter()
        .zip(columns.iter())
        .map(|(val, col)| {
            let value = match val {
                SourceValue::Null => Value::Null,
                SourceValue::Bool(b) => Value::Bool(*b),
                SourceValue::Int(i) => Value::Int64(*i),
                SourceValue::String(s) => Value::String(s.clone()),
                // ... handle all value types
            };
            ColumnValue::new(col.name.clone(), value)
        })
        .collect()
}
```

### Step 5: Update Configuration

In `src/config.rs`, add your source type to the enum:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceType {
    Postgres,
    MyNewSource,  // Add this
}

impl SourceType {
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" => Ok(SourceType::Postgres),
            "mynewsource" => Ok(SourceType::MyNewSource),  // Add this
            other => anyhow::bail!("Unsupported source type: '{}'", other),
        }
    }
}
```

Add source-specific config:

```rust
/// MyNewSource-specific configuration
#[derive(Debug, Clone)]
pub struct MyNewSourceConfig {
    pub stream_name: String,
    // Add other source-specific config
}

/// Add to SourceConfig
#[derive(Debug, Clone)]
pub struct SourceConfig {
    pub source_type: SourceType,
    pub url: String,
    pub tables: Vec<String>,
    pub postgres: Option<PostgresSourceConfig>,
    pub mynewsource: Option<MyNewSourceConfig>,  // Add this
}
```

### Step 6: Register in Factory

In `src/connectors/sources/mod.rs`, add your source to the factory:

```rust
pub mod postgres;
pub mod mynewsource;  // Add this

pub use postgres::PostgresSource;
pub use mynewsource::MyNewSource;  // Add this

pub async fn create_source(
    config: &SourceConfig,
    record_tx: mpsc::Sender<CdcRecord>,
) -> Result<Box<dyn Source>> {
    match config.source_type {
        SourceType::Postgres => {
            // ... existing postgres code
        }

        SourceType::MyNewSource => {  // Add this
            let my_config = config
                .mynewsource
                .as_ref()
                .ok_or_else(|| anyhow!("MyNewSource config required"))?;

            let source = MyNewSource::new(
                &config.url,
                config.tables.clone(),
                record_tx,
            ).await?;

            Ok(Box::new(source))
        }
    }
}
```

### Step 7: Emit CdcRecord Events

In your source implementation, emit normalized `CdcRecord` events:

```rust
// Example: Emitting different record types

// Schema change
let record = CdcRecord::SchemaChange {
    table: TableRef::new(Some("public".to_string()), "orders".to_string()),
    columns: column_defs,
    position: SourcePosition::Offset(12345),
};
self.emit_record(record).await?;

// Begin transaction
let record = CdcRecord::Begin { xid: 98765 };
self.emit_record(record).await?;

// Insert
let record = CdcRecord::Insert {
    table: TableRef::new(Some("public".to_string()), "orders".to_string()),
    columns: vec![
        ColumnValue::new("id".to_string(), Value::Int64(123)),
        ColumnValue::new("amount".to_string(), Value::Float64(99.99)),
    ],
    position: SourcePosition::Offset(12346),
};
self.emit_record(record).await?;

// Update
let record = CdcRecord::Update {
    table: TableRef::new(Some("public".to_string()), "orders".to_string()),
    old_columns: Some(old_values),  // Optional, for sinks that need it
    new_columns: new_values,
    position: SourcePosition::Offset(12347),
};
self.emit_record(record).await?;

// Delete
let record = CdcRecord::Delete {
    table: TableRef::new(Some("public".to_string()), "orders".to_string()),
    columns: old_values,  // Values before deletion
    position: SourcePosition::Offset(12348),
};
self.emit_record(record).await?;

// Commit
let record = CdcRecord::Commit {
    xid: 98765,
    position: SourcePosition::Offset(12349),
};
self.emit_record(record).await?;

// Heartbeat (for lag tracking)
let record = CdcRecord::Heartbeat {
    position: SourcePosition::Offset(12350),
};
self.emit_record(record).await?;
```

## Adding a New Sink Connector

### Step 1: Create the Directory Structure

Create a new directory under `src/connectors/sinks/`:

```bash
cd src/connectors/sinks/
mkdir mynewsink
cd mynewsink
```

### Step 2: Create Required Files

```
mynewsink/
├── mod.rs           # Main implementation (Sink trait)
├── config.rs        # Configuration and validation
├── client.rs        # Client/connection logic (HTTP, gRPC, etc.)
├── setup.rs         # Schema setup and DDL operations
├── types.rs         # Type mapping from core::DataType to sink DB
└── README.md        # Connector documentation
```

### Step 3: Implement the Sink Trait

In `mod.rs`, create your sink struct and implement the `Sink` trait:

```rust
// mynewsink/mod.rs
use anyhow::Result;
use async_trait::async_trait;

use crate::config::SinkConfig;
use crate::core::{
    CdcRecord, ColumnValue, LoadingModel, Sink, SinkCapabilities, SinkResult
};

mod config;
mod client;
mod setup;
mod types;

pub use config::MyNewSinkConfig;
use client::MyNewSinkClient;
use types::TypeMapper;

/// My New Sink connector
pub struct MyNewSink {
    config: MyNewSinkConfig,
    client: MyNewSinkClient,
    type_mapper: TypeMapper,
}

impl MyNewSink {
    /// Create a new sink instance
    pub fn new(config: &SinkConfig) -> Result<Self> {
        let sink_config = MyNewSinkConfig::from_sink_config(config)?;
        let client = MyNewSinkClient::new(
            sink_config.url.clone(),
            sink_config.database.clone(),
            sink_config.user.clone(),
            sink_config.password.clone(),
        );

        Ok(Self {
            config: sink_config,
            client,
            type_mapper: TypeMapper::new(),
        })
    }

    /// Convert CdcRecord to sink-specific format
    fn records_to_batch(&self, records: &[CdcRecord]) -> Result<Vec<SinkRow>> {
        let mut rows = Vec::new();

        for record in records {
            match record {
                CdcRecord::Insert { table, columns, position } => {
                    let row = self.create_insert_row(table, columns, position)?;
                    rows.push(row);
                }
                CdcRecord::Update { table, new_columns, position, .. } => {
                    let row = self.create_update_row(table, new_columns, position)?;
                    rows.push(row);
                }
                CdcRecord::Delete { table, columns, position } => {
                    let row = self.create_delete_row(table, columns, position)?;
                    rows.push(row);
                }
                // Handle or ignore other record types
                _ => {}
            }
        }

        Ok(rows)
    }
}

#[async_trait]
impl Sink for MyNewSink {
    fn name(&self) -> &'static str {
        "mynewsink"
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            supports_upsert: true,
            supports_delete: true,
            supports_schema_evolution: true,
            supports_transactions: false,
            loading_model: LoadingModel::Streaming,
            min_batch_size: Some(1),
            max_batch_size: Some(100_000),
            optimal_flush_interval_ms: 5000,
        }
    }

    async fn validate_connection(&self) -> Result<()> {
        // Validate connection to sink
        self.client.test_connection().await
    }

    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
        if records.is_empty() {
            return Ok(SinkResult {
                records_written: 0,
                bytes_written: 0,
                last_position: None,
            });
        }

        // Extract last position
        let last_position = records.iter().rev().find_map(|r| match r {
            CdcRecord::Insert { position, .. }
            | CdcRecord::Update { position, .. }
            | CdcRecord::Delete { position, .. }
            | CdcRecord::Commit { position, .. }
            | CdcRecord::Heartbeat { position, .. } => Some(position.clone()),
            _ => None,
        });

        // Convert records to sink format
        let rows = self.records_to_batch(&records)?;

        // Write to sink
        let result = self.client.write_rows(rows).await?;

        Ok(SinkResult {
            records_written: result.rows_written,
            bytes_written: result.bytes_written,
            last_position,
        })
    }

    async fn close(&mut self) -> Result<()> {
        // Cleanup and close connections
        self.client.close().await
    }
}
```

### Step 4: Implement Type Mappings

In `types.rs`, map `core::DataType` to sink-specific types:

```rust
// mynewsink/types.rs
use crate::core::{DataType, Value};

pub struct TypeMapper;

impl TypeMapper {
    pub fn new() -> Self {
        Self
    }

    /// Map core DataType to sink SQL type
    pub fn data_type_to_sql(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int16 => "SMALLINT".to_string(),
            DataType::Int32 => "INTEGER".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::Float32 => "REAL".to_string(),
            DataType::Float64 => "DOUBLE".to_string(),
            DataType::Decimal { precision, scale } => {
                format!("DECIMAL({},{})", precision, scale)
            }
            DataType::String | DataType::Text => "TEXT".to_string(),
            DataType::Bytes => "BYTEA".to_string(),
            DataType::Json | DataType::Jsonb => "JSON".to_string(),
            DataType::Uuid => "UUID".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::TimestampTz => "TIMESTAMPTZ".to_string(),
        }
    }

    /// Convert core Value to sink-specific representation
    pub fn value_to_sink_format(&self, value: &Value) -> SinkValue {
        match value {
            Value::Null => SinkValue::Null,
            Value::Bool(b) => SinkValue::Bool(*b),
            Value::Int64(i) => SinkValue::Integer(*i),
            Value::Float64(f) => SinkValue::Float(*f),
            Value::String(s) => SinkValue::String(s.clone()),
            Value::Bytes(b) => SinkValue::Bytes(b.clone()),
            Value::Json(j) => SinkValue::Json(j.clone()),
            Value::Timestamp(t) => SinkValue::Timestamp(*t),
            Value::Decimal(d) => SinkValue::Decimal(d.clone()),
            Value::Uuid(u) => SinkValue::Uuid(u.clone()),
            Value::Unchanged => SinkValue::Null,  // Or handle specially
        }
    }
}
```

### Step 5: Update Configuration

In `src/config.rs`, add your sink type:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkType {
    StarRocks,
    MyNewSink,  // Add this
}

impl SinkType {
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "starrocks" => Ok(SinkType::StarRocks),
            "mynewsink" => Ok(SinkType::MyNewSink),  // Add this
            other => anyhow::bail!("Unsupported sink type: '{}'", other),
        }
    }
}
```

Add sink-specific config:

```rust
#[derive(Debug, Clone)]
pub struct MyNewSinkConfig {
    // Sink-specific settings
}

#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub sink_type: SinkType,
    pub url: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub starrocks: Option<StarRocksSinkConfig>,
    pub mynewsink: Option<MyNewSinkConfig>,  // Add this
}
```

### Step 6: Register in Factory

In `src/connectors/sinks/mod.rs`:

```rust
pub mod starrocks;
pub mod mynewsink;  // Add this

use self::mynewsink::MyNewSink;  // Add this

pub fn create_sink(config: &SinkConfig) -> Result<Box<dyn Sink>> {
    match config.sink_type {
        SinkType::StarRocks => {
            let sink = StarRocksSink::new(config)?;
            Ok(Box::new(sink))
        }
        SinkType::MyNewSink => {  // Add this
            let sink = MyNewSink::new(config)?;
            Ok(Box::new(sink))
        }
    }
}
```

## Checklist for New Connectors

### Source Connector Checklist

- [ ] Created directory under `src/connectors/sources/`
- [ ] Implemented `Source` trait in `mod.rs`
- [ ] Created `config.rs` with connection validation
- [ ] Implemented type mappings in `types.rs`
- [ ] Added source to `SourceType` enum in `src/config.rs`
- [ ] Registered in factory function (`src/connectors/sources/mod.rs`)
- [ ] Emits all necessary `CdcRecord` variants
- [ ] Implements checkpointing via `current_position()`
- [ ] Handles graceful shutdown in `stop()`
- [ ] Added unit tests
- [ ] Added integration tests (if applicable)
- [ ] Created comprehensive `README.md`

### Sink Connector Checklist

- [ ] Created directory under `src/connectors/sinks/`
- [ ] Implemented `Sink` trait in `mod.rs`
- [ ] Created `config.rs` with connection validation
- [ ] Implemented type mappings in `types.rs`
- [ ] Implemented correct `capabilities()`
- [ ] Added sink to `SinkType` enum in `src/config.rs`
- [ ] Registered in factory function (`src/connectors/sinks/mod.rs`)
- [ ] Handles all `CdcRecord` variants appropriately
- [ ] Returns correct `SinkResult` with position tracking
- [ ] Implements retry logic for transient failures
- [ ] Added unit tests
- [ ] Added integration tests (if applicable)
- [ ] Created comprehensive `README.md`

## Testing Requirements

### Unit Tests

Every connector must include unit tests in its `mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_mappings() {
        // Test type conversions
    }

    #[tokio::test]
    async fn test_connection_validation() {
        // Test connection validation
    }

    #[tokio::test]
    async fn test_record_processing() {
        // Test CDC record handling
    }
}
```

### Integration Tests

Create integration tests in `tests/integration_tests.rs` (or similar):

```rust
#[cfg(feature = "integration-tests")]
mod integration {
    use dbmazz::connectors::sources::create_source;

    #[tokio::test]
    async fn test_mynewsource_end_to_end() {
        // Test with real database connection
    }
}
```

Run with: `cargo test --features integration-tests`

### Manual Testing

1. Test with the demo environment (adapt `examples/` if needed)
2. Verify all CDC operations: INSERT, UPDATE, DELETE
3. Test schema evolution
4. Test error handling and recovery
5. Measure performance characteristics

## Documentation Requirements

Each connector must include a comprehensive `README.md` with:

### Required Sections

1. **Overview**: Brief description of the connector
2. **Features**: Key capabilities (e.g., transaction support, CDC mode)
3. **Prerequisites**: Database version, configuration requirements
4. **Configuration**: Environment variables and examples
5. **Architecture**: How the connector works internally
6. **Type Mappings**: Complete table of type conversions
7. **Usage**: Code examples
8. **Error Handling**: Common errors and solutions
9. **Performance**: Tuning recommendations
10. **Limitations**: Known issues or unsupported features
11. **References**: Links to database documentation

### Example README Template

See `src/connectors/sources/_template/README.md` and `src/connectors/sinks/_template/README.md` for complete templates.

### Main README Updates

Update `/Users/dariomazzitelli/repos/db_mazz_project/dbmazz/README.md` to list the new connector:

- Add to "Supported Sources" or "Supported Sinks"
- Update the architecture diagram if needed
- Link to the connector's README

## Examples to Follow

The best examples to reference:

- **Source**: `src/connectors/sources/postgres/` - Complete PostgreSQL CDC implementation
- **Sink**: `src/connectors/sinks/starrocks/` - Complete StarRocks sink implementation

Both include all required files, comprehensive error handling, and complete documentation.

## Getting Help

- Review existing connectors for patterns and best practices
- Check `src/core/traits.rs` for trait definitions
- Review `src/core/record.rs` for `CdcRecord` structure
- Open a GitHub issue for design questions

## License

All contributed connectors must be licensed under the Elastic License v2.0, consistent with the main project.
