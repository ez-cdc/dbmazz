# [Database Name] Sink Connector Template

This template shows the structure for a new sink connector. Replace all `[placeholders]` with your actual values.

## Overview

Brief description of the connector. For example:

> CDC sink connector for [Database Name](https://example.com), a [type of database: OLAP/OLTP/etc.].

## Features

List the key features:

- **Upserts**: [Supported/Not supported, explain mechanism]
- **Deletes**: [Soft deletes, hard deletes, or not supported]
- **Schema Evolution**: [Automatic column addition supported/not supported]
- **Transactions**: [Transactional writes or eventual consistency]
- **Loading Model**: [Streaming / Batch / Staged batch]

## Architecture

Show how the connector works:

```
CdcRecord --> [SinkName]Sink --> [Client] --> [Database Name]
                  |                |              |
                  v                v              v
              types.rs         client.rs     [Protocol]
              (mapping)        (protocol)    [Port/API]
```

## Configuration

The sink is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SINK_URL` | [Database] connection URL | required |
| `SINK_PORT` | [Port description] | `[default]` |
| `SINK_DATABASE` | Target database name | required |
| `SINK_USER` | Authentication username | `[default]` |
| `SINK_PASSWORD` | Authentication password | `""` |
| `SINK_[CUSTOM]` | [Custom parameter] | `[default]` |

### Example

```bash
export SINK_TYPE=[typename]
export SINK_URL=[protocol]://[host]:[port]
export SINK_PORT=[port]
export SINK_DATABASE=[database]
export SINK_USER=[user]
export SINK_PASSWORD=[password]
```

## CDC Audit Columns

Explain if your sink adds audit columns (recommended for tracking CDC operations):

The sink automatically adds audit columns to track CDC operations:

| Column | Type | Description |
|--------|------|-------------|
| `dbmazz_op_type` | [TYPE] | Operation type: 0=INSERT, 1=UPDATE, 2=DELETE |
| `dbmazz_is_deleted` | [TYPE] | Soft delete flag (true for DELETE operations) |
| `dbmazz_synced_at` | [TYPE] | Timestamp when record was synced |
| `dbmazz_cdc_version` | [TYPE] | Source position for ordering |

Or if not applicable:

> This sink does not add audit columns. CDC metadata is tracked [explain where/how].

## Loading Protocol

Explain how data is loaded into the sink:

### [Protocol Name] (e.g., Stream Load, COPY, Bulk Insert)

Explain the loading mechanism:

1. **[Step 1]**: [Description]
2. **[Step 2]**: [Description]
3. **[Step 3]**: [Description]

### Headers/Options

If using HTTP or similar:

```http
[METHOD] /[endpoint] HTTP/1.1
[Header1]: [value1]
[Header2]: [value2]
```

### Batching Strategy

Explain how records are batched:

- **Batch size**: [min-max records]
- **Flush interval**: [time]
- **Grouping**: [by table, by type, etc.]

## Type Mappings

Document how core DataTypes map to sink types:

| CDC DataType | [Database] Type |
|--------------|----------------|
| `Boolean` | `[TYPE]` |
| `Int16` | `[TYPE]` |
| `Int32` | `[TYPE]` |
| `Int64` | `[TYPE]` |
| `Float32` | `[TYPE]` |
| `Float64` | `[TYPE]` |
| `Decimal` | `[TYPE]` |
| `String/Text` | `[TYPE]` |
| `Json/Jsonb` | `[TYPE]` |
| `Uuid` | `[TYPE]` |
| `Date` | `[TYPE]` |
| `Time` | `[TYPE]` |
| `Timestamp` | `[TYPE]` |
| `Bytes` | `[TYPE]` |

## Table Requirements

Explain how target tables should be created:

```sql
CREATE TABLE [tablename] (
    [column1] [TYPE],
    [column2] [TYPE],
    -- CDC audit columns (if applicable)
    dbmazz_op_type [TYPE] COMMENT '[description]',
    dbmazz_is_deleted [TYPE] COMMENT '[description]',
    dbmazz_synced_at [TYPE] COMMENT '[description]',
    dbmazz_cdc_version [TYPE] COMMENT '[description]'
)
[TABLE_PROPERTIES];
```

Explain any special requirements:

- Primary keys: [required/optional, how they're used]
- Indexes: [recommendations]
- Partitioning: [considerations]
- Other: [database-specific requirements]

## Error Handling

The sink includes retry logic:

- **Max retries**: [number]
- **Backoff strategy**: [exponential, linear, etc.]
- **Retriable errors**: [which errors are retried]
- **Non-retriable errors**: [which errors fail immediately]

### Common Errors

1. **"[error message]"**
   - Cause: [explanation]
   - Solution: [how to fix]

2. **"[error message]"**
   - Cause: [explanation]
   - Solution: [how to fix]

## Module Structure

```
[typename]/
├── mod.rs           # Main sink implementation (Sink trait)
├── config.rs        # Configuration parsing and validation
├── client.rs        # Client/connection logic
├── setup.rs         # Schema setup and DDL operations
├── types.rs         # Type mappings and conversions
└── README.md        # This file
```

## Usage

### Basic Usage

```rust
use dbmazz::connectors::sinks::create_sink;
use dbmazz::config::SinkConfig;

// Create sink from config
let sink = create_sink(&config)?;

// Validate connection
sink.validate_connection().await?;

// Write batch of CDC records
let result = sink.write_batch(records).await?;
println!("Wrote {} records, {} bytes",
    result.records_written,
    result.bytes_written
);

// Close when done
sink.close().await?;
```

### Advanced Usage

If there are advanced features:

```rust
// Example: Custom batching, partial updates, etc.
```

## Performance Tuning

Provide tuning recommendations:

- **Batch size**: [recommendation and why]
- **Flush interval**: [recommendation and why]
- **Concurrency**: [if applicable]
- **[Database-specific tuning]**: [explanation]

## Capabilities

Explain the capabilities returned by the sink:

```rust
SinkCapabilities {
    supports_upsert: [true/false],
    supports_delete: [true/false],
    supports_schema_evolution: [true/false],
    supports_transactions: [true/false],
    loading_model: LoadingModel::[Streaming/StagedBatch/MessageQueue],
    min_batch_size: Some([number]),
    max_batch_size: Some([number]),
    optimal_flush_interval_ms: [milliseconds],
}
```

Explain what each means for this sink.

## Schema Evolution

Explain how schema changes are handled:

### Automatic Column Addition

- Supported: [yes/no]
- Mechanism: [how it works]
- Limitations: [what's not supported]

### Manual Schema Management

If automatic evolution isn't supported:

```sql
-- Example: How to manually add columns
ALTER TABLE [tablename] ADD COLUMN [columnname] [TYPE];
```

## Limitations

List any known limitations:

- [Limitation 1]: [explanation]
- [Limitation 2]: [explanation]
- Not supported: [features that aren't supported]
- Known issues: [any known bugs or workarounds]

## References

- [Database Name Documentation](https://example.com/docs)
- [Loading API Documentation](https://example.com/docs/loading)
- [Data Types Documentation](https://example.com/docs/types)

## Implementation Notes

This section is for developers implementing the connector:

### Files to Create

1. **mod.rs**: Implement `Sink` trait
   - `new()` - Create instance from config
   - `name()` - Return sink name
   - `capabilities()` - Return sink capabilities
   - `validate_connection()` - Test connection
   - `write_batch()` - Write CdcRecords
   - `close()` - Cleanup

2. **config.rs**: Configuration
   - Parse SinkConfig
   - Validate parameters
   - Build connection strings

3. **client.rs**: Client implementation
   - Connection management
   - Protocol implementation
   - Request/response handling
   - Retry logic

4. **setup.rs**: Schema operations
   - Create tables
   - Add audit columns
   - Schema evolution
   - DDL execution

5. **types.rs**: Type mappings
   - `data_type_to_sql()` - Map DataType to SQL
   - `value_to_[format]()` - Convert values
   - Handle special cases (NULL, Unchanged, etc.)

### CdcRecord Handling

Your sink must handle these record types:

```rust
match record {
    CdcRecord::Insert { table, columns, position } => {
        // Create INSERT or UPSERT statement
    }
    CdcRecord::Update { table, old_columns, new_columns, position } => {
        // Create UPDATE or UPSERT statement
        // Handle TOAST/unchanged columns if needed
    }
    CdcRecord::Delete { table, columns, position } => {
        // Create DELETE or soft-delete UPDATE
    }
    CdcRecord::SchemaChange { table, columns, position } => {
        // Optionally handle schema evolution
    }
    CdcRecord::Begin { xid } => {
        // Start transaction if supported
    }
    CdcRecord::Commit { xid, position } => {
        // Commit transaction if supported
    }
    CdcRecord::Heartbeat { position } => {
        // Update position tracking, no data write
    }
}
```

### Testing Checklist

- [ ] Unit tests for type mappings
- [ ] Unit tests for value conversions
- [ ] Connection validation tests
- [ ] Integration test with real database
- [ ] Test INSERT operations
- [ ] Test UPDATE operations (including TOAST/partial)
- [ ] Test DELETE operations (soft delete)
- [ ] Test schema changes (if supported)
- [ ] Test error handling and retries
- [ ] Test batch size limits
- [ ] Performance benchmarks
- [ ] Test capabilities() accuracy

### Configuration Integration

Add to `src/config.rs`:

```rust
// Add to SinkType enum
pub enum SinkType {
    StarRocks,
    [YourSink],
}

// Add config struct
#[derive(Debug, Clone)]
pub struct [YourSink]SinkConfig {
    // Sink-specific fields
}

// Add to SinkConfig
pub struct SinkConfig {
    // ... existing fields
    pub [yoursink]: Option<[YourSink]SinkConfig>,
}
```

### Factory Registration

Add to `src/connectors/sinks/mod.rs`:

```rust
pub mod [yoursink];

use self::[yoursink]::[YourSink]Sink;

pub fn create_sink(config: &SinkConfig) -> Result<Box<dyn Sink>> {
    match config.sink_type {
        // ... existing cases
        SinkType::[YourSink] => {
            let sink = [YourSink]Sink::new(config)?;
            Ok(Box::new(sink))
        }
    }
}
```
