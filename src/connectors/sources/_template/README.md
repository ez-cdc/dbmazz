# [Database Name] CDC Source Connector Template

This template shows the structure for a new source connector. Replace all `[placeholders]` with your actual values.

## Overview

Brief description of the connector. For example:

> This connector implements Change Data Capture (CDC) for [Database Name] using [CDC Method: logical replication / binlog / change streams / etc.].

## Features

List the key features:

- CDC method: [e.g., logical replication, binlog streaming, change streams]
- Supported operations: [INSERT, UPDATE, DELETE, DDL]
- [Any special features like transaction support, schema evolution, etc.]
- Checkpoint mechanism: [e.g., LSN, binlog position, timestamp]
- Normalized output using core `CdcRecord` format

## Prerequisites

### Database Configuration

List required database configuration. For example:

1. **Enable CDC** in database configuration:

```conf
[configuration parameter] = [value]
```

2. **Restart [Database]** after changing settings.

3. **Create a replication user** (or similar):

```sql
CREATE USER cdc_user WITH [permissions];
GRANT [required permissions] ON [resources] TO cdc_user;
```

### Table Configuration

Document any table-level requirements:

```sql
-- Example: Set replica identity, enable CDC, etc.
ALTER TABLE [table_name] [configuration];
```

## Architecture

Show how the connector works:

```
[Database Name] [Change Log/Stream]
      |
      v
[Connection Protocol]
      |
      v (protocol messages)
[Parser] --> [InternalMessage]
      |
      v (normalized records)
[MySource] --> [CdcRecord] --> [Channel] --> [Pipeline]
      ^
      |
[Checkpoint Feedback]
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOURCE_URL` | Yes | - | Connection URL |
| `SOURCE_TYPE` | No | `[type]` | Source type identifier |
| `SOURCE_[PARAM]` | No | `[default]` | [Description] |
| `TABLES` | Yes | - | Comma-separated list of tables |

### Example

```bash
export SOURCE_URL="[protocol]://[host]:[port]/[database]"
export SOURCE_TYPE="[typename]"
export SOURCE_[PARAM]="[value]"
export TABLES="table1,table2"
```

## Module Structure

```
[typename]/
├── mod.rs          # Main implementation (Source trait)
├── config.rs       # Configuration validation
├── parser.rs       # Protocol/message parsing
├── connection.rs   # Connection and streaming logic
├── setup.rs        # Initial setup (create streams, etc.)
├── types.rs        # Type mapping to core::DataType
└── README.md       # This file
```

## Usage

### Programmatic

```rust
use dbmazz::connectors::sources::create_source;
use dbmazz::config::SourceConfig;
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(10000);

let source = create_source(&config.source, tx).await?;
source.validate().await?;
source.start().await?;

// Process records
while let Some(record) = rx.recv().await {
    match record {
        CdcRecord::Insert { table, columns, position } => {
            // Handle insert
        }
        CdcRecord::Update { table, old_columns, new_columns, position } => {
            // Handle update
        }
        CdcRecord::Delete { table, columns, position } => {
            // Handle delete
        }
        _ => {}
    }
}
```

### Direct Source Construction

```rust
use dbmazz::connectors::sources::[typename]::[TypeName]Source;

let mut source = [TypeName]Source::new(
    "[connection_url]",
    vec!["table1".to_string()],
    tx,
).await?;

// Start from a specific position (for resuming)
source.start_from_position(last_checkpoint).await?;
```

## Type Mappings

Document how database types map to core DataTypes:

| [Database] Type | Core DataType |
|-----------------|---------------|
| `[type1]` | `Boolean` |
| `[type2]` | `Int32` |
| `[type3]` | `Int64` |
| `[type4]` | `Float64` |
| `[type5]` | `String` |
| `[type6]` | `Timestamp` |
| ... | ... |

## Checkpointing

Explain how checkpointing works:

The connector uses [checkpoint mechanism] for position tracking:

1. Each `CdcRecord` includes a `SourcePosition::[Type]([value])`
2. After successfully processing a batch, confirm the position
3. On restart, resume from the last confirmed position
4. The connector [how it sends acknowledgment]

```rust
// Example code for checkpointing
source.confirm_position(batch_end_position);
```

## Error Handling

### Common Errors

Document common errors and solutions:

1. **"[error message]"**
   - Cause: [explanation]
   - Solution: [how to fix]

2. **"[error message]"**
   - Cause: [explanation]
   - Solution: [how to fix]

## Performance Considerations

1. **Buffer sizes**: [recommendations]
2. **Checkpoint frequency**: [trade-offs]
3. **[Database-specific consideration]**: [explanation]
4. **[Performance tip]**: [explanation]

## Cleanup

Explain how to clean up resources:

```sql
-- Example cleanup queries
DROP [resource];
```

## Limitations

List any known limitations:

- [Limitation 1]
- [Limitation 2]
- Not supported: [features]

## References

- [Database Name CDC Documentation](https://example.com/docs)
- [Replication Protocol Documentation](https://example.com/docs)
- [Change Stream/Binlog Documentation](https://example.com/docs)

## Implementation Notes

This section is for developers implementing the connector:

### Files to Create

1. **mod.rs**: Implement `Source` trait
   - `new()` - Create instance
   - `validate()` - Check connection and config
   - `start()` - Begin streaming
   - `stop()` - Graceful shutdown
   - `current_position()` - Return checkpoint position

2. **config.rs**: Configuration validation
   - Parse connection strings
   - Validate parameters
   - Handle position encoding/decoding

3. **parser.rs**: Protocol message parsing
   - Parse [CDC protocol] messages
   - Convert to internal representation

4. **connection.rs**: Connection management
   - Establish connection
   - Handle reconnection
   - Stream CDC events

5. **setup.rs**: Initial setup
   - Create [streams/slots/etc.]
   - Validate table configuration
   - Cleanup on shutdown

6. **types.rs**: Type mappings
   - `[db]_type_to_data_type()` - Map types
   - `values_to_column_values()` - Convert values
   - `columns_to_defs()` - Convert schemas

### Testing Checklist

- [ ] Unit tests for type mappings
- [ ] Unit tests for parser
- [ ] Connection validation tests
- [ ] Integration test with real database
- [ ] Test all CDC operations (INSERT, UPDATE, DELETE)
- [ ] Test schema changes
- [ ] Test checkpoint/resume
- [ ] Test error handling
- [ ] Performance benchmarks
