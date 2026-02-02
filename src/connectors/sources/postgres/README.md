# PostgreSQL CDC Source Connector

This connector implements Change Data Capture (CDC) for PostgreSQL using logical replication with the `pgoutput` plugin.

## Features

- Logical replication using native `pgoutput` plugin (PostgreSQL 10+)
- Zero-copy parsing with SIMD-optimized operations
- Automatic publication and replication slot management
- Support for REPLICA IDENTITY FULL (required for proper DELETE handling)
- LSN-based checkpointing for exactly-once delivery
- Normalized output using core `CdcRecord` format

## Prerequisites

### PostgreSQL Configuration

1. **Enable logical replication** in `postgresql.conf`:

```conf
wal_level = logical
max_replication_slots = 10  # adjust based on needs
max_wal_senders = 10        # adjust based on needs
```

2. **Restart PostgreSQL** after changing these settings.

3. **Create a replication user** (or use an existing superuser):

```sql
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'your_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

### Table Configuration

For proper CDC support (especially DELETE operations), tables should have `REPLICA IDENTITY FULL`:

```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
```

**Note:** With `REPLICA IDENTITY DEFAULT`, DELETE operations only include primary key columns. This is insufficient for sinks that need all columns (e.g., for partitioned tables in StarRocks/ClickHouse).

## Architecture

```
PostgreSQL WAL
      |
      v
[Replication Protocol]
      |
      v (XLogData messages)
[pgoutput Parser] --> [CdcMessage]
      |
      v (normalized records)
[PostgresSource] --> [CdcRecord] --> [Channel] --> [Pipeline]
      ^
      |
[Standby Status] <-- [Checkpoint Feedback]
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOURCE_URL` | Yes | - | PostgreSQL connection URL |
| `SOURCE_TYPE` | No | `postgres` | Source type identifier |
| `SOURCE_SLOT_NAME` | No | `dbmazz_slot` | Replication slot name |
| `SOURCE_PUBLICATION_NAME` | No | `dbmazz_pub` | Publication name |
| `TABLES` | Yes | - | Comma-separated list of tables |

### Example

```bash
export SOURCE_URL="postgres://cdc_user:password@localhost:5432/mydb"
export SOURCE_SLOT_NAME="myapp_slot"
export SOURCE_PUBLICATION_NAME="myapp_pub"
export TABLES="public.orders,public.order_items"
```

## Module Structure

```
postgres/
├── mod.rs          # PostgresSource implementation (Source trait)
├── config.rs       # Configuration validation and LSN utilities
├── parser.rs       # pgoutput protocol parser (CdcMessage)
├── replication.rs  # WAL streaming protocol (WalMessage)
├── setup.rs        # Publication/slot management
├── types.rs        # PostgreSQL OID -> DataType mapping
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

### Direct PostgresSource

```rust
use dbmazz::connectors::sources::postgres::PostgresSource;

let mut source = PostgresSource::new(
    "postgres://localhost/mydb",
    "my_slot".to_string(),
    "my_pub".to_string(),
    vec!["orders".to_string()],
    tx,
).await?;

// Start from a specific LSN
let stream = source.start_replication_from(last_checkpoint_lsn).await?;
```

## Type Mappings

| PostgreSQL Type | Core DataType |
|-----------------|---------------|
| `boolean` | `Boolean` |
| `smallint` | `Int16` |
| `integer` | `Int32` |
| `bigint` | `Int64` |
| `real` | `Float32` |
| `double precision` | `Float64` |
| `numeric(p,s)` | `Decimal{p,s}` |
| `text`, `varchar`, `char` | `String` |
| `bytea` | `Bytes` |
| `json` | `Json` |
| `jsonb` | `Jsonb` |
| `uuid` | `Uuid` |
| `date` | `Date` |
| `time` | `Time` |
| `timestamp` | `Timestamp` |
| `timestamptz` | `TimestampTz` |

## Checkpointing

The connector uses LSN (Log Sequence Number) for checkpointing:

1. Each `CdcRecord` includes a `SourcePosition::Lsn(u64)`
2. After successfully processing a batch, confirm the LSN
3. On restart, resume from the last confirmed LSN
4. The connector sends Standby Status Update to PostgreSQL

```rust
// After batch is written to sink
source.confirm_lsn(batch_end_lsn);

// Build and send status update
let status = build_standby_status_update(confirmed_lsn);
stream.send(status).await?;
```

## Error Handling

### Common Errors

1. **"replication slot does not exist"**
   - The slot was dropped externally
   - Solution: Restart the connector to recreate the slot

2. **"replication slot is active"**
   - Another process is using the slot
   - Solution: Stop the other process or use a different slot name

3. **"publication does not exist"**
   - The publication was dropped
   - Solution: Restart to recreate or create manually

4. **"REPLICA IDENTITY NOTHING"**
   - Table doesn't support CDC properly
   - Solution: `ALTER TABLE x REPLICA IDENTITY FULL;`

## Performance Considerations

1. **Buffer sizes**: Adjust channel capacity based on memory and throughput needs
2. **Checkpoint frequency**: More frequent = less reprocessing, more overhead
3. **WAL retention**: PostgreSQL retains WAL until slot confirms; monitor `pg_replication_slots`
4. **Slot cleanup**: Always cleanup slots on shutdown to prevent WAL accumulation

## Cleanup

The connector manages cleanup automatically on graceful shutdown. For manual cleanup:

```sql
-- View slots
SELECT * FROM pg_replication_slots;

-- Drop a slot (be careful!)
SELECT pg_drop_replication_slot('dbmazz_slot');

-- View publications
SELECT * FROM pg_publication;

-- Drop a publication
DROP PUBLICATION dbmazz_pub;
```

## References

- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html)
- [pgoutput Message Formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
