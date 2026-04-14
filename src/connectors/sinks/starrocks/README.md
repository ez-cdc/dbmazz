# StarRocks Sink Connector

CDC sink connector for [StarRocks](https://www.starrocks.io/), a high-performance OLAP database.

## Overview

The StarRocks sink uses the Stream Load HTTP API for high-throughput data ingestion with support for:

- **Upserts**: Primary Key tables with automatic UPSERT semantics
- **Deletes**: Soft deletes with `dbmazz_is_deleted` flag
- **Partial Updates**: TOAST column optimization for large values
- **Schema Evolution**: Automatic column addition when source schema changes

## Architecture

```
CdcRecord --> StarRocksSink --> StreamLoadClient --> StarRocks FE (8040)
                  |                   |                    |
                  v                   v                    v
              types.rs          stream_load.rs       307 Redirect
              (mapping)         (HTTP client)             |
                                                          v
                                                    StarRocks BE (8040)
```

## Configuration

The sink is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SINK_URL` | StarRocks FE HTTP URL | required |
| `SINK_PORT` | MySQL protocol port for DDL | `9030` |
| `SINK_DATABASE` | Target database name | required |
| `SINK_USER` | Authentication username | `root` |
| `SINK_PASSWORD` | Authentication password | `""` |

### Example

```bash
export SINK_TYPE=starrocks
export SINK_URL=http://starrocks.example.com:8040
export SINK_PORT=9030
export SINK_DATABASE=cdc_analytics
export SINK_USER=root
export SINK_PASSWORD=secret
```

## CDC Audit Columns

The sink automatically adds audit columns to track CDC operations:

| Column | Type | Description |
|--------|------|-------------|
| `dbmazz_op_type` | TINYINT | Operation type: 0=INSERT, 1=UPDATE, 2=DELETE |
| `dbmazz_is_deleted` | BOOLEAN | Soft delete flag (true for DELETE operations) |
| `dbmazz_synced_at` | DATETIME | Timestamp when record was synced |
| `dbmazz_cdc_version` | BIGINT | Source LSN/position for ordering |

## Stream Load Protocol

The sink uses StarRocks' Stream Load HTTP API with these key features:

### Two-Phase Protocol

1. **FE Request**: Initial PUT request to Frontend (port 8040)
2. **307 Redirect**: FE returns redirect to Backend node
3. **BE Request**: Actual data upload to Backend

### Headers

```http
PUT /api/{database}/{table}/_stream_load HTTP/1.1
Expect: 100-continue
format: json
strip_outer_array: true
ignore_json_size: true
max_filter_ratio: 0.2
```

### Partial Updates

For TOAST columns (PostgreSQL large values that weren't changed):

```http
partial_update: true
partial_update_mode: row
columns: col1,col2,dbmazz_op_type,dbmazz_is_deleted,dbmazz_synced_at,dbmazz_cdc_version
```

## Type Mappings

| CDC Type | StarRocks Type |
|----------|---------------|
| Boolean | BOOLEAN |
| Int16 | SMALLINT |
| Int32 | INT |
| Int64 | BIGINT |
| Float32 | FLOAT |
| Float64 | DOUBLE |
| Decimal | DECIMAL(38,9) |
| String/Text | STRING |
| Json/Jsonb | JSON |
| Uuid | STRING |
| Date | DATE |
| Time | STRING |
| Timestamp | DATETIME |
| Bytes | VARBINARY |

## Table Requirements

Target tables must be created as **Primary Key** tables:

```sql
CREATE TABLE orders (
    id BIGINT,
    customer_id BIGINT,
    total DECIMAL(10,2),
    status VARCHAR(50),
    created_at DATETIME,
    -- CDC audit columns (auto-added if missing)
    dbmazz_op_type TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE',
    dbmazz_is_deleted BOOLEAN COMMENT 'Soft delete flag',
    dbmazz_synced_at DATETIME COMMENT 'Sync timestamp',
    dbmazz_cdc_version BIGINT COMMENT 'Source LSN'
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 8;
```

## Error Handling

The sink includes retry logic with exponential backoff:

- **Max retries**: 3
- **Backoff**: 100ms, 200ms, 400ms
- **Retriable errors**: Network timeouts, 5xx responses
- **Non-retriable**: Schema errors, authentication failures

## Files

| File | Description |
|------|-------------|
| `mod.rs` | Main sink implementation with `Sink` trait |
| `config.rs` | Configuration parsing and validation |
| `stream_load.rs` | HTTP Stream Load client |
| `setup.rs` | Schema setup and DDL operations |
| `types.rs` | Type mappings and conversions |

## Usage

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

## Performance Tuning

- **Batch size**: 10,000-100,000 records per batch
- **Flush interval**: 5 seconds
- **HTTP timeout**: 30 seconds
- **Filter ratio**: 0.2 (20% of rows can be filtered)

## Limitations

- No transaction support (eventual consistency)
- DATETIME doesn't preserve timezone (use UTC)
- TIME type not supported (stored as STRING)
- UUID stored as STRING (no native type)

## References

- [StarRocks Stream Load](https://docs.starrocks.io/docs/loading/StreamLoad/)
- [StarRocks Primary Key Tables](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/)
- [StarRocks Data Types](https://docs.starrocks.io/docs/sql-reference/data-types/)
