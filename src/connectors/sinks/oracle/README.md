# Oracle Sink Connector

> CDC sink connector for [Oracle Database](https://www.oracle.com/database/) (12c+), using MERGE INTO for idempotent upserts.

## Features

- **Upserts**: Idempotent via Oracle `MERGE INTO ... USING DUAL` (at-least-once safe)
- **Deletes**: Hard deletes via `DELETE WHERE EXISTS`
- **Schema Evolution**: Automatic `ALTER TABLE ADD` for new columns
- **Pure Rust**: No Oracle Client (OCI) installation required — uses `oracle-rs` pure Rust TNS protocol driver

## Architecture

```
write_batch(Vec<CdcRecord>)
  │
  ├── [1] Schema evolution pre-pass (ALTER TABLE ADD COLUMN)
  ├── [2] Convert records to MERGE SQL per table
  ├── [3] Execute SQL statements in Oracle
  └── [4] COMMIT
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SINK_URL` | Oracle connection string (host:port/service) | required |
| `SINK_DATABASE` | Oracle schema name | required |
| `SINK_USER` | Oracle username | required |
| `SINK_PASSWORD` | Oracle password | required |
| `SINK_TYPE` | Must be `oracle` | required |

### Example

```bash
export SINK_TYPE=oracle
export SINK_URL="localhost:1521/ORCLCDB"
export SINK_DATABASE=CDC_SCHEMA
export SINK_USER=cdc_user
export SINK_PASSWORD=cdc_pass
```

## CDC Audit Columns

| Column | Type | Description |
|--------|------|-------------|
| `dbmazz_op_type` | `NUMBER(1)` | 0=INSERT, 1=UPDATE, 2=DELETE |
| `dbmazz_is_deleted` | `NUMBER(1)` | Soft delete flag |
| `dbmazz_synced_at` | `TIMESTAMP(6)` | Timestamp when record was synced |
| `dbmazz_cdc_version` | `NUMBER(19)` | Source LSN/position for ordering |

## Type Mappings

| DataType | Oracle Target Type |
|----------|-------------------|
| Boolean | `NUMBER(1)` |
| Int16 | `NUMBER(5)` |
| Int32 | `NUMBER(10)` |
| Int64 | `NUMBER(19)` |
| Float32 | `BINARY_FLOAT` |
| Float64 | `BINARY_DOUBLE` |
| Decimal(p,s) | `NUMBER(p,s)` |
| String | `VARCHAR2(4000)` |
| Text | `CLOB` |
| Bytes | `BLOB` |
| Json/Jsonb | `CLOB` |
| Uuid | `VARCHAR2(36)` |
| Date | `DATE` |
| Time | `INTERVAL DAY TO SECOND` |
| Timestamp | `TIMESTAMP(6)` |
| TimestampTz | `TIMESTAMP(6) WITH TIME ZONE` |

## Capabilities

```rust
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
```

## Limitations

- Schema evolution: ADD COLUMN only (no DROP or type change)
- Requires at least one primary key column for upsert functionality
- Tables without PKs will use INSERT-only mode
- Oracle 12c+ required
