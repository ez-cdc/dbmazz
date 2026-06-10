# Oracle Sink Connector

> CDC sink connector for [Oracle Database](https://www.oracle.com/database/), an enterprise OLTP/OLAP database. Uses MERGE (upsert) for INSERT/UPDATE and direct DELETE for deletion operations.

## Features

- **Upserts**: Supported via MERGE (Oracle 9i+)
- **Deletes**: Hard deletes via DELETE
- **Schema Evolution**: Not supported (new columns from source are skipped)
- **Transactions**: Atomic writes per batch (MERGE + metadata in one transaction)
- **Loading Model**: Streaming — direct MERGE to target tables (no staging)

## Architecture

```
CdcRecord --> OracleSink --> oracle crate (kubo/rust-oracle) --> Oracle Database
                  |                      |                            |
                  v                      v                            v
              types.rs           spawn_blocking                  SQL*Net
              (mapping)          (sync API on Tokio)          (OCI/OPI-C)
```

## Configuration

The sink is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SINK_URL` | Oracle connection string (e.g., `//host:1521/FREEPDB1`) | required |
| `SINK_SCHEMA` | Target schema name | `DBMAZZ` |
| `SINK_USER` | Oracle username | required |
| `SINK_PASSWORD` | Oracle password | required |
| `SINK_TYPE` | Must be `oracle` | required |

### Example

```bash
export SINK_TYPE=oracle
export SINK_URL="//localhost:1521/FREEPDB1"
export SINK_DATABASE=FREEPDB1
export SINK_SCHEMA=DBMAZZ
export SINK_USER=postgres
export SINK_PASSWORD=postgres
```

## CDC Audit Columns

The sink adds two audit columns to every target table:

| Column | Type | Description |
|--------|------|-------------|
| `_DBMAZZ_SYNCED_AT` | `TIMESTAMP(6)` | Timestamp when record was synced (default: `SYSTIMESTAMP`) |
| `_DBMAZZ_OP_TYPE` | `NUMBER(2)` | Last operation type: 0=INSERT, 1=UPDATE, 2=DELETE |

### Metadata Table

The sink creates a `_DBMAZZ_METADATA` table in the target schema during `setup`:

```sql
CREATE TABLE DBMAZZ._DBMAZZ_METADATA (
    key   VARCHAR2(255) PRIMARY KEY,
    val   VARCHAR2(4000)
);
```

This table tracks CDC offset state (SCN-based position tracking).

## Loading Protocol

### Direct MERGE (no staging)

Unlike the PostgreSQL sink (which uses a staging raw table + background normalizer), the Oracle sink writes directly to target tables using MERGE statements:

1. **MERGE** (for INSERT/UPDATE): `MERGE INTO target USING DUAL ON (pk match) WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...)`
2. **DELETE** (for DELETE): `DELETE FROM target WHERE pk = :1`
3. Each batch runs in a single transaction with `_DBMAZZ_METADATA` position tracking

### Batching Strategy

- **Batch size**: 1 – 250,000 records
- **Flush interval**: 10,000ms
- **Grouping**: Records grouped by table, one MERGE per table per batch

## Type Mappings

| CDC DataType | Oracle Type |
|--------------|-------------|
| `Boolean` | `NUMBER(1)` |
| `Int16` | `NUMBER(5)` |
| `Int32` | `NUMBER(10)` |
| `Int64` | `NUMBER(19)` |
| `Float32` | `BINARY_FLOAT` |
| `Float64` | `BINARY_DOUBLE` |
| `Decimal` | `NUMBER(p,s)` |
| `String/Text` | `VARCHAR2(4000)` |
| `Json/Jsonb` | `CLOB` |
| `Uuid` | `VARCHAR2(36)` |
| `Date` | `DATE` |
| `Time` | `TIMESTAMP` |
| `Timestamp` | `TIMESTAMP` |
| `Bytes` | `BLOB` |

## Table Requirements

Target tables are created automatically by the sink during `setup` with:

```sql
CREATE TABLE DBMAZZ."{table_name}" (
    {columns},
    _DBMAZZ_SYNCED_AT TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
    _DBMAZZ_OP_TYPE NUMBER(2) DEFAULT 0
);
```

- **Primary keys**: Required for MERGE (upsert) — sourced from the source table's schema
- **Identifiers**: All table/column names are quoted and uppercased to match Oracle's metadata convention

## TOAST Handling

When PostgreSQL sends an UPDATE with unchanged TOAST columns (`Value::Unchanged`), the sink:

1. Detects unchanged columns from the incoming `CdcRecord`
2. Excludes those columns from the MERGE `UPDATE SET` clause
3. Prevents silent NULL overwrite of unmodified TOAST columns in Oracle

## Error Handling

The sink includes retry logic:

- **Max retries**: 3 per batch
- **Backoff strategy**: Exponential (1s → 2s → 4s)
- **Retriable errors**: Connection failures, transient Oracle errors
- **Non-retriable errors**: Invalid SQL, missing tables, constraint violations

### Common Errors

1. **`ORA-01830` - date format picture ends before converting entire input**
   - Cause: Timestamp string with trailing dot or incomplete fractional seconds
   - Solution: The sink uses `.FF6` format mask and strips trailing dots before formatting

2. **`ORA-00942` - table or view does not exist**
   - Cause: Target table not created yet
   - Solution: The sink creates tables automatically during `setup()` — ensure `setup` runs before `write_batch`

3. **Connection failures with Instant Client**
   - Cause: Oracle Instant Client libraries not found at runtime
   - Solution: Use the `dbmazz-oracle:dev` Docker image which bundles Instant Client, or set `LD_LIBRARY_PATH`

## Module Structure

```
oracle/
├── mod.rs              # OracleSink: Sink trait implementation
├── merge_generator.rs  # MERGE SQL generation (Oracle dialect)
├── schema_tracking.rs  # Schema state cache
├── setup.rs            # DDL: metadata table, target tables
├── types.rs            # DataType → Oracle type mapping + value conversion
└── README.md           # This file
```

## Capabilities

```rust
SinkCapabilities {
    supports_upsert: true,
    supports_delete: true,
    supports_schema_evolution: false,
    supports_transactions: true,
    loading_model: LoadingModel::Streaming,
    min_batch_size: Some(1),
    max_batch_size: Some(250_000),
    optimal_flush_interval_ms: 10_000,
}
```

## Schema Evolution

Schema evolution is **not supported** for the Oracle sink. New columns added to the source table are silently skipped during CDC replication. To add columns to the target Oracle table, apply them manually:

```sql
ALTER TABLE DBMAZZ."{table_name}" ADD ({column_name} {oracle_type});
```

## Prerequisites

| Requirement | Why | How to satisfy |
|---|---|---|
| Oracle Instant Client | The `oracle` crate requires ODPI-C (bundled in Instant Client) at runtime | Use the `dbmazz-oracle` Docker image which bundles Instant Client Basic Light |
| Target schema must exist | The sink creates tables within the target schema but does not create the schema itself | `CREATE USER DBMAZZ IDENTIFIED BY ...; GRANT CONNECT, RESOURCE TO DBMAZZ;` |
| Oracle 12c+ | Tested against Oracle 12c, 19c, 21c, and 23c (including Oracle Free) | Use `gvenzl/oracle-free` for testing |

## Limitations

- **Schema evolution**: Not supported — `ALTER TABLE ADD COLUMN` is not propagated
- **No staging area**: Writes go directly to target tables (no transactional rollback for partial batch failures)
- **No Instant Client on ARM64 macOS**: Oracle Instant Client for macOS is x86_64 only — use Docker for development on Apple Silicon

## References

- [Oracle Database Documentation](https://docs.oracle.com/en/database/)
- [kubo/rust-oracle crate](https://github.com/kubo/rust-oracle)
- [Oracle MERGE statement](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/MERGE.html)
