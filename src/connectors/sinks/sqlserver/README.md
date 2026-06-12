# SQL Server Sink Connector

> CDC sink connector for [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/), a relational OLTP database, using direct T-SQL MERGE statements with parameterized queries via tiberius.

## Features

- **Upserts**: Supported via T-SQL MERGE with parameterized queries
- **Deletes**: Hard deletes via DELETE FROM WHERE PK = @P1
- **Schema Evolution**: Automatic column addition via ALTER TABLE ADD COLUMN
- **Transactions**: Atomic writes per batch (MERGE + DDL in one transaction)
- **Loading Model**: Streaming — direct MERGE to target table, no intermediate staging

## Architecture

```
write_batch(Vec<CdcRecord>)
  │
  ├── [1] SchemaChange → ALTER TABLE ADD COLUMN  (within TX)
  ├── [2] INSERT/UPDATE  → MERGE INTO target      (within TX)
  ├── [3] DELETE         → DELETE FROM target      (within TX)
  └── [4] COMMIT                                   (atomic)
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SINK_URL` | SQL Server hostname or IP | required |
| `SINK_PORT` | SQL Server port | `1433` |
| `SINK_DATABASE` | Target database name | required |
| `SINK_USER` | SQL Server login user | `sa` |
| `SINK_PASSWORD` | SQL Server login password | `""` |
| `SINK_SCHEMA` | Target schema | `dbo` |

### Example

```bash
export SINK_TYPE=sqlserver
export SINK_URL=localhost
export SINK_PORT=1433
export SINK_DATABASE=ezcdc_demo
export SINK_USER=sa
export SINK_PASSWORD="P0stgr3s#2024"
export SINK_SCHEMA=dbo
```

## CDC Audit Columns

The sink adds two audit columns to every target table:

| Column | Type | Description |
|--------|------|-------------|
| `_dbmazz_synced_at` | `DATETIME2` | Timestamp when record was synced |
| `_dbmazz_op_type` | `SMALLINT` | Last operation type: 0=INSERT, 1=UPDATE, 2=DELETE |

## Loading Protocol

### Direct MERGE (target table)

1. **Schema Evolution**: `ALTER TABLE [schema].[table] ADD [column] [type] NULL` before any data mutations
2. **MERGE**: For INSERT/UPDATE records, a T-SQL MERGE statement using `USING (VALUES (...)) AS source` with parameterized `@P1, @P2, ...` placeholders
3. **DELETE**: For DELETE records, `DELETE FROM [schema].[table] WHERE [pk] = @P1`
4. **TOAST handling**: `Value::Unchanged` columns (PG TOAST) are excluded from the UPDATE SET clause

### Batching Strategy

- **Batch size**: 1 – 5,000 records
- **Flush interval**: 5,000ms
- **Grouping**: Records grouped by table, one MERGE per record

## Type Mappings

| Source DataType | SQL Server Type |
|----------------|-----------------|
| Boolean | BIT |
| Int16 | SMALLINT |
| Int32 | INT |
| Int64 | BIGINT |
| UInt64 | NUMERIC(20, 0) |
| Float32 | REAL |
| Float64 | FLOAT(53) |
| Decimal | NUMERIC(38, 0) |
| String / Text | NVARCHAR(MAX) |
| Bytes | VARBINARY(MAX) |
| Json / Jsonb | NVARCHAR(MAX) |
| Date | DATETIME2 |
| Time | TIME |
| Timestamp | DATETIME2 |
| TimestampTz | DATETIME2 |
| Uuid | UNIQUEIDENTIFIER |
