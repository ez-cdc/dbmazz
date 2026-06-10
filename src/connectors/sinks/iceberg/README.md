# Iceberg Sink Connector

CDC sink connector for [Apache Iceberg](https://iceberg.apache.org/) tables on S3-compatible object stores, managed through an Iceberg REST catalog.

## Overview

Streams CDC events into Iceberg tables as an **append-only changelog**: every source INSERT/UPDATE/DELETE becomes one row in the target table, annotated with CDC metadata columns. The resulting tables are readable by any Iceberg-compliant engine (Spark, Trino, DuckDB, Athena, Snowflake external tables).

## Features

- **Upserts**: Not supported (by design). The table is a changelog; consumers materialize current state downstream (e.g. `MERGE`/window-dedup by primary key and `_cdc_position`).
- **Deletes**: Delivered as changelog rows with `_cdc_op = 'D'` carrying the deleted row image — never physical deletes.
- **Schema Evolution**: Not auto-applied (iceberg-rust 0.9 transactions cannot update schemas). `SchemaChange` events are counted in the `schema_evolution_skipped` metric and logged as WARN; values for unknown columns are skipped with a WARN naming the columns.
- **Transactions**: Atomic per table per batch (one Iceberg snapshot commit). No cross-table atomicity within a batch.
- **Loading Model**: Staged batch — Parquet data files + FastAppend snapshot commit.

## Architecture

```
CdcRecord --> IcebergSink --> arrow_convert --> DataFileWriter --> S3 (Parquet)
                  |               (strict           (iceberg-rust:      |
                  v                typed             field-ids,         v
              schema.rs            mapping)          stats)        FastAppend commit
              (DataType →                                          via REST catalog
               Iceberg types)                                      (snapshot N+1)
```

**Durability contract**: `write_batch()` returns `Ok` only after the catalog commit succeeds for every touched table. The pipeline's LSN confirmation therefore never outruns durable data. A crash mid-batch leaves at most *uncommitted* (unreferenced) Parquet files, which Iceberg readers never see; on restart the batch replays from the last confirmed LSN (at-least-once, like every dbmazz sink).

## Configuration

Enabled with the opt-in cargo feature `sink-iceberg` (not in default features):

```bash
cargo build --release --features sink-iceberg
```

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SINK_TYPE` | Must be `iceberg` | required |
| `SINK_DATABASE` | Target Iceberg namespace | required |
| `ICEBERG_CATALOG_URI` | REST catalog endpoint | required |
| `ICEBERG_WAREHOUSE` | Warehouse location/identifier passed to the catalog | required |
| `ICEBERG_COMMIT_RETRIES` | Commit retries on transient catalog failures | `3` |
| `S3_REGION` | S3 region | unset |
| `S3_ENDPOINT` | Custom endpoint (MinIO/LocalStack) | unset |
| `S3_ACCESS_KEY_ID` | Static access key (falls back to ambient credentials) | unset |
| `S3_SECRET_ACCESS_KEY` | Static secret key | unset |
| `S3_PATH_STYLE` | Path-style addressing (`true` for MinIO) | `false` |

`SINK_URL`, `SINK_PORT`, `SINK_USER`, and `SINK_PASSWORD` are unused by this sink.

### Example (MinIO + Lakekeeper-style REST catalog)

```bash
export SINK_TYPE=iceberg
export SINK_DATABASE=analytics
export ICEBERG_CATALOG_URI=http://localhost:8181/catalog
export ICEBERG_WAREHOUSE=warehouse
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
export S3_REGION=us-east-1
export S3_PATH_STYLE=true
```

## CDC Metadata Columns

Every table gets three required columns appended after the source columns:

| Column | Iceberg type | Content |
|--------|--------------|---------|
| `_cdc_op` | `string` | `I` (insert), `U` (update, new image), `D` (delete, last image) |
| `_cdc_position` | `long` | PostgreSQL: LSN (globally ordered). MySQL: binlog position — ordered **within** a binlog file only; across file rotations use `_cdc_ts` as tiebreaker |
| `_cdc_ts` | `timestamptz` | Sink-side write timestamp (UTC) |

## Table Naming & Creation

- Namespace = `SINK_DATABASE`; created at setup if missing.
- Source `schema.table` → Iceberg table `schema__table` (flat naming avoids multi-level-namespace differences between catalog servers).
- Tables are auto-created at setup (format V2, unpartitioned) with field-ids `1..N` for source columns and `N+1..N+3` for metadata columns. All source columns are nullable in the target — a changelog row may legitimately miss columns (e.g. DELETE images under partial replica identity).
- An existing table is reused if it contains every source column plus the metadata columns; otherwise setup fails with the missing column list.

## Type Mappings

| dbmazz `DataType` | Iceberg type | Notes |
|---|---|---|
| `Boolean` | `boolean` | |
| `Int16`, `Int32` | `int` | |
| `Int64` | `long` | |
| `UInt64` | `decimal(20,0)` | full u64 range, no wrap-around |
| `Float32` | `float` | |
| `Float64` | `double` | |
| `Decimal{p,s}` | `decimal(p,s)` | clamped to Iceberg's 1..38 |
| `String`, `Text`, `Json`, `Jsonb` | `string` | |
| `Uuid` | `uuid` | |
| `Date` | `date` | |
| `Time` | `time` | |
| `Timestamp` | `timestamp` | |
| `TimestampTz` | `timestamptz` | |
| `Bytes` | `binary` | PG `\x…` hex text is decoded |

Conversion is **strict**: a value that does not match its column type fails the batch with an error naming table, column, and both types. Values are never coerced to silent defaults. `Value::Unchanged` (PostgreSQL TOAST) is written as NULL and counted in a WARN log — use `REPLICA IDENTITY FULL` (dbmazz's PG setup default) to avoid it.

## Operational Notes

- **Snapshot growth**: each flushed batch commits one Iceberg snapshot per touched table. The sink advertises `optimal_flush_interval_ms = 60_000` to bound commit frequency. Snapshot expiry, manifest compaction, and orphan-file cleanup are the **operator's responsibility** (e.g. Spark's `expire_snapshots` / `remove_orphan_files` procedures, or your catalog's maintenance).
- **Catalog support**: any Iceberg REST catalog (Lakekeeper, Polaris, Nessie, Tabular, AWS Glue REST endpoint). Hive Metastore / native Glue / filesystem catalogs are not supported.
- **Concurrent writers**: commits retry with table refresh up to `ICEBERG_COMMIT_RETRIES` on conflicts, but dbmazz assumes one job per table (its standard deployment model).

## Error Handling

| Error | Behavior |
|-------|----------|
| Catalog unreachable | `validate_connection()`/`setup()` fail at startup |
| Missing `ICEBERG_CATALOG_URI` / `ICEBERG_WAREHOUSE` | Config load fails naming the variable |
| Commit conflict / transient 5xx | Retry with table refresh + linear backoff, then fail the batch |
| Type mismatch in a record | Batch fails (pipeline stops) — data is never silently corrupted |
| Schema drift (new source columns) | Rows written without those columns; WARN + `schema_evolution_skipped` metric |

## Module Structure

```
iceberg/
├── mod.rs            IcebergSink (Sink trait impl, write/commit loop)
├── config.rs         IcebergSinkConfig (env parsing, redacted Debug)
├── catalog.rs        REST catalog wrapper (namespace/table bootstrap, probe)
├── schema.rs         DataType → Iceberg schema (field-ids, _cdc_* columns)
└── arrow_convert.rs  CdcRecord → Arrow RecordBatch (strict typed mapping)
```
