# Snowflake Sink Connector

CDC sink connector for [Snowflake](https://www.snowflake.com), a cloud data warehouse.

## Overview

The Snowflake sink uses a two-phase ELT pipeline: Parquet files uploaded to
an internal stage, `COPY INTO` a raw table with `PARSE_JSON → VARIANT`, and
an async normalizer `MERGE` that populates the final target tables. This
design decouples the ingestion throughput from the MERGE cost, which is
the right shape for Snowflake (where `MERGE` is relatively expensive).

## Features

- **Upserts**: Supported via `MERGE` on the primary key. Late-arriving
  duplicates are dedup'd via `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY cdc_version DESC)`.
- **Deletes**: Configurable. Default is **soft delete** (`_DBMAZZ_IS_DELETED=true`);
  `SINK_SNOWFLAKE_SOFT_DELETE=false` enables hard `DELETE` in the MERGE.
- **Schema Evolution**: Automatic. New columns in the source trigger
  `ALTER TABLE ADD COLUMN IF NOT EXISTS` on the raw and target tables.
- **Transactions**: Per-COPY atomicity. The raw table insert and metadata
  update happen in the same implicit Snowflake transaction. The normalizer
  MERGE is a separate transaction.
- **Loading Model**: Staged batch. Parquet files accumulate in the stage
  and are loaded in groups of ~20 per `COPY INTO` to amortize compile cost.
- **TOAST Handling**: `Unchanged` placeholders from PG logical decoding are
  resolved in the converter layer before serialization, so Snowflake always
  receives the actual value.

## Architecture

```
 CdcRecord batch
      │
      ▼
┌─────────────┐     ┌────────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Parquet    │     │  Internal      │     │  Raw table      │     │  Target      │
│  serializer │────▶│  stage (PUT)   │────▶│  _RAW_orders    │────▶│  orders      │
│             │     │  @_DBMAZZ      │     │  (VARIANT col)  │     │  (normalized)│
└─────────────┘     └────────────────┘     └─────────────────┘     └──────────────┘
     (sync)             (sync)              (sync COPY INTO)          (async MERGE)
```

The async normalizer runs in a background task and polls the
`_DBMAZZ._METADATA` table to find pending batches. It processes them
sequentially to preserve ordering.

## Configuration

The sink reads these environment variables:

| Variable | Description | Default |
|---|---|---|
| `SINK_TYPE` | Must be `snowflake` | — |
| `SINK_SNOWFLAKE_ACCOUNT` | Snowflake account (`xy12345.us-east-1`) | required |
| `SINK_DATABASE` | Target database | required |
| `SINK_SCHEMA` | Target schema | `PUBLIC` |
| `SINK_USER` | Authentication user | required |
| `SINK_PASSWORD` | Password (if not using key-pair) | — |
| `SINK_SNOWFLAKE_PRIVATE_KEY_PATH` | Path to RSA private key (key-pair JWT) | — |
| `SINK_SNOWFLAKE_WAREHOUSE` | Warehouse for COPY/MERGE | required |
| `SINK_SNOWFLAKE_ROLE` | Role (optional) | *(empty)* |
| `SINK_SNOWFLAKE_SOFT_DELETE` | `true` = soft delete, `false` = hard delete | `true` |

## CDC Audit Columns

The sink adds these audit columns to each replicated table:

| Column | Type | Description |
|---|---|---|
| `_DBMAZZ_OP_TYPE` | `NUMBER(3,0)` | Operation type: 0=INSERT, 1=UPDATE, 2=DELETE |
| `_DBMAZZ_IS_DELETED` | `BOOLEAN` | Soft delete flag (only when `SINK_SNOWFLAKE_SOFT_DELETE=true`) |
| `_DBMAZZ_SYNCED_AT` | `TIMESTAMP_NTZ` | When the row was last synced to the target |
| `_DBMAZZ_CDC_VERSION` | `NUMBER(20,0)` | Source position (LSN) for ordering within MERGE dedup |

All audit column names are upper-cased because Snowflake case-folds unquoted
identifiers to upper case.

## Loading Protocol

### Phase 1 — Sync (fast)

For each `write_batch()`:

1. **Serialize** the batch to a Parquet file using Arrow. Each column type
   maps to its Parquet equivalent (see Type Mappings below).
2. **PUT** the file to the internal stage `@_DBMAZZ` via the Snowflake
   driver's `PUT` protocol. This uses per-session temporary credentials
   to upload directly to the underlying cloud storage (S3/GCS/Azure).
3. **COPY INTO** the raw table `_DBMAZZ._RAW_{table}` with
   `FILE_FORMAT = (TYPE = PARQUET)`, parsing the Parquet into a single
   `VARIANT` column.
4. **Update metadata**: increment `sync_batch_id` in `_DBMAZZ._METADATA`
   for this job.

Files accumulate in the stage; every ~20 files triggers a single `COPY INTO`
to amortize the compile cost of each COPY.

### Phase 2 — Async (background normalizer)

The normalizer task polls `_DBMAZZ._METADATA` every few seconds:

1. Fetch `(normalize_batch_id, sync_batch_id)`. If `sync > normalize`, there's work.
2. For each pending batch, read the new rows from the raw table.
3. Run a `MERGE` against the target table:
   - `ON` the primary key columns
   - `WHEN MATCHED AND target.cdc_version < source.cdc_version THEN UPDATE`
   - `WHEN NOT MATCHED THEN INSERT`
   - If hard delete: `WHEN MATCHED AND source.op_type=2 THEN DELETE`
4. Delete the processed rows from the raw table.
5. Advance `normalize_batch_id` in `_DBMAZZ._METADATA`.

The normalizer uses `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY cdc_version DESC)`
to dedupe within a batch before running the MERGE, so multiple updates to the
same row collapse to the latest.

## Type Mappings

| CDC DataType | Snowflake Type |
|---|---|
| `Boolean` | `BOOLEAN` |
| `Int16` | `NUMBER(5,0)` |
| `Int32` | `NUMBER(10,0)` |
| `Int64` | `NUMBER(20,0)` |
| `Float32` | `FLOAT` |
| `Float64` | `FLOAT` |
| `Decimal(p, s)` | `NUMBER(p, s)` |
| `String`, `Text` | `VARCHAR` |
| `Bytes` | `BINARY` |
| `Json`, `Jsonb` | `VARIANT` |
| `Uuid` | `VARCHAR(36)` |
| `Date` | `DATE` |
| `Time` | `TIME` |
| `Timestamp` | `TIMESTAMP_NTZ` |
| `TimestampTz` | `TIMESTAMP_TZ` |

## Module Structure

```
snowflake/
├── mod.rs                  # Sink trait impl, capabilities, wiring
├── config.rs               # Parse env vars into SnowflakeSinkConfig
├── client.rs               # Low-level SQL client (connect, query, execute)
├── stage.rs                # Internal stage lifecycle (create, PUT, COPY INTO)
├── parquet_writer.rs       # CdcRecord → Parquet serialization via Arrow
├── normalizer.rs           # Async MERGE normalizer (background task)
├── merge_generator.rs      # Generates MERGE SQL from table metadata
├── setup.rs                # DDL: create schema, raw tables, metadata, target tables
├── types.rs                # CDC DataType → Snowflake SQL type mapping
└── README.md               # This file
```

## Performance Tuning

- **`FLUSH_SIZE`**: 10,000–50,000 events. Larger batches = fewer Parquet
  files + fewer `COPY INTO` statements = lower cost.
- **`FLUSH_INTERVAL_MS`**: 5,000–30,000 ms. Lower = less latency; higher =
  better batching. The default of 5s is a good balance for most workloads.
- **Warehouse size**: X-SMALL is enough for the normalizer MERGE on most
  tables. Scale up if target tables exceed ~100M rows.
- **File accumulation**: currently hard-coded at 20 files per COPY INTO.
  Lower it if you see `COPY INTO` failures due to file-count limits;
  raise it to reduce compile overhead.

## Capabilities

```rust
SinkCapabilities {
    supports_upsert: true,
    supports_delete: true,          // hard or soft depending on config
    supports_schema_evolution: true,
    supports_transactions: false,   // per-batch only, not cross-batch
    loading_model: LoadingModel::StagedBatch,
    min_batch_size: Some(1),
    max_batch_size: Some(1_000_000),
    optimal_flush_interval_ms: 5000,
}
```

## Schema Evolution

New columns added to the source via `ALTER TABLE ADD COLUMN` are detected
automatically and propagated to both the raw table and the target table.
The normalizer uses `SELECT *` from the raw table, so new columns flow
through transparently.

Column drops and renames are **not** automated — they require manual
`ALTER TABLE` on the target.

## Limitations

- **Hard delete + late-arriving updates**: if the same row is deleted and
  then updated in a later batch, the normalizer MERGE will hard-delete the
  row. This matches Snowflake's standard `MERGE` semantics and is the
  expected behavior.
- **VARIANT size limit**: Snowflake has a 16 MB limit per VARIANT value.
  Extremely wide rows (e.g., large JSON blobs) can exceed this. dbmazz
  does not split such rows — they'll error at COPY INTO.
- **Cross-region**: the sink writes to the stage in Snowflake's configured
  region. If your worker is in a different region, expect higher latency
  on the PUT phase.
- **ARRAY/ENUM types**: not yet supported. Source columns of these types
  are serialized as text in the Parquet file.

## References

- [Snowflake COPY INTO documentation](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- [Snowflake PUT documentation](https://docs.snowflake.com/en/sql-reference/sql/put)
- [Snowflake MERGE documentation](https://docs.snowflake.com/en/sql-reference/sql/merge)
- [Snowflake Parquet format](https://docs.snowflake.com/en/user-guide/data-load-prepare-file-format-parquet)
- [Snowflake key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)
