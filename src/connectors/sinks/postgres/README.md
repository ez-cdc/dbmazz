# PostgreSQL Sink Connector

> CDC sink connector for [PostgreSQL](https://www.postgresql.org/) >= 15, using a raw table + MERGE pattern based on [PeerDB](https://github.com/PeerDB-io/peerdb).

## Features

- **Upserts**: Supported via MERGE (PG >= 15)
- **Deletes**: Hard deletes via MERGE WHEN MATCHED ... DELETE
- **Schema Evolution**: Not supported (tables must be pre-created or auto-created at setup)
- **Transactions**: Atomic writes per batch (COPY + metadata in one transaction)
- **Loading Model**: Staged batch — COPY into raw table, async MERGE into target

## Architecture

```
write_batch(Vec<CdcRecord>)
  │
  ├── [1] COPY INTO _dbmazz._raw_{job}      (staging table, within TX)
  ├── [2] UPDATE _dbmazz._metadata           (sync_batch_id++, within TX)
  ├── [3] COMMIT                             (atomic: raw data + metadata)
  └── [4] Notify normalizer                  (async background task)
              │
              └── For each pending batch (one at a time):
                    ├── MERGE INTO target_table   (dedup + upsert/delete)
                    ├── UPDATE _metadata           (normalize_batch_id)
                    ├── DELETE FROM raw_table       (cleanup)
                    └── COMMIT                     (atomic: all 3 above)
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SINK_URL` | PostgreSQL connection string | required |
| `SINK_DATABASE` | Target database name | required |
| `SINK_SCHEMA` | Target schema | `public` |
| `SINK_TYPE` | Must be `postgres` | required |

### Example

```bash
export SINK_TYPE=postgres
export SINK_URL="postgres://user:pass@host:5432/mydb"
export SINK_DATABASE=mydb
export SINK_SCHEMA=public
```

## CDC Audit Columns

The sink adds two audit columns to every target table:

| Column | Type | Description |
|--------|------|-------------|
| `_dbmazz_synced_at` | `TIMESTAMPTZ` | Timestamp when record was synced (default: `now()`) |
| `_dbmazz_op_type` | `SMALLINT` | Last operation type: 0=INSERT, 1=UPDATE, 2=DELETE |

## Loading Protocol

### COPY + MERGE (raw table → target)

1. **COPY**: CDC records are serialized to JSON and written to the raw table via `COPY FROM STDIN` (binary protocol, fastest bulk insert)
2. **Dedup**: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _timestamp DESC)` keeps only the latest record per PK within each batch
3. **MERGE**: Applies deduped records to the target table — INSERT for new rows, UPDATE for existing, DELETE for removed

### Raw Table Schema

```sql
CREATE TABLE _dbmazz._raw_{job_name} (
    _uid           UUID      NOT NULL DEFAULT gen_random_uuid(),
    _timestamp     BIGINT    NOT NULL,    -- nanosecond wall clock
    _dst_table     TEXT      NOT NULL,    -- "schema.table"
    _data          JSONB     NOT NULL,    -- all column values
    _record_type   SMALLINT  NOT NULL,    -- 0=INSERT, 1=UPDATE, 2=DELETE
    _match_data    JSONB,                 -- old values (for UPDATE/DELETE)
    _batch_id      BIGINT,               -- batch identifier
    _toast_columns TEXT                   -- comma-separated unchanged columns
);
-- Indexes:
CREATE INDEX idx_{job}_batch ON ... (_batch_id);
CREATE INDEX idx_{job}_dst   ON ... (_dst_table);
```

### Batching Strategy

- **Batch size**: 1 – 250,000 records
- **Flush interval**: 10,000ms (optimal for MERGE overhead)
- **Grouping**: All tables in one COPY, MERGE per table

## Type Mappings

PostgreSQL → PostgreSQL is mostly identity mapping. Serial types become plain integers (no sequence in target).

| Source OID | Source Type | Target Type |
|-----------|-------------|-------------|
| 16 | boolean | boolean |
| 21 | smallint | smallint |
| 23 | integer | integer |
| 20 | bigint | bigint |
| 700 | real | real |
| 701 | double precision | double precision |
| 1700 | numeric | numeric |
| 25 | text | text |
| 1043 | varchar | text |
| 114 | json | json |
| 3802 | jsonb | jsonb |
| 2950 | uuid | uuid |
| 1114 | timestamp | timestamp without time zone |
| 1184 | timestamptz | timestamp with time zone |
| 17 | bytea | bytea |
| _ | (unknown) | text |

Full mapping in `types.rs` (80+ OIDs).

## TOAST Handling

When PostgreSQL sends an UPDATE with unchanged TOAST columns (`Value::Unchanged`), the sink:

1. Records the unchanged column names in `_toast_columns` (e.g., `"big_col,other_col"`)
2. Excludes those values from `_data` JSON
3. The MERGE generator creates a separate `WHEN MATCHED` clause per unique TOAST combination, updating only the changed columns

## Error Handling

- **Normalizer retries**: Infinite with exponential backoff (2s → 4s → ... → 300s max)
- **Retriable errors**: Any MERGE/transaction failure (PG connection lost, deadlock, etc.)
- **Non-retriable errors**: None — normalizer always retries until success
- **Atomicity**: Each batch normalize (MERGE + metadata + cleanup) runs in a single transaction — crash-safe

## Module Structure

```
postgres/
├── mod.rs              # PostgresSink: Sink trait implementation
├── raw_table.rs        # COPY writer: CdcRecord → raw table (binary COPY)
├── normalizer.rs       # Async background task: raw → MERGE → target
├── merge_generator.rs  # Dynamic MERGE SQL generation (PG >= 15)
├── setup.rs            # DDL: schema, raw table, metadata, target tables
├── types.rs            # PG OID → target type mapping (80+ types)
└── README.md           # This file
```

## Capabilities

```rust
SinkCapabilities {
    supports_upsert: true,
    supports_delete: true,
    supports_schema_evolution: false,
    supports_transactions: true,
    loading_model: LoadingModel::StagedBatch { stage_format: StageFormat::Json },
    min_batch_size: Some(1),
    max_batch_size: Some(250_000),
    optimal_flush_interval_ms: 10_000,
}
```

## Limitations

- **Requires PostgreSQL >= 15** for MERGE support (no fallback for older versions)
- **Soft delete not supported** — DELETE records result in hard deletes
- **Schema evolution not supported** — new columns in source require manual `ALTER TABLE` on target
- **Arrays and bytea** use direct casting from JSON (may fail for complex nested arrays)
- **User-defined types** fall back to `text`

## References

- [PostgreSQL MERGE documentation](https://www.postgresql.org/docs/15/sql-merge.html)
- [PostgreSQL COPY documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [PeerDB PostgreSQL connector](https://github.com/PeerDB-io/peerdb/tree/main/flow/connectors/postgres) — reference implementation
