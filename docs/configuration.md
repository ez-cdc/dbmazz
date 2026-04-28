# Configuration Reference

This document lists every environment variable the `dbmazz` daemon reads.
Variables marked **required** must always be provided via environment.

If you are using the `ez-cdc` CLI, most of these variables are set for you
from an `ez-cdc.yaml` file. Run `ez-cdc datasource init` to create one with
every option documented inline. The default location is
`$XDG_CONFIG_HOME/ez-cdc/config.yaml` (on macOS and Linux). Override with
`--config PATH` or `$EZ_CDC_CONFIG`.

## Source (PostgreSQL)

| Variable | Default | Description |
|---|---|---|
| `SOURCE_URL` | — (required) | PostgreSQL connection string. Must include `?replication=database` to enable logical replication. Example: `postgres://user:pass@host:5432/mydb?replication=database` |
| `SOURCE_TYPE` | `postgres` | Source connector type. Currently only `postgres` is supported. |
| `SOURCE_SLOT_NAME` | `dbmazz_slot` | Logical replication slot name. Created automatically if it doesn't exist. |
| `SOURCE_PUBLICATION_NAME` | `dbmazz_pub` | Publication name. Created automatically if it doesn't exist. |
| `TABLES` | `orders,order_items` | Comma-separated list of tables to replicate. Each table must exist in the source schema. |

## Sink (all types)

| Variable | Default | Description |
|---|---|---|
| `SINK_TYPE` | `starrocks` | Sink connector type: `starrocks`, `postgres`, or `snowflake`. |
| `SINK_URL` | — | Sink connection URL. For StarRocks: `http://host:8030` (FE HTTP). For PostgreSQL: `postgres://...`. For Snowflake: ignored (use `SINK_SNOWFLAKE_ACCOUNT`). |
| `SINK_PORT` | `9030` | Additional port when needed (e.g., StarRocks MySQL protocol port for DDL). |
| `SINK_DATABASE` | — (required) | Target database name. |
| `SINK_SCHEMA` | `public` | Target schema (PostgreSQL) or `PUBLIC` (Snowflake). StarRocks does not use this. |
| `SINK_USER` | `root` | Authentication username. |
| `SINK_PASSWORD` | *(empty)* | Authentication password. |

## Sink (Snowflake-specific)

| Variable | Default | Description |
|---|---|---|
| `SINK_SNOWFLAKE_ACCOUNT` | — | Snowflake account identifier, e.g. `xy12345.us-east-1`. |
| `SINK_SNOWFLAKE_WAREHOUSE` | — | Snowflake warehouse used for `COPY INTO` and `MERGE`. |
| `SINK_SNOWFLAKE_ROLE` | *(empty)* | Snowflake role (optional). |
| `SINK_SNOWFLAKE_PRIVATE_KEY_PATH` | *(empty)* | Path to the RSA private key for key-pair JWT authentication (preferred over user/password). |
| `SINK_SNOWFLAKE_SOFT_DELETE` | `true` | `true` = rows marked with `_DBMAZZ_IS_DELETED=true`; `false` = hard `DELETE`. |
| `SINK_SNOWFLAKE_MERGE_INTERVAL_MS` | `30000` | Normalizer MERGE polling interval in ms. |
| `SINK_SNOWFLAKE_FLUSH_FILES` | `20` | Trigger `COPY INTO` after accumulating this many staged Parquet files. For e2e testing, set to `1` for immediate flush. |
| `SINK_SNOWFLAKE_FLUSH_BYTES` | `104857600` | Trigger `COPY INTO` after accumulating this many bytes (default 100 MB). Whichever threshold (files or bytes) is reached first wins. |

## Pipeline / batching

| Variable | Default | Description |
|---|---|---|
| `FLUSH_SIZE` | `10000` | Maximum events per batch before flushing to the sink. |
| `FLUSH_INTERVAL_MS` | `5000` | Maximum milliseconds a batch can accumulate before being flushed. Whichever limit is hit first wins. |

## Snapshot / backfill

| Variable | Default | Description |
|---|---|---|
| `DO_SNAPSHOT` | `false` | Set to `true` to run a full initial snapshot before entering CDC mode. Uses the Flink CDC concurrent snapshot algorithm. |
| `SNAPSHOT_CHUNK_SIZE` | `50000` | Rows per snapshot chunk. Progress is tracked in the `dbmazz_snapshot_state` table in PostgreSQL, so interrupted snapshots resume from the last completed chunk. |
| `SNAPSHOT_PARALLEL_WORKERS` | `2` | Number of parallel snapshot workers. Reserved for future concurrent implementation; currently sequential. |
| `INITIAL_SNAPSHOT_ONLY` | `false` | If `true`, dbmazz exits after the snapshot completes — no CDC phase. Useful for one-off backfills. |

## Ports / networking

| Variable | Default | Description |
|---|---|---|
| `DBMAZZ_CONTROL_PORT` | `50051` | Port the internal HTTP control plane binds to (health, status, metrics, control). Formerly `GRPC_PORT`; the legacy name is still read for rolling compatibility. |

## Sink-specific requirements

Each sink has its own server-side prerequisites that the operator must satisfy
**before** starting dbmazz. The daemon validates these at setup time and fails
loud with an actionable error message if they are missing.

### StarRocks

| Requirement | Why | How to satisfy |
|---|---|---|
| Server version ≥ 3.2.0 | Older versions do not support `fast_schema_evolution`; runtime ALTER would be asynchronous and slow | Upgrade your StarRocks cluster |
| Target tables created by the operator | dbmazz does not create tables (different table types, distribution keys, partitions are operator decisions) | `CREATE TABLE ... PROPERTIES('fast_schema_evolution' = 'true')` ahead of time |
| Per-table `fast_schema_evolution=true` (recommended) | Without it, schema evolution operates in **degraded mode**: dbmazz arranca, logs a loud `WARN` per affected table, and skips ALTER for that table on every `SchemaChange` event. The daemon does not fail, but new columns added in source are NOT propagated. | At table creation: include `PROPERTIES('fast_schema_evolution' = 'true')`. The property is creation-only — to add it to an existing table, recreate via CTAS. |
| MySQL protocol port (default 9030) | DDL operations (audit columns, schema evolution) flow through the MySQL protocol | Open the port in your network |

### Snowflake

| Requirement | Why | How to satisfy |
|---|---|---|
| `ALTER TABLE` privilege on target schema | dbmazz applies `ALTER TABLE ADD COLUMN` for audit columns at setup and for new source columns at runtime | `GRANT ALTER ON ALL TABLES IN SCHEMA <db>.<schema> TO ROLE <role>` (or use a role with OWNERSHIP) |
| Warehouse + role + database configured via env vars | dbmazz uses these for COPY INTO / MERGE | See `SINK_SNOWFLAKE_*` env vars above |
| Target tables created by the operator | dbmazz does not create tables; CLUSTER BY, transient/permanent, and other Snowflake-specific decisions are operator-owned | `CREATE TABLE ...` ahead of time |
| Key-pair JWT auth (recommended over password) | Lower auth-related rotation overhead; `SINK_SNOWFLAKE_PRIVATE_KEY_PATH` env var | See [Snowflake key-pair auth docs](https://docs.snowflake.com/en/user-guide/key-pair-auth) |

### PostgreSQL (sink)

| Requirement | Why | How to satisfy |
|---|---|---|
| PostgreSQL 15+ | dbmazz uses `MERGE` (added in PG 15) | Upgrade |
| `CREATE` privilege on target database | dbmazz creates `_dbmazz` schema, raw table, metadata, schema-tracking, and target tables | `GRANT CREATE ON DATABASE <db> TO <role>` |
| (Existing PG sink behavior) target tables created by dbmazz | The PG sink **does** create target tables if they don't exist (legacy behavior; not symmetric with SR/SF) | No action needed |

## Observability

The HTTP control plane exposes runtime metrics at `GET /api/v1/cdc/metrics`. Notable counters:

| Field | Description |
|---|---|
| `events_per_second` | Throughput of CDC events processed in the last second. |
| `lag_bytes` | Difference between `current_lsn` and `confirmed_lsn` (PostgreSQL source). |
| `lag_events` | Pending events queued in the in-memory pipeline channel. |
| `replication_lag_ms` | End-to-end lag from source commit timestamp to sink ack. |
| `schema_evolution_skipped_total` | Cumulative count of `SchemaChange` events skipped because the target table doesn't satisfy the sink's schema-evolution prerequisite (e.g. StarRocks `fast_schema_evolution=true` not set). A non-zero value here means the daemon emitted a `WARN: schema change skipped` log somewhere — review the logs for the specific table/column. |
| `total_events_processed` | Cumulative event count since daemon start. |
| `total_batches_sent` | Cumulative batches flushed to sink. |
| `cpu_millicores` | Process CPU usage (1000 = 1 full core). |

## Logging

| Variable | Default | Description |
|---|---|---|
| `RUST_LOG` | `info` | Log level. Standard `env_logger` / `tracing_subscriber` format. Examples: `info`, `debug`, `dbmazz=trace,tokio_postgres=warn`. |

## Security notes

- **Never log `SOURCE_URL` or `SINK_PASSWORD`.** Both contain credentials.
- Structs that carry these values in the code MUST NOT derive `Debug` without
  redacting the secret fields.
- For Snowflake, prefer key-pair JWT (`SINK_SNOWFLAKE_PRIVATE_KEY_PATH`) over
  username/password — it's rotated less frequently and doesn't leak in
  audit logs.

## See also

- [`docs/architecture.md`](architecture.md) — data flow, module map, design decisions
- [`docs/contributing-connectors.md`](contributing-connectors.md) — how to add new source/sink connectors
