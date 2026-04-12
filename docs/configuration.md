# Configuration Reference

This document lists every environment variable the `dbmazz` daemon reads.
The HTTP API (with web UI at `http://localhost:8080`) is part of the default
build, so all variables are optional — you can configure everything from the
browser. For minimal builds made with `--no-default-features` (no HTTP API),
variables marked **required** must be provided via environment.

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
| `GRPC_PORT` | `50051` | gRPC server port (`HealthService`, `CdcControlService`, `CdcStatusService`, `CdcMetricsService`). |
| `HTTP_API_PORT` | `8080` | HTTP API port. Serves the dashboard UI, Prometheus metrics, and REST endpoints. Enabled by default; disabled only when built with `--no-default-features`. |

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
- [`e2e-cli/README.md`](../e2e-cli/README.md) — test harness and CLI
