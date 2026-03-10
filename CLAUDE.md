# CLAUDE.md - DBMazz CDC Daemon

## Project Overview

Rust CDC (Change Data Capture) daemon. Reads PostgreSQL WAL (Write-Ahead Log) via logical replication and streams changes to StarRocks via Stream Load HTTP API. Each instance handles one replication job.

## Architecture

```
dbmazz (single binary, tokio async runtime)
├── Source: PostgreSQL logical replication
│   ├── Replication slot management
│   ├── WAL message parsing (pgoutput protocol)
│   └── LSN tracking (current_lsn, confirmed_lsn)
├── Pipeline
│   ├── Schema cache (table metadata, column types)
│   ├── Record transformation (PG types → StarRocks types)
│   └── Batching and buffering
├── Sink: StarRocks Stream Load
│   ├── HTTP-based bulk loading
│   ├── CSV/JSON format conversion
│   └── Transaction management
└── gRPC server
    ├── Health and status reporting
    ├── Metrics exposure (LSN, events, tables)
    └── State management (running, paused, stopped)
```

## Key Directories

- `src/connectors/sources/postgres/` - PostgreSQL source connector
  - `replication.rs` - WAL logical replication stream
  - `parser.rs` - pgoutput protocol parser
  - `types.rs` - PG type mapping
  - `config.rs` - Source configuration
  - `setup.rs` - Replication slot and publication setup
- `src/connectors/sinks/starrocks/` - StarRocks sink connector
  - `stream_load.rs` - HTTP Stream Load client
  - `types.rs` - StarRocks type mapping
  - `config.rs` - Sink configuration
  - `setup.rs` - Table validation and DDL
- `src/pipeline/` - Data pipeline (schema cache, transformation, batching)
- `src/core/` - Core abstractions (Record, Position, traits, errors)
- `src/engine/` - Engine orchestration
  - `snapshot/` - Snapshot/backfill worker (Flink CDC concurrent snapshot algorithm)
  - `setup/` - Source/sink setup phase
- `src/replication/` - WAL handler and replication state
- `src/http_api/` - HTTP API + web UI (--features http-api)
- `src/demo/` - Demo/quickstart data generation (--features demo)
- `src/grpc/` - gRPC server (state, services, metrics)
- `src/config.rs` - Configuration from environment variables
- `src/source/` - Source abstraction layer
- `src/sink/` - Sink abstraction layer

## Feature Flags

- `--features http-api` - Enables HTTP API + web UI on port 8080 (setup wizard, dashboard, REST endpoints)
- `--features demo` - Enables demo mode with sample data generation

## HTTP API (--features http-api)

When built with `--features http-api`, dbmazz exposes a web UI and REST endpoints on `HTTP_API_PORT` (default 8080):

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Web UI (setup wizard or live dashboard) |
| GET | `/healthz` | Health check |
| GET | `/status` | Full metrics JSON |
| GET | `/metrics/prometheus` | Prometheus metrics |
| POST | `/pause` | Pause replication |
| POST | `/resume` | Resume replication |
| POST | `/drain-stop` | Graceful drain and stop |
| POST | `/api/datasources/test` | Test connection |
| POST | `/api/tables/discover` | Discover tables |
| POST | `/api/replication/start` | Start replication |
| POST | `/api/replication/stop` | Stop replication |

## Snapshot / Backfill

Set `DO_SNAPSHOT=true` for initial data backfill. Uses Flink CDC concurrent snapshot algorithm:
1. Chunks table by PK ranges (`SNAPSHOT_CHUNK_SIZE`, default 50000 rows)
2. For each chunk: low-watermark → SELECT → high-watermark → Stream Load
3. WAL consumer checks `should_emit()` to suppress duplicate events within completed chunks
4. Progress tracked in `dbmazz_snapshot_state` table (resumable)

Can also be triggered on-demand via gRPC: `CdcControlService/StartSnapshot`

## gRPC Services

- `HealthService` - Health check
- `CdcControlService` - Pause/Resume/StartSnapshot/DrainStop
- `CdcStatusService` - GetStatus (LSN, events, snapshot progress)
- `CdcMetricsService` - StreamMetrics (streaming metrics at configurable interval)

## Key Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_URL` | — | PostgreSQL connection string |
| `SOURCE_TYPE` | `postgres` | Source connector type |
| `SINK_URL` | — | StarRocks FE HTTP URL |
| `SINK_TYPE` | `starrocks` | Sink connector type |
| `FLUSH_SIZE` | `10000` | Max events per batch |
| `FLUSH_INTERVAL_MS` | `5000` | Max ms before flushing |
| `GRPC_PORT` | `50051` | gRPC server port |
| `HTTP_API_PORT` | `8080` | HTTP API port |
| `DO_SNAPSHOT` | `false` | Enable initial snapshot |
| `SNAPSHOT_CHUNK_SIZE` | `50000` | Rows per snapshot chunk |
| `INITIAL_SNAPSHOT_ONLY` | `false` | Exit after snapshot (no CDC) |

## Build & Test

```bash
cargo build --release    # Build
cargo test               # Test
cargo fmt -- --check     # Format check
cargo clippy -- -D warnings  # Lint
```

## Review Rules

These are patterns that have caused real bugs or are critical for data integrity. Flag them with HIGH confidence.

### Data Integrity
- LSN (Log Sequence Number) tracking MUST be accurate. `confirmed_lsn` should only advance AFTER data is successfully committed to StarRocks. Premature confirmation = data loss.
- Stream Load responses MUST be fully checked. StarRocks returns HTTP 200 even for partial failures - always check the response body JSON for `Status` field.
- Replication slot management: NEVER drop a replication slot without confirming the consumer is done. Lost slots = re-snapshot required.

### PostgreSQL Replication
- WAL parser MUST handle all pgoutput message types: Begin, Commit, Relation, Insert, Update, Delete, Truncate, Type, Origin. Unknown types should be logged and skipped, not panic.
- Column type mapping between PostgreSQL and StarRocks MUST be exhaustive. Unmapped types should produce a clear error, not silently drop data.
- `keepalive` messages MUST be responded to promptly to prevent replication slot disconnection.

### StarRocks Stream Load
- Stream Load MUST use `partial_update` mode where appropriate to avoid overwriting unchanged columns.
- Connection timeouts MUST be set on all HTTP calls to StarRocks. Hanging connections block the entire pipeline.
- CSV escaping MUST handle: commas, quotes, newlines, null bytes, and binary data in text columns.

### Credential Safety
- Database credentials come from environment variables or gRPC config. NEVER log connection strings, passwords, or auth tokens.
- Structs containing `password` fields MUST NOT use `#[derive(Debug)]` without redacting sensitive fields.

### Error Handling
- Use `tracing` (info!, warn!, error!) for all logging. NEVER use `println!` or `eprintln!` in production code.
- Transient errors (network, timeout) should be retried with backoff. Permanent errors (schema mismatch, auth failure) should be reported and stop the pipeline.
- `unwrap()` MUST NOT appear in production code paths. Use `?` or explicit error handling.

### Rust Patterns
- Prefer `?` over `match` for error propagation. Use `.context("msg")` or `.with_context(|| format!(...))` from anyhow for meaningful error chains.
- Use `Arc<T>` for shared ownership across tasks. Use `Arc<RwLock<T>>` only when mutation is needed — prefer atomics (`AtomicU64`, `AtomicBool`) for counters and flags.
- Async: always use `tokio::select!` with `biased;` when one branch is a cancellation signal. Put the cancellation arm first.
- Never block the tokio runtime — use `spawn_blocking` for CPU-heavy or synchronous I/O work.
- For hot-path serialization (Stream Load, WAL parsing), prefer byte-level operations over `serde_json` — allocate with `Vec::with_capacity()` and use `extend_from_slice`.
- Use `#[cfg(test)]` modules for unit tests, keep them in the same file as the code they test.
- Prefer `tracing::instrument` on async functions for automatic span creation. Use `skip(self)` to avoid logging large structs.
