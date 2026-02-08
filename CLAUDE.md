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
- `src/pipeline/` - Data pipeline (schema cache, transformation)
- `src/core/` - Core abstractions (Record, Position, traits, errors)
- `src/grpc/` - gRPC server (state, services, metrics)
- `src/config.rs` - Configuration from environment variables
- `src/source/` - Source abstraction layer
- `src/sink/` - Sink abstraction layer

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
