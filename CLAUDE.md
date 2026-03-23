# CLAUDE.md - DBMazz CDC Daemon

## Project Overview

Rust CDC (Change Data Capture) daemon. Reads PostgreSQL WAL (Write-Ahead Log) via logical replication and streams changes to any supported sink. Each instance handles one replication job.

Supported sinks: StarRocks (stable), PostgreSQL (in development).

## Architecture

```
dbmazz (single binary, tokio async runtime)
├── Source: PostgreSQL logical replication
│   ├── WAL message parsing (pgoutput protocol)
│   ├── source/converter.rs: CdcMessage → CdcRecord (generic boundary)
│   ├── Replication slot + publication management
│   └── LSN tracking (current_lsn, confirmed_lsn)
├── Pipeline (generic — only sees CdcRecord, never pgoutput)
│   ├── Batching (FLUSH_SIZE / FLUSH_INTERVAL_MS)
│   ├── Calls sink.write_batch(Vec<CdcRecord>)
│   └── Checkpoint feedback (LSN confirmation)
├── Sink trait (6 methods — see ARCHITECTURE.md)
│   ├── StarRocksSink: JSON → Stream Load HTTP API
│   └── (future: PostgresSink, SnowflakeSink, etc.)
├── Snapshot (Flink CDC concurrent snapshot, uses same write_batch)
└── gRPC server
    ├── Health, Control, Status, Metrics services
    └── SharedState (metrics, dedup, control signals)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full data flow, module map, and design decisions.

## Key Directories

- `src/core/` - Generic abstractions: CdcRecord, Value, Sink trait, SourceTableSchema
- `src/source/` - PostgreSQL source layer
  - `converter.rs` - CdcMessage → CdcRecord conversion (the PG↔generic boundary)
  - `parser.rs` - pgoutput protocol parser
  - `postgres.rs` - Replication connection
- `src/pipeline/` - Generic batching + dispatch to sink
  - `schema_cache.rs` - Table schema tracking (used by converter)
- `src/connectors/sinks/` - Sink implementations
  - `mod.rs` - `create_sink()` factory
  - `starrocks/` - StarRocks: Stream Load HTTP API
- `src/engine/` - Orchestration (setup → CDC → snapshot → shutdown)
  - `snapshot/` - Flink CDC concurrent snapshot (PK-range chunking, watermarks, dedup)
  - `setup/` - Source + sink setup (conditional by SinkType)
- `src/replication/` - WAL handler (parse, convert, dedup, send to pipeline)
- `src/grpc/` - gRPC server (SharedState, services)
- `src/config.rs` - Configuration from environment variables
- `src/state_store.rs` - LSN checkpoint persistence

## Feature Flags

- `--features http-api` - Enables HTTP API + web UI on port 8080 (setup wizard, dashboard, REST endpoints)
- `--features demo` - Enables demo mode with sample data generation

## Snapshot / Backfill

Set `DO_SNAPSHOT=true` for initial data backfill. Uses Flink CDC concurrent snapshot algorithm:
1. Chunks table by PK ranges (`SNAPSHOT_CHUNK_SIZE`, default 50000 rows)
2. For each chunk: low-watermark → SELECT → high-watermark → sink.write_batch()
3. WAL consumer checks `should_emit()` to suppress duplicate events within completed chunks
4. Progress tracked in `dbmazz_snapshot_state` table (resumable)
5. N parallel workers, each with its own PG connection and sink instance

Can also be triggered on-demand via gRPC: `CdcControlService/StartSnapshot`

## Adding a New Sink

```
1. Create src/connectors/sinks/my_sink/
   ├── mod.rs      Implement Sink trait (6 methods)
   └── config.rs   MySinkConfig

2. Add SinkType::MySink to src/config.rs

3. Add match arm in create_sink() (src/connectors/sinks/mod.rs)

4. Add match arm in SetupManager (src/engine/setup/mod.rs)

Done. CDC and snapshot work automatically via write_batch().
```

## Sink Trait

```rust
trait Sink: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> SinkCapabilities;
    async fn validate_connection(&self) -> Result<()>;
    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> { Ok(()) }
    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult>;
    async fn close(&mut self) -> Result<()>;  // flush + cleanup
}
```

6 methods, 1 with default. Modeled after Kafka Connect. The sink is fully responsible for its loading strategy — the engine doesn't know about Stream Load, COPY protocol, S3 staging, etc. Snapshot and CDC both use `write_batch()`.

## Key Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_URL` | — | PostgreSQL connection string |
| `SOURCE_TYPE` | `postgres` | Source connector type |
| `SINK_URL` | — | Sink connection URL |
| `SINK_TYPE` | `starrocks` | Sink connector type (starrocks) |
| `SINK_DATABASE` | — | Target database name |
| `FLUSH_SIZE` | `10000` | Max events per batch |
| `FLUSH_INTERVAL_MS` | `5000` | Max ms before flushing |
| `GRPC_PORT` | `50051` | gRPC server port |
| `DO_SNAPSHOT` | `false` | Enable initial snapshot |
| `SNAPSHOT_CHUNK_SIZE` | `50000` | Rows per snapshot chunk |
| `INITIAL_SNAPSHOT_ONLY` | `false` | Exit after snapshot (no CDC) |

## gRPC Services

- `HealthService` - Health check
- `CdcControlService` - Pause/Resume/StartSnapshot/DrainStop
- `CdcStatusService` - GetStatus (LSN, events, snapshot progress)
- `CdcMetricsService` - StreamMetrics (streaming metrics at configurable interval)

## Versioning & Release

Format: `vMAJOR.MINOR.PATCH` (e.g., `v1.3.2`). Tags only on `main`.

| Bump | When |
|------|------|
| **MAJOR** | Breaking changes in checkpoint/state_store format, breaking gRPC changes, incompatible config changes |
| **MINOR** | New sink connector types, new PG type support, new features, new gRPC methods (additive) |
| **PATCH** | Bug fixes, type mapping fixes, performance improvements, dependency updates |

### Branching

- `main` is trunk — always deployable, all work via PR
- Tags from `main`: `v1.3.0`
- Hotfix: branch `release/v1.3.x` from tag → cherry-pick fix → tag `v1.3.1`

### Deploy

- Binaries uploaded to S3/GCS: `releases/dbmazz/vX.Y.Z/dbmazz-linux-amd64`
- Rollout via canary tiers: `canary` → `early_adopter` → `stable`
- Worker-agent manages dbmazz lifecycle; version comes from `daemon_versions` table

## Build & Test

```bash
cargo build --release                    # Build
cargo build --release --features http-api # With web UI
cargo test                               # Test
cargo fmt -- --check                     # Format check
cargo clippy -- -D warnings              # Lint
```

## Review Rules

These are patterns that have caused real bugs or are critical for data integrity.

### Data Integrity
- LSN tracking MUST be accurate. `confirmed_lsn` only advances AFTER data is committed to the sink. Premature confirmation = data loss.
- Checkpoint saved BEFORE confirming to PostgreSQL. Always. See `handle_checkpoint_feedback()` in engine.
- Replication slot: NEVER drop without confirming the consumer is done. Lost slots = re-snapshot required.
- Sink responses MUST be fully checked. StarRocks returns HTTP 200 even for partial failures.

### Pipeline & Sink
- Pipeline only sees `CdcRecord` — never `CdcMessage` or pgoutput types.
- Conversion happens in `source/converter.rs`, not in the sink or pipeline.
- Sink implementations are self-contained. Internal strategy (raw table, staging, etc.) is not exposed via the trait.
- `write_batch()` handles both CDC and snapshot records — no separate snapshot path.

### PostgreSQL Replication
- WAL parser MUST handle all pgoutput message types. Unknown types: log and skip, never panic.
- `keepalive` messages MUST be responded to promptly to prevent replication slot disconnection.
- Column type mapping MUST be exhaustive. Unmapped types → clear error, never silent data drop.

### Credential Safety
- NEVER log connection strings, passwords, or auth tokens.
- Structs with `password` fields MUST NOT use `#[derive(Debug)]` without redacting.

### Error Handling
- Use `tracing` for all logging. NEVER `println!` or `eprintln!`.
- Transient errors → retry with backoff. Permanent errors → report and stop pipeline.
- `unwrap()` MUST NOT appear in production code paths.

### Rust Patterns
- Prefer `?` over `match` for error propagation. Use `.context()` from anyhow.
- `Arc<T>` for shared ownership. Prefer atomics over `RwLock` for counters/flags.
- Async: `tokio::select!` with `biased;` when one branch is a cancellation signal.
- Never block the tokio runtime — use `spawn_blocking` for CPU-heavy work.
- Hot-path serialization: byte-level operations, `Vec::with_capacity()`, `extend_from_slice`.
