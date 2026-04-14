# CLAUDE.md - DBMazz CDC Daemon

## Project Overview

Rust CDC (Change Data Capture) daemon. Reads PostgreSQL WAL (Write-Ahead Log) via logical replication and streams changes to any supported sink. Each instance handles one replication job.

Supported sinks: StarRocks, PostgreSQL, Snowflake.

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
├── Sink trait (6 methods — see docs/architecture.md)
│   ├── StarRocksSink: JSON → Stream Load HTTP API
│   ├── PostgresSink: COPY → raw table → MERGE
│   └── SnowflakeSink: Parquet → PUT (stage) → COPY INTO → MERGE
├── Snapshot (Flink CDC concurrent snapshot, uses same write_batch)
└── gRPC server
    ├── Health, Control, Status, Metrics services
    └── SharedState (metrics, dedup, control signals)
```

See [docs/architecture.md](docs/architecture.md) for the full data flow, module map, and design decisions.

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
  - `postgres/` - PostgreSQL: COPY → raw table → MERGE normalizer
  - `snowflake/` - Snowflake: Parquet → PUT (stage) → COPY INTO → MERGE normalizer
- `src/engine/` - Orchestration (setup → CDC → snapshot → shutdown)
  - `snapshot/` - Flink CDC concurrent snapshot (PK-range chunking, watermarks, dedup)
  - `setup/` - Source + sink setup (conditional by SinkType)
- `src/replication/` - WAL handler (parse, convert, dedup, send to pipeline)
- `src/grpc/` - gRPC server (SharedState, services)
- `src/config.rs` - Configuration from environment variables
- `src/state_store.rs` - LSN checkpoint persistence

## Feature Flags

Default features: `sink-starrocks`, `sink-postgres`, `sink-snowflake`,
`grpc-reflection`, **`http-api`**. `http-api` is now part of the default
set so a single build serves both enterprise (gRPC) and self-host (HTTP
API on port 8080 — health check, Prometheus metrics, control endpoints).

For an extra-minimal build without the HTTP API:

```bash
cargo build --release --no-default-features \
  --features "sink-starrocks,sink-postgres,sink-snowflake,grpc-reflection"
```

## Snapshot / Backfill

Set `DO_SNAPSHOT=true` for initial data backfill. Uses Flink CDC concurrent snapshot algorithm:
1. Chunks table by PK ranges (`SNAPSHOT_CHUNK_SIZE`, default 50000 rows)
2. For each chunk: low-watermark → SELECT → high-watermark → sink.write_batch()
3. WAL consumer checks `should_emit()` to suppress duplicate events within completed chunks
4. Progress tracked in `dbmazz_snapshot_state` table (resumable)
5. N parallel workers, each with its own PG connection and sink instance

## Adding a New Sink

```
1. Create src/connectors/sinks/my_sink/
   ├── mod.rs      Implement Sink trait (6 methods)
   └── config.rs   MySinkConfig

2. Add SinkType::MySink to src/config.rs

3. Add match arm in create_sink() (src/connectors/sinks/mod.rs)

Done. CDC and snapshot work automatically via write_batch().
Sink-specific setup goes in Sink::setup(), not in SetupManager.
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
| `SINK_TYPE` | `starrocks` | Sink connector type (starrocks, postgres, snowflake) |
| `SINK_DATABASE` | — | Target database name |
| `FLUSH_SIZE` | `10000` | Max events per batch |
| `FLUSH_INTERVAL_MS` | `5000` | Max ms before flushing |
| `DO_SNAPSHOT` | `false` | Enable initial snapshot |
| `SNAPSHOT_CHUNK_SIZE` | `50000` | Rows per snapshot chunk |
| `INITIAL_SNAPSHOT_ONLY` | `false` | Exit after snapshot (no CDC) |

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

## Changelog Discipline (NON-NEGOTIABLE)

**Every change that affects user-visible behaviour MUST update `CHANGELOG.md` under the `[Unreleased]` section in the same PR.** Do not defer it to release time. Do not assume "the release bot will pick it up". The `[Unreleased]` block must always reflect the current main branch state.

### What counts as user-visible (update CHANGELOG)

- New features, new sinks, new sources, new env vars
- New gRPC RPCs or HTTP endpoints
- Bug fixes that change observable behaviour
- Performance improvements with measurable impact
- Breaking changes (config, wire protocol, checkpoint format, schema)
- Default value changes
- Removed features, deprecated APIs
- New supported PG versions, new supported sink versions
- Security fixes (always)

### What does NOT count

- Internal refactors with no behaviour change
- Test additions
- README / docs reorganisation (unless it documents new behaviour)
- CI / build pipeline changes that don't affect what gets shipped

### Format

Follow [Keep a Changelog](https://keepachangelog.com/). The existing `CHANGELOG.md` already uses this convention:

```markdown
## [Unreleased]

### Added
- New thing X

### Changed
- Behaviour Y now does Z

### Fixed
- Bug in W

### Removed
- Deprecated function Q
```

The release workflow closes `[Unreleased]` and creates a versioned section (`## [1.6.4] - 2026-04-13`) automatically when the release tag is cut. Your job as a contributor is to keep `[Unreleased]` accurate as you go — **never wait until release time**, and never assume someone else will do it.

### When in doubt, write the entry

If you're not sure whether a change is user-visible, write the entry anyway. A noisy CHANGELOG is recoverable; a missed entry leaves users confused about what changed in a release.

## Build & Test

```bash
cargo build --release                                     # Default (all sinks + http-api)
cargo build --release --no-default-features --features "..."  # Minimal build
cargo test                                                # Unit + integration tests
cargo fmt -- --check                                      # Format check
cargo clippy -- -D warnings                               # Lint
```

The release workflow builds `dbmazz-linux-amd64` and `dbmazz-linux-arm64`
(both musl-static) and uploads them to S3/GCS (enterprise) and GitHub
releases (self-host).

## Docker Image

The official multi-arch Docker image is published to GHCR on every
release:

```bash
docker pull ghcr.io/ez-cdc/dbmazz:1.5.2   # immutable version
docker pull ghcr.io/ez-cdc/dbmazz:latest  # latest stable (avoid for prod)
```

The `Dockerfile` at the repo root uses `debian:bookworm-slim` as base,
runs as non-root (UID 65532), and copies the pre-compiled binary by
`TARGETARCH` during a `docker buildx build --platform linux/amd64,
linux/arm64`.

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
