# CLAUDE.md — dbmazz

Guidance for working on the dbmazz codebase.

## Project Overview

dbmazz is a Rust-based CDC (Change Data Capture) daemon. It reads PostgreSQL WAL via logical replication and streams changes to any supported sink. Each instance handles one replication job.

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
└── Snapshot (Flink CDC concurrent snapshot, uses same write_batch)
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
- `src/config.rs` - Configuration from environment variables
- `src/state_store.rs` - LSN checkpoint persistence

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

Full env var reference: [docs/configuration.md](docs/configuration.md).

## Git Rules

- **NEVER** add `Co-Authored-By` to commits. The only author is the user.
- **NEVER** add "Generated with Claude Code" or similar footers to PRs, commits, or code.
- Don't use `--no-verify` or `--force` without explicit authorization.
- **NEVER** commit or push directly to `main`. All changes go through a feature branch + PR.

### Branching Workflow

```
1. git checkout -b <type>/<short-description>   # Create branch from main
2. (make changes, commits)
3. git push -u origin <branch>
4. gh pr create --title "..." --body "..."
```

Branch names: `<type>/<short-description>` where type is `feat|fix|refactor|chore|docs|perf`.

Examples: `feat/new-sink`, `fix/handle-timeout`, `chore/update-deps`.

### Conventional Commits

All commits MUST use:

```
<type>: <description>
```

| Type | When to use |
|------|------------|
| `feat` | New functionality |
| `fix` | Bug fix |
| `refactor` | Restructuring without behavior change |
| `perf` | Performance improvement |
| `docs` | Documentation |
| `test` | Adding or modifying tests |
| `chore` | Maintenance (deps, CI, cleanup) |
| `security` | Security fix |

Rules:
- Description in English, imperative, lowercase after the type
- First line < 72 characters
- Extra context goes in the body
- Don't use scope in parentheses

## Build & Test

```bash
cargo build --release
cargo test
cargo fmt -- --check
cargo clippy -- -D warnings
```

Requires Rust 1.91.1+.

## Code Conventions

- **Format**: always run `cargo fmt` before committing
- **Lint**: `cargo clippy -- -D warnings` with no warnings
- **Naming**: `snake_case` for functions/modules, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants
- **Errors**: prefer `?` with `anyhow::Context` over `match`. No `unwrap()` in production code paths.
- **Logging**: use `tracing`. Never `println!` or `eprintln!`.
- **Async**: use tokio. Never block the runtime — use `spawn_blocking` for CPU-heavy work.
- **Credentials**: NEVER log connection strings, passwords, or auth tokens. Structs with sensitive fields MUST NOT derive `Debug` without redacting.

## Versioning

dbmazz uses Semantic Versioning: `vMAJOR.MINOR.PATCH`. Tags only on `main`.

- **MAJOR**: breaking changes
- **MINOR**: new features (backward-compatible)
- **PATCH**: bug fixes, dependency updates

## Changelog Discipline

Every change that affects user-visible behavior MUST update `CHANGELOG.md` under the `[Unreleased]` section in the same PR. Follow [Keep a Changelog](https://keepachangelog.com/) format.

What counts as user-visible:
- New features, new sinks, new sources, new env vars
- Bug fixes that change observable behavior
- Performance improvements with measurable impact
- Breaking changes
- Security fixes

What does NOT count:
- Internal refactors with no behavior change
- Test additions
- README/docs reorganisation
- CI/build pipeline changes that don't affect what gets shipped

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

## License

dbmazz is licensed under the Elastic License v2.0. Contributions are accepted under the same license.
