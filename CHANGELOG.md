# Changelog

All notable changes to dbmazz will be documented here.

## [Unreleased]

### Fixed
- **PostgreSQL sink: last-write-wins on multiple operations for the same
  PK inside a single batch.** The raw-table writer stamped every record in
  a batch with the same `_timestamp`, and the normalizer MERGE uses
  `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _timestamp DESC)` to pick
  the surviving row per PK. When two UPDATEs on the same row landed in
  one batch (e.g. `BEGIN; UPDATE; UPDATE; COMMIT;`), the tiebreaker was
  effectively random and the non-final write could win, silently
  corrupting the target. Records now carry a monotonic per-row offset
  (`batch_base + rows_written`) so the MERGE deterministically selects
  the last write within the batch.

## [1.6.8] - 2026-04-14

### Changed
- **Release version is now driven by `Cargo.toml`** instead of being
  calculated from conventional commits. To cut a release, bump
  `[package].version` in `Cargo.toml` in the PR that should ship (and
  regenerate `Cargo.lock` with `cargo check`). If `Cargo.toml` still
  matches the latest tag on merge, the release workflow cleanly no-ops
  and the job summary lists the accumulated unreleased commits so they
  don't hide. Previously every merge with any conventional-commit type
  produced a release, including `docs:` and `chore:` PRs, which caused
  patch-bump noise (tags `v1.6.4`–`v1.6.7` were all no-op docs/chore
  releases).
- **`gh release create` now creates the git tag atomically** via
  `--target $GITHUB_SHA`, as the final mutation of the release job.
  Previously the tag was pushed in a separate step before the release
  was created; a workflow abort between those two steps left an orphan
  tag and any re-run on the same SHA would skip the release because the
  tag already existed. Re-runs now retry cleanly.
- **Release build now uses `cargo build --release --locked`** to catch
  `Cargo.lock` drift deterministically instead of silently regenerating.

### Added
- **Monotonicity guard** in the release workflow: a `Cargo.toml` version
  that equals or precedes the latest git tag fails the workflow with a
  clear error, preventing accidental backwards or sideways bumps.
- **Pre-release detection from the version string** (`-rc`, `-alpha`,
  `-beta`, `-pre`). A version like `1.7.0-rc.1` is now correctly flagged
  as a prerelease, and the Docker publish step skips the mutable
  `latest`/`X`/`X.Y` tags for prereleases.

### Removed
- The `ez-cdc` CLI source code (`e2e-cli/`) and the `install.sh` installer
  have been moved out of this repository. The CLI binary remains publicly
  installable via the same kind of one-liner, now hosted at:
  `curl -sSL https://raw.githubusercontent.com/ez-cdc/ez-cdc-cli-releases/main/install.sh | sh`
- The `build-ez-cdc` CI job has been removed from the release workflow;
  dbmazz releases now only build and publish the daemon binaries and the
  Docker image. CLI binaries are produced and published from a separate
  repository.
- **Conventional-commit-based version calculation** in the release workflow
  (superseded by the Cargo.toml-driven model above), along with the
  `Sync dbmazz version with release version` build-time sed patch and the
  `bump` output that used to propagate the computed bump type through the
  tag annotation and the workflow summary.

## [1.6.3] - 2026-04-13

### Fixed
- **Release pipeline now patches the root `Cargo.toml`** with the calculated release version before compiling `dbmazz`, mirroring the existing behavior for `e2e-cli/Cargo.toml`. Previously the dbmazz binary always reported `1.4.4` (the last manually-bumped version) regardless of the release tag, causing a drift between `git tag`, the GHCR Docker image tag, and `CARGO_PKG_VERSION` baked into the binary.
- **Synced both `Cargo.toml` files to `1.6.3`** to align source-of-truth with the latest published release tag (`v1.6.2`). After this PR, every release patches the version at build time, so manual sync is no longer required.

### Added
- **Official Docker image on GHCR** (`ghcr.io/ez-cdc/dbmazz`)
  - Multi-arch manifest with `linux/amd64` and `linux/arm64`
  - Base image: `debian:bookworm-slim`, runs as non-root (UID 65532)
  - Published on every release with tags `X.Y.Z`, `X.Y`, `X`, `latest`, and `sha-<short>`
  - Built from pre-compiled musl-static binaries in the release workflow
- **`install.sh` one-liner installer** for the `ez-cdc` CLI
  - `curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh`
  - Detects OS (Linux/Darwin) and arch (amd64/arm64), verifies SHA256 against `SHA256SUMS`
  - Honors `EZ_CDC_VERSION` and `EZ_CDC_INSTALL_DIR` env var overrides
  - POSIX sh, shellcheck-clean, no dependencies beyond `curl` and `uname`
- **linux/arm64 release binaries** for both `dbmazz` and `ez-cdc` CLI
  - S3/GCS now host `dbmazz-linux-arm64` alongside `dbmazz-linux-amd64` at the same paths
  - GitHub release assets include all 4 CLI targets (linux amd64/arm64 musl, darwin amd64/arm64)
- **`docs/production-deployment.md`** — full self-host deployment guide
  - `docker run`, Docker Compose, AWS ECS Fargate examples
  - Secrets management (env_file, Secrets Manager, SSM)
  - Prometheus monitoring setup and alerting metrics
  - Operations guide (pause/resume/drain-stop/upgrade)
  - Troubleshooting and "when to graduate to EZ-CDC Cloud" section

### Changed
- **`http-api` is now a default Cargo feature.** A single `cargo build --release` builds a binary that serves both enterprise (gRPC) and self-host (HTTP API on port 8080 — health check, status JSON, Prometheus metrics, pause/resume/drain-stop). For an extra-minimal build without the HTTP API, use `cargo build --release --no-default-features --features "sink-starrocks,sink-postgres,sink-snowflake,grpc-reflection"`.
- **Release workflow restructured into 5 jobs**: `version`, `build-dbmazz` (matrix amd64/arm64), `build-ez-cdc` (matrix 4 targets, now all musl for Linux), `docker-publish`, and `release`. The git tag and GitHub Release are only created after `docker-publish` succeeds, so a failed image push does not produce orphan tags.
- **`ez-cdc` CLI pulls the dbmazz image from GHCR** instead of cross-compiling the daemon and mounting it as a volume. The image reference is pinned to the CLI's own `CARGO_PKG_VERSION` (kept in sync at build time by the release workflow) and overridable via `DBMAZZ_IMAGE` for local-dev testing of patched daemons.
- **CLI Linux targets migrated from `gnu` to `musl`** for portability across distributions.
- **`gh release create` no longer shell-interpolates the changelog**, protecting against commit messages containing quotes, backticks, or command substitutions.
- **Docker tags now generated via `docker/metadata-action@v5`** instead of shell-ad-hoc computation.

### Removed
- **`e2e-cli/Dockerfile.runtime`** and the cross-compile + bind-mount development loop. The CLI now uses the official image.
- **`e2e-cli/bin/dbmazz-linux-amd64`** (gitignored, was a local artifact). No longer consumed by any code path.
- **`check_linux_binary()`, `build_dbmazz_image()`, and the `DBMAZZ_IMAGE` constant** in `e2e-cli/src/commands/mod.rs`. Replaced by `dbmazz_image()` fn, `pull_dbmazz_image()`, and a redesigned `ensure_dbmazz_image()` that pulls instead of builds.
- **`paths::LINUX_BINARY`** static in `e2e-cli/src/paths.rs`.

- **Snowflake Sink Connector**: Two-phase ELT replication to Snowflake via HTTPS
  - Parquet files for type-safe bulk loading (Arrow + Parquet crates)
  - PUT protocol for stage upload (S3/GCS/Azure via Snowflake temp credentials)
  - COPY INTO with PARSE_JSON → VARIANT (parsed once, not per-MERGE)
  - Background normalizer with MERGE (ROW_NUMBER dedup, VARIANT extraction, TOAST handling)
  - File accumulation for snapshot optimization (20 files per COPY INTO)
  - Configurable soft delete (`SINK_SNOWFLAKE_SOFT_DELETE`) or hard DELETE
  - Key-pair JWT auth (preferred) or username/password fallback
  - TRANSIENT schema for raw table (no fail-safe, cheaper storage)
  - Schema evolution via ALTER TABLE ADD COLUMN
  - 29 unit tests covering types, Parquet, MERGE SQL, stage parsing, config
- **Automatic PostgreSQL Setup**: Zero configuration, `dbmazz` configures everything automatically
  - Verifies that tables exist
  - Configures `REPLICA IDENTITY FULL` automatically
  - Creates/verifies Publication and Replication Slot
  - Recovery mode: detects existing resources after crashes
- **Automatic StarRocks Setup**: Audit columns added automatically
  - `dbmazz_op_type`, `dbmazz_is_deleted`, `dbmazz_synced_at`, `dbmazz_cdc_version`
  - Validates connectivity and table existence
  - Idempotent: detects existing columns
- **Improved Error Handling**: Descriptive messages for the control plane
  - New `error_detail` field in Health Check
  - `status: NOT_SERVING` when there are setup errors
  - gRPC server keeps running for queries even with errors
- **CPU Metrics in Millicores**: Consistent CPU consumption monitoring across environments
  - New `cpu_millicores` field in `MetricsResponse`
  - Direct reading from `/proc/[pid]/stat` (same algorithm as `ps` and `top`)
  - Consistent across bare metal, Docker and Kubernetes
  - 1000 millicores = 100% of 1 core (Kubernetes standard)
  - Validated: 3000 ev/s → 45 millicores (4.5% of 1 core)
  - Efficiency: 66 events/millicore on bare metal
- **gRPC Reflection**: gRPC server with reflection enabled for simple use of `grpcurl` without `.proto` files
- **Basic Schema Evolution**: Automatic detection of new columns and `ALTER TABLE ADD COLUMN` in StarRocks

### Changed
- Migration from `reqwest` to `curl` crate (libcurl bindings) for StarRocks Stream Load
  - Correct handling of `Expect: 100-continue` protocol
  - Native support for FE → BE redirects with authentication
- **Smart Redirect for Autoscaling**: Stream Load supports StarRocks FE with autoscaling
  - Connection to Frontend (port 8030) instead of direct Backend
  - Automatic detection of HTTP 307 redirects to `127.0.0.1`
  - Hostname rewriting for Docker/Kubernetes compatibility
  - Full support for load balancing and failover across multiple BEs
  - Works on bare metal, Docker Compose and Kubernetes without additional configuration
  - **Note**: Current implementation has not been validated against libcurl best practices (TODO: benchmarks, connection pooling)
- **CPU Optimization**: Reduction of main loop overhead
  - Migration from `RwLock<CdcState>` to `AtomicU8` for lock-free access
  - State checks reduced from every iteration to every 256 iterations
  - Zero-copy in WAL parsing (`bytes.slice(..)` instead of `clone()`)
  - Pre-allocation of JSON structures with known capacity
- **BREAKING**: PostgreSQL manual configuration is no longer required
  - Publication, Slot and REPLICA IDENTITY are now automatic
  - Simplifies deployment: just specify the tables

### Fixed
- Clarification of TOAST behavior:
  - INSERTs always receive complete data (even > 2KB)
  - Only UPDATEs that don't modify TOAST column send 'u' marker
  - Partial Update preserves existing values in StarRocks
- Docker compatibility: FE→BE redirects pointing to `127.0.0.1` are now rewritten to the correct hostname

---

## [1.4.4] - 2026-04-09
- docs: consolidate architecture docs under docs/ and remove stale version

## [1.4.3] - 2026-04-09
- chore: remove 5 unused dependencies from Cargo.toml

## [1.4.2] - 2026-04-09
- chore: commit Cargo.lock and declare MSRV (1.91.1)

## [1.4.1] - 2026-04-09
- docs: update CLAUDE.md and ARCHITECTURE.md to reflect current sink implementations

## [1.4.0] - 2026-04-09
- feat: add end-to-end testing profile for Snowflake sink
- refactor: rename quickstart profile to starrocks

## [1.3.0] - 2026-04-09
- feat: Snowflake sink connector (Parquet, PUT, COPY INTO, MERGE)
- fix: resolve end-to-end replication bugs in Snowflake sink

## [1.2.7] - 2026-04-09
- chore: strip debug symbols from release binary in Dockerfile

## [1.2.6] - 2026-04-03
- refactor: simplify CdcEngine by removing Option fields and deduplicating logic

## [1.2.5] - 2026-04-02
- refactor: replace SinkConfig Option fields with enum and remove legacy Config fields

## [1.2.4] - 2026-04-02
- refactor: unify sink factory with SinkMode enum

## [1.2.3] - 2026-04-02
- refactor: move sink setup into Sink::setup() trait method

## [1.2.2] - 2026-04-01
- fix: resolve PostgreSQL sink normalizer failures

## [1.2.1] - 2026-03-23
- docs: add PostgreSQL sink connector README

## [1.2.0] - 2026-03-23
- feat: add PostgreSQL sink connector (COPY + raw table + MERGE)
- refactor: multi-sink architecture

## [1.1.4] - 2026-03-13
- fix: replace expect() with error handling, add SQL validation
- refactor: remove dead code from incomplete connector migration

## [1.1.3] - 2026-03-12
- fix: move snapshot pause check inside spawned task

## [1.1.2] - 2026-03-12
- chore: unify release workflow

## [1.1.1] - 2026-03-12
- chore: unify release and auto-tag into single workflow

## [1.1.0] - 2026-03-12
- feat: add snapshot pause/resume gRPC controls and execution window support

---

## [1.0.0] - 2025-12-11

### Main Features

#### High-Performance CDC
- Native PostgreSQL → StarRocks replication using `pgoutput` protocol
- Zero-copy parsing with `bytes::Bytes`
- SIMD optimizations (`memchr`, `simdutf8`, `sonic-rs`)
- Throughput: 300K+ events processed without degradation

#### gRPC API for Remote Control
- **HealthService**: Health check with lifecycle stages (INIT → SETUP → CDC)
- **CdcControlService**: Pause, Resume, DrainAndStop, Stop, ReloadConfig
- **CdcStatusService**: Current state (LSN, tables, pending events)
- **CdcMetricsService**: Real-time metrics stream

#### Lifecycle Stages
- `STAGE_INIT`: Initializing
- `STAGE_SETUP`: Connecting source/sink, validating tables
- `STAGE_CDC`: Actively replicating
- Allows control plane to monitor initialization progress

#### TOAST Support (Large Columns)
- Automatic detection of TOAST columns with 64-bit bitmap
- StarRocks Partial Update to preserve large values without sending them
- SIMD optimizations (POPCNT, CTZ) for column tracking
- Supports JSONs up to 10MB without data loss

#### Robust Checkpointing
- LSN persistence in PostgreSQL table `dbmazz_checkpoints`
- Automatic recovery from last checkpoint
- Confirmation to PostgreSQL via `StandbyStatusUpdate`
- "At-least-once" delivery guarantee

#### CDC Auditing
- Automatic columns in StarRocks:
  - `dbmazz_op_type`: 0=INSERT, 1=UPDATE, 2=DELETE
  - `dbmazz_is_deleted`: Soft delete flag
  - `dbmazz_synced_at`: Synchronization timestamp
  - `dbmazz_cdc_version`: PostgreSQL LSN

#### Modular Architecture
- Refactoring: `main.rs` from 284 → 28 lines (-90%)
- Separate modules: `config`, `engine`, `replication`, `grpc`
- Testable and maintainable code

### Performance

| Metric | Value |
|---------|-------|
| Throughput | 300K+ events |
| CPU | ~25% under load (287 eps) |
| Memory | ~5MB in use |
| Lag | <1KB under normal conditions |
| Replication latency | <5 seconds p99 |

### Technical Optimizations

#### JSON Serialization
- Migration from `serde_json` → `sonic-rs`
- Use of SIMD for ultra-fast JSON parsing
- 85% reduction in lag under high load

#### Connection Pooling
- HTTP connection reuse to StarRocks
- `pool_max_idle_per_host: 10`
- `tcp_keepalive: 60s`

#### Configurable Batching
- `FLUSH_SIZE`: Events per batch (default: 10000)
- `FLUSH_INTERVAL_MS`: Maximum interval between flushes (default: 5000ms)
- Adjustable via gRPC `ReloadConfig`

### Commercial Demo

#### Features
- Setup in 1 command: `./demo-start.sh`
- PostgreSQL + StarRocks in Docker
- 3 e-commerce tables: `orders`, `order_items`, `toast_test`
- Configurable traffic generator (up to 3000+ eps)
- TOAST generator to test large columns
- Real-time TUI dashboard with dynamic metrics
- Automatic cleanup for clean demos

#### Visible Metrics
- PostgreSQL vs StarRocks counts
- Deleted records (soft deletes)
- Replication lag in seconds
- Current LSN
- Last synchronization

### Validations

#### REPLICA IDENTITY FULL
- Automatic validation on startup
- Warning if not configured correctly
- Required for soft deletes in analytical databases

### Configuration

#### Environment Variables
- `DATABASE_URL`: PostgreSQL connection with `?replication=database`
- `SLOT_NAME`: Replication slot name (default: `dbmazz_slot`)
- `PUBLICATION_NAME`: Publication name (default: `dbmazz_pub`)
- `TABLES`: Comma-separated list of tables (default: `orders,order_items`)
- `STARROCKS_URL`: StarRocks Backend URL
- `STARROCKS_DB`: Target database
- `STARROCKS_USER`: User (default: `root`)
- `STARROCKS_PASS`: Password (default: empty)
- `FLUSH_SIZE`: Events per batch (default: 10000)
- `FLUSH_INTERVAL_MS`: Flush interval (default: 5000)
- `GRPC_PORT`: gRPC port (default: 50051)

### Main Dependencies

- `tokio-postgres` (Materialize fork): Logical replication
- `tonic` + `prost`: gRPC server
- `sonic-rs`: JSON with SIMD
- `curl`: libcurl bindings for Stream Load
- `mysql_async`: MySQL client for DDL in StarRocks
- `hashbrown`: High-performance HashMap
- `memchr` + `simdutf8`: SIMD optimizations
- `libc`: Access to Linux syscalls for CPU metrics

### Project Structure

```
dbmazz/
├── src/
│   ├── main.rs              # Entry point (28 lines)
│   ├── config.rs            # Configuration from env vars
│   ├── engine/              # CDC engine and automatic setup
│   │   ├── mod.rs           # Main orchestrator
│   │   └── setup/           # Automatic PG/SR configuration
│   ├── grpc/                # gRPC API (4 services)
│   │   ├── services.rs      # Service implementation
│   │   ├── state.rs         # Shared state (AtomicU8)
│   │   └── cpu_metrics.rs   # CPU tracker (/proc)
│   ├── replication/         # WAL processing
│   ├── pipeline/            # Batching and schema cache
│   ├── sink/                # Destinations (StarRocks)
│   ├── source/              # Sources (PostgreSQL)
│   └── state_store.rs       # Checkpointing
├── examples/                # Demo environment
└── CHANGELOG.md             # This file
```

### Testing

- ✅ Compilation: No errors
- ✅ Replication: 100% of data (20K+ records)
- ✅ Checkpoints: Persisting correctly
- ✅ gRPC API: 16/16 tests passed
- ✅ TOAST: 90+ events with partial updates
- ✅ Performance: No degradation under load

---

## Roadmap

### v0.2.0 (Planned)
- [ ] Prometheus metrics endpoint
- [ ] Additional sinks: Kafka, ClickHouse
- [ ] YAML configuration (in addition to env vars)
- [ ] Initial snapshot (before CDC)
- [ ] Complete unit tests

### v0.3.0 (Future)
- [ ] Multi-tenant: multiple sources → multiple destinations
- [ ] Integrated alerting (Slack, PagerDuty)
- [x] Automatic schema evolution (partial: adding columns works, pending type changes and deletion)
- [ ] Payload compression
