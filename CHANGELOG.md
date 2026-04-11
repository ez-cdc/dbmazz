# Changelog

All notable changes to dbmazz will be documented here.

## [Unreleased]

### Added
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
- [ ] Web UI for monitoring
- [ ] Integrated alerting (Slack, PagerDuty)
- [x] Automatic schema evolution (partial: adding columns works, pending type changes and deletion)
- [ ] Payload compression
