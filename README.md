<div align="center">

<a href="https://github.com/ez-cdc/dbmazz" target="_blank">
  <img src="assets/ez-cdc-banner.png" alt="dbmazz — Real-time PostgreSQL CDC" width="100%">
</a>

<br>

# dbmazz

**Real-time PostgreSQL CDC. One binary. No Kafka.**

Stream PostgreSQL changes to StarRocks, Snowflake, or another PostgreSQL — via a single Rust daemon. No JVM, no Kafka, no ZooKeeper, no orchestration layer.

[![License: ELv2](https://img.shields.io/badge/license-ELv2-blue.svg)](LICENSE)
[![GHCR](https://img.shields.io/badge/ghcr.io-ez--cdc%2Fdbmazz-0969da?logo=docker)](https://github.com/ez-cdc/dbmazz/pkgs/container/dbmazz)
[![Discussions](https://img.shields.io/badge/GitHub-Discussions-181717?logo=github)](https://github.com/ez-cdc/dbmazz/discussions)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange?logo=rust)](https://www.rust-lang.org)

[Quickstart](#-try-it-in-2-minutes) · [What is dbmazz?](#what-is-dbmazz) · [How it works](#how-it-works) · [Performance](#-performance) · [Production](#-production-deployment) · [Contributing](#-contributing)

</div>

---

## What is dbmazz?

dbmazz is an open-source Change Data Capture (CDC) daemon written in Rust. It reads the PostgreSQL Write-Ahead Log via logical replication and streams every `INSERT`, `UPDATE`, and `DELETE` to a downstream system in near-real time — without requiring Kafka, Flink, ZooKeeper, or a JVM.

Each instance handles one replication job. Run it directly from the official Docker image, deploy it on ECS / Kubernetes / a bare VM, or build it from source. It's a single static binary (~30 MB) with a Prometheus endpoint, a gRPC control plane, and an HTTP API for operations.

<div align="center">
  <img src="assets/demo.svg" alt="dbmazz TUI dashboard demo" width="90%">
  <br>
  <sup><i>The <code>ez-cdc</code> CLI in <code>quickstart</code> mode — live throughput, lag, source vs target row counts.</i></sup>
</div>

---

## 🤔 Why dbmazz?

### The problem

You need to replicate PostgreSQL into an analytical warehouse, lakehouse, or another database. The incumbent tool — **Debezium** — needs Kafka, ZooKeeper, a JVM, a Connect cluster, schema registries, and a small army of YAML to wire it all together. You wanted CDC; you got a distributed system.

dbmazz is the alternative: a single Rust binary that connects to your Postgres source on one side and your sink on the other. No brokers, no clusters, no orchestration. Run it, point it at two databases, and you're replicating.

### What dbmazz is for

- **🏔️ Postgres → Lakehouse** — stream operational data into StarRocks or Snowflake for analytics, with audit columns and primary-key upserts handled automatically.
- **🔁 Postgres → Postgres** — migrate, sync, or replicate between Postgres instances using logical replication on the source and a `COPY` + `MERGE` pipeline on the target.
- **🔌 Embed CDC in your own tooling** — every dbmazz instance exposes gRPC and HTTP APIs for control and observability, so you can build higher-level systems on top.

### What dbmazz is *not* for

- **You need ten different sources.** dbmazz speaks PostgreSQL only. If you also need MySQL/Oracle/MongoDB CDC today, look at Debezium or a managed platform.
- **You need a hosted UI for non-technical users.** dbmazz is a daemon. The control surface is APIs and a TUI dashboard. If you want a web portal for your team, see the [About EZ-CDC](#-about-ez-cdc) note at the bottom.
- **You need horizontal scaling of a single job.** dbmazz scales vertically. One instance handles one replication job. Sharding across multiple instances is your responsibility.

---

## 🚀 Try it in 2 minutes

### Install the CLI

```bash
curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh
```

Linux and macOS, amd64 and arm64. The binary is downloaded from the latest GitHub release, checksum-verified, and installed to `$HOME/.local/bin/ez-cdc`.

### Spin up the demo

```bash
ez-cdc datasource init                                    # write a starter config
ez-cdc datasource add                                     # interactive wizard
ez-cdc quickstart --source demo-pg --sink demo-starrocks  # spin up + live dashboard
```

`quickstart` pulls the official `ghcr.io/ez-cdc/dbmazz` image, starts the source and sink containers, and opens the TUI shown above. Press `t` to generate live traffic, `q` to quit.

> **Requires** Docker. To use your own Postgres source instead of the demo, see the [Run with your own databases](#run-with-your-own-databases) section below.

### Run the verification suite

The CLI ships with a 13-check end-to-end test harness that validates every supported sink: schema, snapshot integrity, CDC `INSERT`/`UPDATE`/`DELETE`, TOAST handling, NULL roundtrip, and idempotency drift over time.

```bash
ez-cdc verify --source demo-pg --sink demo-starrocks           # full suite
ez-cdc verify --source demo-pg --sink demo-pg-target --quick   # skip slow checks
```

Use `--json-report results.json` to wire it into CI.

### Run with your own databases

If you don't want to use the CLI, run dbmazz directly with `docker run` and pass connection details via environment variables:

```bash
docker run -d --name dbmazz \
  -e SOURCE_URL="postgres://user:pass@host:5432/mydb?replication=database" \
  -e TABLES="orders,order_items" \
  -e SINK_TYPE="starrocks" \
  -e SINK_URL="http://starrocks-fe:8030" \
  -e SINK_DATABASE="analytics" \
  -p 8080:8080 -p 50051:50051 \
  ghcr.io/ez-cdc/dbmazz:latest
```

Full configuration reference: [`docs/configuration.md`](docs/configuration.md). Production deployment guide (Compose, ECS, secrets, monitoring): [`docs/production-deployment.md`](docs/production-deployment.md).

---

## 🔌 Supported sources & sinks

| Source | Sink | Status | Notes |
|---|---|:---:|---|
| PostgreSQL 12+ | **StarRocks** | ✅ Stable | JSON Stream Load, partial-update for TOAST columns, audit columns auto-managed |
| PostgreSQL 12+ | **PostgreSQL 15+** | ✅ Stable | Binary `COPY` → raw table → `MERGE` normalizer; supports hard delete |
| PostgreSQL 12+ | **Snowflake** | ✅ Stable | Parquet → PUT (stage) → `COPY INTO` → background `MERGE`; JWT key-pair auth supported |
| PostgreSQL 12+ | S3 / Iceberg | 🚧 Roadmap | Tracked in [issues](https://github.com/ez-cdc/dbmazz/issues) |
| MySQL | * | 🚧 Roadmap | The source layer is generic; PostgreSQL is the only implementation today |

Adding a new sink is intentionally small: implement a 6-method `Sink` trait and CDC + snapshot work automatically. See [`docs/contributing-connectors.md`](docs/contributing-connectors.md).

---

## How it works

```
PostgreSQL (source)               dbmazz                          Sink (target)
┌──────────────┐               ┌────────────────────┐          ┌──────────────┐
│  WAL         │   logical     │ WAL Handler        │          │ StarRocks    │
│  (INSERT,    │   replication │   │                │          │ PostgreSQL   │
│   UPDATE,    │ ────────────▶ │   ▼                │          │ Snowflake    │
│   DELETE)    │   (pgoutput)  │ source/converter   │          │              │
│              │               │   │                │          │              │
│              │               │   ▼                │          │              │
│              │               │ Pipeline           │  write   │              │
│              │               │   │ batch + flush  │──batch──▶│              │
│              │               │   ▼                │          │              │
│              │               │ Checkpoint (LSN)   │          │              │
│              │ ◀─────────────│  confirm to PG     │          │              │
└──────────────┘               └────────────────────┘          └──────────────┘
```

dbmazz reads PostgreSQL's logical replication stream (`pgoutput` protocol), converts each event to a generic `CdcRecord`, batches records in a small in-memory pipeline, and writes them to a sink via a 6-method `Sink` trait. The sink owns its loading strategy entirely — Stream Load for StarRocks, binary `COPY` for Postgres, Parquet staging for Snowflake — the engine doesn't care.

The critical invariant: **LSN checkpoints are persisted before being confirmed to PostgreSQL**. If we confirmed first and crashed, the WAL would be discarded with no local record — permanent data loss. dbmazz never does that.

For initial backfill, dbmazz implements the [Flink CDC concurrent snapshot algorithm](https://github.com/apache/flink-cdc): each table is divided into PK-range chunks, processed by N parallel workers, and de-duplicated against the live WAL stream via low/high watermarks emitted with `pg_logical_emit_message`. Snapshot progress is persisted in PostgreSQL, so an interrupted snapshot resumes from the last completed chunk.

Full data flow, module map, and design decisions: [`docs/architecture.md`](docs/architecture.md).

---

## 📊 Performance

We publish reproducible benchmarks with full hardware specs and methodology. Numbers below come from real runs, not synthetic projections.

### Read this first — Snapshot vs CDC

dbmazz operates in two modes with **fundamentally different resource profiles**. Don't conflate them.

- **Snapshot** (one-time backfill) is a heavy parallel workload. It runs N concurrent `SELECT` chunks over multiple PostgreSQL connections, serializes millions of rows to the sink format, and pushes them in batches. The CPU and memory you see in the snapshot benchmark below reflect this *workload* — N workers each holding a chunk in memory waiting for the sink to ACK — not steady-state daemon overhead. Snapshot runs once.

- **CDC streaming** (steady state) is an entirely different beast. dbmazz reads logical replication messages over a *single* PostgreSQL connection, batches them in a small in-memory channel, and flushes to the sink. There is no read-side parallelism because the WAL stream is sequential. Memory is bounded by the channel buffer (a few thousand events, configurable). This is what you run 99% of the time, and it costs almost nothing.

> **TL;DR**: if you see "1.7 GB RSS" in the snapshot benchmark and assume that's what dbmazz uses on your production CDC pipeline — that's the wrong takeaway. CDC steady state runs in a fraction of the resources because the mechanism is fundamentally lighter.

### Snapshot — TPC-DS 1 TB (partial run)

We published a partial TPC-DS 1 TB snapshot run alongside the source code so you can see exactly what we measured, on what hardware, and where the bottleneck is.

- **Source**: PostgreSQL 16 on RDS (`us-west-2`), gp3 storage
- **Sink**: StarRocks on EC2 (same region)
- **Worker**: c5.2xlarge — 8 vCPU, 16 GB RAM
- **Workers / chunk size**: 25 concurrent / 50,000 rows
- **Dataset**: 25 tables, ~6.35 B rows total
- **Result**: ~110 K rows/sec sustained, ~1.7 GB RSS, 82–91 % CPU sustained
- **Estimated total time** (full 1 TB): ~16 hours

> **Status**: this is a *partial* run (2,176 of 113,129 chunks completed at the time of recording). A complete end-to-end run is in progress. Full report with timing breakdown, optimizations applied, and known bottlenecks: [`benchmarks/2026-03-07-tpcds-1tb-snapshot.md`](benchmarks/2026-03-07-tpcds-1tb-snapshot.md).

**Where's the bottleneck?** All 8 vCPUs of the worker are saturated on JSON serialization and gzip compression — the bottleneck is *worker compute*, not PostgreSQL or StarRocks. Larger instances scale further until you hit the source DB's read IOPS ceiling (typically ~12 K IOPS on RDS gp3).

### CDC streaming — per-daemon footprint

In a 3-day production load test, **40 dbmazz daemons ran concurrently on a single ~t3.medium worker** (2 vCPU / 4 GB RAM), split across three workload tiers that differed by 4 orders of magnitude in configured insert rate. Per-daemon footprint was nearly constant across all three tiers:

| Tier | Configured rate (per source) | Jobs | CPU avg | RSS avg |
|---|---|:---:|:---:|:---:|
| **High-rate** | 500–1 000 inserts/sec | 10 | **11.3 millicores** | **11.0 MB** |
| **Moderate-rate** | 50–100 inserts/sec | 20 | **10.7 millicores** | **10.7 MB** |
| **Low-rate** | 1–5 inserts/min | 10 | **12.3 millicores** | **10.7 MB** |

> **The headline finding**: dbmazz overhead is **fixed-cost per daemon**, not load-dependent. Whether the source is producing 1 000 inserts/sec or 1 insert/minute, a daemon converges on roughly **1 % of one CPU core and ~11 MB of RSS**. The cost is dominated by holding the replication slot, parsing pgoutput, and maintaining the sink connection — the marginal cost per event is invisible at this scale.

The same worker reported **15 % total CPU and 522 MB total RAM used (12.7 % of capacity)** with all 40 daemons running. dbmazz scales horizontally on a single host: each daemon is an independent Unix process with its own replication slot and sink connection.

Full setup, per-tier breakdown, methodology, queries, and honest caveats: [`benchmarks/2026-04-13-cdc-footprint-multitenant.md`](benchmarks/2026-04-13-cdc-footprint-multitenant.md).

**What this does NOT measure**: maximum sustained throughput per daemon (no daemon was anywhere close to saturated), end-to-end lag percentiles (the workload was light enough that lag stayed below the metric's reporting threshold), or behaviour under a sink slowdown. A reproducible single-daemon throughput benchmark with `pgbench`-driven sustained load is tracked in [issue #71](https://github.com/ez-cdc/dbmazz/issues/71).

---

## 🐳 Production deployment

The production path is a Docker container with a pinned version, a restart policy, and secrets in an env file:

```bash
docker run -d \
  --name dbmazz \
  --restart unless-stopped \
  -p 8080:8080 -p 50051:50051 \
  --env-file /etc/dbmazz/env \
  ghcr.io/ez-cdc/dbmazz:1
```

The official image is published on every release to [GitHub Container Registry](https://github.com/ez-cdc/dbmazz/pkgs/container/dbmazz). It's multi-arch (`linux/amd64` + `linux/arm64`), runs as non-root, and ships with the HTTP API and gRPC control plane enabled by default.

For Docker Compose, AWS ECS Fargate, secrets management (Secrets Manager / SSM / env files), Prometheus monitoring, and operations (`pause` / `resume` / `drain-stop`), see [`docs/production-deployment.md`](docs/production-deployment.md).

---

## 👩‍💻 For developers

### Build from source

```bash
cargo build --release
```

Default build includes all 3 sinks plus the HTTP API. For a minimal build:

```bash
cargo build --release --no-default-features \
  --features "sink-starrocks,sink-postgres,sink-snowflake,grpc-reflection"
```

Requires Rust 1.91.1+ (MSRV pinned by `aws-smithy-async`). System deps: `protobuf-compiler`, `musl-tools`, `pkg-config`, `perl`, `make`.

### Add a new sink

The `Sink` trait is intentionally small — six methods, one with a default — modelled after Kafka Connect:

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> SinkCapabilities;
    async fn validate_connection(&self) -> Result<()>;
    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> { Ok(()) }
    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult>;
    async fn close(&mut self) -> Result<()>;
}
```

Snapshot and CDC both use `write_batch()` — there is no separate snapshot path. The sink is fully responsible for its loading strategy (Stream Load, `COPY`, S3 staging, `MERGE`, etc.) and the engine doesn't care.

Step-by-step guide: [`docs/contributing-connectors.md`](docs/contributing-connectors.md).

### Run the tests

```bash
cargo test                        # unit + integration
cargo fmt -- --check              # formatting
cargo clippy -- -D warnings       # lints
```

The end-to-end suite lives in [`e2e-cli/`](e2e-cli/). It wraps Docker, spins up source/sink containers, runs dbmazz against them, and validates the result with the 13-check verification suite. See [`e2e-cli/README.md`](e2e-cli/README.md).

---

## 📖 Reference

<details>
<summary><strong>⚙️ Configuration (environment variables)</strong></summary>

Configured via environment variables. See [`docs/configuration.md`](docs/configuration.md) for the full reference.

| Variable | Default | Description |
|---|---|---|
| `SOURCE_URL` | — | PostgreSQL connection string (`?replication=database` required) |
| `SOURCE_SLOT_NAME` | `dbmazz_slot` | Logical replication slot name |
| `SOURCE_PUBLICATION_NAME` | `dbmazz_pub` | Publication name |
| `TABLES` | — | Comma-separated list of tables to replicate |
| `SINK_TYPE` | `starrocks` | `starrocks` \| `postgres` \| `snowflake` |
| `SINK_URL` | — | Sink connection URL |
| `SINK_DATABASE` | — | Target database |
| `SINK_USER` | `root` | Sink user |
| `SINK_PASSWORD` | *(empty)* | Sink password |
| `FLUSH_SIZE` | `10000` | Max events per batch |
| `FLUSH_INTERVAL_MS` | `5000` | Max ms before flushing |
| `DO_SNAPSHOT` | `false` | Run initial backfill on startup |
| `SNAPSHOT_CHUNK_SIZE` | `50000` | Rows per snapshot chunk |
| `SNAPSHOT_PARALLEL_WORKERS` | `2` | Concurrent snapshot workers |
| `GRPC_PORT` | `50051` | gRPC server port |
| `HTTP_API_PORT` | `8080` | HTTP API port |
| `RUST_LOG` | `info` | Log level |

</details>

<details>
<summary><strong>🌐 HTTP API</strong></summary>

Enabled by default on port 8080.

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Container health probe |
| GET | `/status` | Full state as JSON |
| GET | `/metrics/prometheus` | Prometheus scrape target |
| POST | `/pause` | Pause replication |
| POST | `/resume` | Resume replication |
| POST | `/drain-stop` | Graceful drain and stop |

```bash
curl http://localhost:8080/healthz
curl -X POST http://localhost:8080/pause
```

</details>

<details>
<summary><strong>🔌 gRPC API</strong></summary>

Reflection enabled — `grpcurl` works without `.proto` files.

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Pause
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/StartSnapshot
grpcurl -plaintext -d '{"interval_ms": 2000}' localhost:50051 dbmazz.CdcMetricsService/StreamMetrics
```

Services: `HealthService`, `CdcControlService`, `CdcStatusService`, `CdcMetricsService`. Full RPC list in [`proto/`](proto/).

</details>

<details>
<summary><strong>📸 Snapshot / Backfill</strong></summary>

Set `DO_SNAPSHOT=true` to run a full snapshot on startup. dbmazz uses the [Flink CDC concurrent snapshot algorithm](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/#scan-incremental-snapshot):

1. Each table is divided into PK-range chunks.
2. N parallel workers pick up chunks: emit a low-watermark, `SELECT` the chunk, emit a high-watermark, write to the sink.
3. The WAL consumer dedups events whose PK falls inside a completed chunk and whose LSN ≤ the chunk's HW.

Progress is persisted in `dbmazz_snapshot_state` in the source DB, so an interrupted snapshot resumes from the last completed chunk.

You can also trigger a snapshot on-demand at any time via gRPC:

```bash
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/StartSnapshot
```

</details>

<details>
<summary><strong>🐳 Docker tags</strong></summary>

The official image is published on every release to GHCR:

```bash
docker pull ghcr.io/ez-cdc/dbmazz:1.6.3   # exact version (recommended for prod)
docker pull ghcr.io/ez-cdc/dbmazz:1.6     # latest patch of 1.6
docker pull ghcr.io/ez-cdc/dbmazz:1       # latest minor of 1
docker pull ghcr.io/ez-cdc/dbmazz:latest  # latest stable (avoid for prod)
```

</details>

---

## 💬 Community

- **GitHub Discussions** — questions, ideas, show & tell: [github.com/ez-cdc/dbmazz/discussions](https://github.com/ez-cdc/dbmazz/discussions)
- **Issues** — bug reports and feature requests: [github.com/ez-cdc/dbmazz/issues](https://github.com/ez-cdc/dbmazz/issues)
- **Changelog** — [`CHANGELOG.md`](CHANGELOG.md)

We're a small project. PRs and issues are read by humans, not bots.

---

## 🤝 Contributing

dbmazz welcomes contributions. The simplest path:

1. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup, conventions, and the PR checklist.
2. For new connectors, see [`docs/contributing-connectors.md`](docs/contributing-connectors.md) — the `Sink` trait is small and the engine handles snapshot + CDC for you.
3. For architecture-level questions, [`docs/architecture.md`](docs/architecture.md) is the deep dive.

By contributing you agree your contributions are licensed under the Elastic License v2.0, the same license as the project.

---

## 📄 License

[Elastic License v2.0](LICENSE).

In plain English: **dbmazz is free for commercial and non-commercial use**, including running it in production, embedding it in your own product, or modifying it for internal use. The only restriction is that you cannot offer dbmazz to third parties as a managed service. Self-hosting is unrestricted.

---

## ☁️ About EZ-CDC

dbmazz is the open-source CDC engine maintained by [**EZ-CDC**](https://ez-cdc.com). The same team also runs **EZ-CDC Cloud**, a managed BYOC platform for teams that want to run multiple replication jobs across AWS or GCP without operating dbmazz themselves — auto-healing workers, centralized monitoring, a web portal, and a REST API. It's built on top of the same engine you'll find in this repo.

If you're happy self-hosting one or two pipelines, dbmazz is what you want. If you'd like someone else to run them for you, [ez-cdc.com](https://ez-cdc.com) has the details.
