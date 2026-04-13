<div align="center">

<a href="https://github.com/ez-cdc/dbmazz" target="_blank">
  <img src="assets/ez-cdc-banner.png" alt="dbmazz — Real-time PostgreSQL CDC" width="100%">
</a>

<br>

# dbmazz

**Real-time PostgreSQL CDC in ~11 MB of RAM. One binary. No Kafka.**

Stream PostgreSQL → StarRocks, Snowflake, or another PostgreSQL with a single Rust daemon. In a 3-day production load test, **40 dbmazz instances ran in parallel on a single 2 vCPU / 4 GB worker** at 15 % total CPU. Each daemon used ~1 % of one CPU core and ~11 MB RSS — fixed cost, regardless of load.

That's **23–360× lighter** than [Debezium standard deployments][deb-faq] and **727× lighter** than [Airbyte's minimum recommendation][air-deploy]. ([How we measured ↓](#-performance))

[![License: ELv2](https://img.shields.io/badge/license-ELv2-blue.svg)](LICENSE)
[![GHCR](https://img.shields.io/badge/ghcr.io-ez--cdc%2Fdbmazz-0969da?logo=docker)](https://github.com/ez-cdc/dbmazz/pkgs/container/dbmazz)
[![Discussions](https://img.shields.io/badge/GitHub-Discussions-181717?logo=github)](https://github.com/ez-cdc/dbmazz/discussions)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange?logo=rust)](https://www.rust-lang.org)

[Quickstart](#-try-it-in-2-minutes) · [At a glance](#at-a-glance) · [What is dbmazz?](#what-is-dbmazz) · [How it works](#how-it-works) · [Performance](#-performance) · [Production](#-production-deployment)

[deb-faq]: https://debezium.io/documentation/faq/
[air-deploy]: https://docs.airbyte.com/platform/deploying-airbyte

</div>

---

## At a glance

How dbmazz compares to other open-source CDC tools on **resource footprint** and **deployment complexity** — the two dimensions where dbmazz is purpose-built to win.

|  | **dbmazz** | Debezium (standard) | PeerDB | Airbyte | Sequin |
|---|---|---|---|---|---|
| **Language / runtime** | Rust (static binary) | Java / JVM | Go + Temporal | Java / Python | Elixir / BEAM |
| **Replication mode** | **Real-time CDC** (sub-second WAL streaming) | Real-time CDC (sub-second WAL streaming) | Near-real-time (~10 s freshness at high throughput) | **Batch ETL** (scheduled syncs, minutes – hours) | Real-time CDC (~55 ms p50) |
| **Components for one PG → sink pipeline** | **1** (single binary) | 2–7 (JVM + Kafka + …) | 3–4 (Server + Postgres + Temporal) | 7–10+ (microservices) | 2 (Container + Postgres) |
| **External dependencies** | **none** | Kafka, ZooKeeper | Temporal, MinIO / S3 | Postgres, Temporal, scheduler | Postgres |
| **Published min memory** | **~11 MB RSS** | 256 MB – 4 GB | 48–64 GB (enterprise tier) | 8 GB+ minimum recommended | not published |
| **Deployment** | `docker run -e SOURCE_URL=… -e SINK_URL=…` | Compose / K8s with multiple services | Docker Compose with Temporal stack | Docker Compose / K8s with microservices | Docker + Postgres |

Each tool optimizes for different things — see [where dbmazz is *not* the right tool](#where-dbmazz-is-not-the-right-tool) below for the honest tradeoffs. The numbers above are from the projects' own documentation:

- **Debezium**: [official FAQ](https://debezium.io/documentation/faq/) (256 MB – 2 GB typical heap), with 4 GB+ for high-throughput per [RisingWave's deployment guide](https://risingwave.com/blog/debezium-kubernetes-deployment-production/).
- **PeerDB**: [GitHub issue #2727](https://github.com/PeerDB-io/peerdb/issues/2727) (enterprise flow workers; smaller deployments are not published).
- **Airbyte**: [official deployment docs](https://docs.airbyte.com/platform/deploying-airbyte) (4 vCPU + 8 GB minimum recommended).
- **Sequin**: their docs do not publish a memory footprint as of this comparison; we list it as "not published" rather than guess.
- **dbmazz**: from [our own benchmark report](benchmarks/2026-04-13-cdc-footprint-multitenant.md) (40 daemons concurrent on a single 2 vCPU / 4 GB worker, ~11 MB RSS each).

---

## What is dbmazz?

dbmazz is what CDC looks like without the JVM tax: a single Rust binary that reads the PostgreSQL WAL via logical replication and streams every `INSERT`, `UPDATE`, and `DELETE` into your sink in **real time, with sub-second replication lag in steady state**. No Kafka, no Flink, no ZooKeeper, no Connect cluster, no schema registry — and no batch windows.

The whole thing is **~30 MB on disk** and **~11 MB resident in memory** — about the size of an idle shell session, not a database tool. It ships with a Prometheus endpoint, a gRPC control plane, and an HTTP API for operations. Run it from the official Docker image, on ECS, on Kubernetes, on a bare VM, or build it from source. Each instance handles one replication job.

<div align="center">
  <img src="assets/demo.svg" alt="dbmazz TUI dashboard demo" width="90%">
  <br>
  <sup><i>The <code>ez-cdc</code> CLI in <code>quickstart</code> mode — live throughput, lag, source vs target row counts.</i></sup>
</div>

---

## 🤔 Why dbmazz?

### The problem

You need to replicate PostgreSQL into an analytical warehouse, lakehouse, or another database. The incumbent tool — **[Debezium](https://debezium.io/)** — needs Kafka, ZooKeeper, a JVM, a Connect cluster, schema registries, and a small army of YAML to wire it all together. You wanted CDC; you got a distributed system.

For perspective: in a 3-day production load test we ran **40 dbmazz daemons in parallel on a single 2 vCPU / 4 GB worker**, each replicating its own PostgreSQL database to a shared StarRocks instance. Total worker memory used: 522 MB out of 4 GB. Total worker CPU: 15 % average, 32 % peak. Every daemon held its own replication slot, parsed pgoutput, and pushed batched writes to the sink — for **~1 % of one CPU core and ~11 MB of RSS**, regardless of whether the source was producing 1 000 inserts/sec or 1 insert/minute. dbmazz overhead is **fixed-cost per daemon, not load-dependent**. ([Full benchmark report](benchmarks/2026-04-13-cdc-footprint-multitenant.md))

dbmazz is the alternative to running a streaming platform: a single Rust binary that connects to your Postgres source on one side and your sink on the other. No brokers, no clusters, no orchestration. Run it, point it at two databases, and you're replicating.

### Where dbmazz wins

- **⚡ Real-time streaming, not scheduled batch syncs.** dbmazz delivers WAL events as they happen, with sub-second replication lag in steady state. Most tools that *call themselves* "Postgres replication" — Airbyte, Fivetran, Stitch — are actually batch ETL with sync windows of minutes or hours. If freshness matters to your downstream, that's the difference between an *operational replica* and a *day-old data lake*.
- **🪶 Multi-tenant CDC at minimal cost** — dozens of independent replication jobs on a single small worker. The benchmark shows 40 daemons on a 2 vCPU / 4 GB box with ~70 % memory headroom still free.
- **🏔️ Postgres → analytical warehouse without a streaming platform** — direct sink writes (Stream Load for StarRocks, binary `COPY` for Postgres, Parquet staging for Snowflake), no Kafka in between.
- **🌱 Edge or resource-constrained environments** — runs comfortably on a `t3.micro`, a Raspberry Pi, or as a sidecar to your application container.
- **🔌 Embed CDC in your own product** — every dbmazz instance exposes gRPC and HTTP APIs for control and observability. Build higher-level systems on top, or integrate it into existing tooling without taking on a SaaS dependency.

### Where dbmazz is *not* the right tool

We try to be honest about where other tools are a better fit. **If your situation matches one of the following, look elsewhere first**:

- **You need MySQL, Oracle, MongoDB, or SQL Server CDC.** dbmazz is PostgreSQL-only today. → Look at [Debezium](https://debezium.io/) (12+ source databases) or a managed platform.
- **You need a hosted web UI for non-technical users to manage pipelines.** dbmazz is a daemon. The control surface is environment variables, APIs, and an optional terminal dashboard. → Look at [Airbyte](https://airbyte.com/) for a no-code UI, or see the [About EZ-CDC](#-about-ez-cdc) note for a managed multi-job platform built on dbmazz.
- **You need built-in transformations or a SQL-based pipeline language.** dbmazz delivers raw CDC events; transformations are your downstream concern (dbt, the sink's own SQL, etc.). → Look at [PeerDB](https://www.peerdb.io/) (SQL transformations baked in) or [Estuary Flow](https://estuary.dev/) (derivations).
- **You need horizontal scaling of a single replication job.** dbmazz scales *vertically* — one instance handles one job. Sharding across instances is your responsibility (or use a job-orchestrator like the one [EZ-CDC Cloud](#-about-ez-cdc) provides on top).

---

## 🚀 Try it in 2 minutes

### The simplest path — `docker run`

If you already have a PostgreSQL source and a sink, the entire dbmazz deployment is one command:

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

That's it. No Kafka cluster to provision. No separate state store. No connector to register. The container image is multi-arch (`amd64` + `arm64`), runs as non-root, and uses ~11 MB of RAM at steady state. dbmazz creates the publication, the replication slot, and the audit columns on the sink automatically on first start, and keeps its LSN checkpoint in the source database itself.

Full configuration reference: [`docs/configuration.md`](docs/configuration.md). Compose, AWS ECS Fargate, secrets management, and monitoring: [`docs/production-deployment.md`](docs/production-deployment.md).

### Or use the CLI for a complete demo (no databases of your own needed)

The `ez-cdc` CLI bundles a one-liner installer plus a `quickstart` command that spins up a demo PostgreSQL source, a demo StarRocks sink, and a dbmazz daemon — all in containers — and opens a live terminal dashboard:

```bash
curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh
ez-cdc datasource init                                    # write a starter config
ez-cdc datasource add                                     # interactive wizard
ez-cdc quickstart --source demo-pg --sink demo-starrocks  # spin up + live dashboard
```

The CLI runs on Linux and macOS (`amd64` and `arm64`); the binary is downloaded from the latest GitHub release, checksum-verified, and installed to `$HOME/.local/bin/ez-cdc`. Press `t` in the dashboard to generate live traffic, `q` to quit.

### Run the verification suite

The CLI also ships with a 13-check end-to-end test harness that validates every supported sink: schema, snapshot integrity, CDC `INSERT`/`UPDATE`/`DELETE`, TOAST handling, NULL roundtrip, and idempotency drift over time.

```bash
ez-cdc verify --source demo-pg --sink demo-starrocks           # full suite
ez-cdc verify --source demo-pg --sink demo-pg-target --quick   # skip slow checks
```

Use `--json-report results.json` to wire it into CI.

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
