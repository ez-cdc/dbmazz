<div align="center">

<a href="https://github.com/ez-cdc/dbmazz" target="_blank">
  <img src="assets/ez-cdc-banner.png" alt="dbmazz — Real-time PostgreSQL CDC" width="100%">
</a>

<br>

# dbmazz

**Real-time PostgreSQL CDC in ~11 MB of RAM. One binary. No Kafka.**

A single Rust daemon that streams PostgreSQL changes to StarRocks, Snowflake, or another PostgreSQL with sub-second replication lag — **23–360× lighter** than [Debezium standard deployments][deb-faq] and **727× lighter** than [Airbyte's minimum recommendation][air-deploy]. ([How we measured ↓](#-performance))

Built and maintained by **[EZ-CDC](https://ez-cdc.com)**.

[![License: ELv2](https://img.shields.io/badge/license-ELv2-blue.svg)](LICENSE)
[![GHCR](https://img.shields.io/badge/ghcr.io-ez--cdc%2Fdbmazz-0969da?logo=docker)](https://github.com/ez-cdc/dbmazz/pkgs/container/dbmazz)
[![Discussions](https://img.shields.io/badge/GitHub-Discussions-181717?logo=github)](https://github.com/ez-cdc/dbmazz/discussions)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange?logo=rust)](https://www.rust-lang.org)

[Quickstart](#-try-it-in-2-minutes) · [What is dbmazz?](#what-is-dbmazz) · [At a glance](#at-a-glance) · [Performance](#-performance) · [Production](#-production-deployment)

[deb-faq]: https://debezium.io/documentation/faq/
[air-deploy]: https://docs.airbyte.com/platform/deploying-airbyte

</div>

---

## What is dbmazz?

dbmazz is what CDC looks like without the JVM tax: a single Rust binary, built and maintained by **[EZ-CDC](https://ez-cdc.com)**, that reads the PostgreSQL WAL via logical replication and streams every `INSERT`, `UPDATE`, and `DELETE` into your sink in **real time, with sub-second replication lag in steady state**. No Kafka, no Flink, no ZooKeeper, no Connect cluster, no schema registry — and no batch windows.

The whole thing is **~30 MB on disk** and **~11 MB resident in memory** — about the size of an idle shell session, not a database tool. Run it from the official Docker image or build it from source. Each instance handles one replication job.

<div align="center">
  <img src="assets/demo.gif" alt="dbmazz TUI dashboard demo" width="90%">
  <br>
  <sup><i>The <code>ez-cdc</code> CLI in <code>quickstart</code> mode — live throughput, lag, source vs target row counts.</i></sup>
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
| **Deployment** | **Single CLI** | Compose / K8s with multiple services | Docker Compose with Temporal stack | Docker Compose / K8s with microservices | Docker + Postgres |

Each tool optimizes for different things. dbmazz optimizes for **resource efficiency and operational simplicity**. The numbers above are from each project's own documentation:

- **Debezium**: [official FAQ](https://debezium.io/documentation/faq/) (256 MB – 2 GB typical heap), with 4 GB+ for high-throughput per [RisingWave's deployment guide](https://risingwave.com/blog/debezium-kubernetes-deployment-production/).
- **PeerDB**: [GitHub issue #2727](https://github.com/PeerDB-io/peerdb/issues/2727) (enterprise flow workers; smaller deployments are not published).
- **Airbyte**: [official deployment docs](https://docs.airbyte.com/platform/deploying-airbyte) (4 vCPU + 8 GB minimum recommended).
- **Sequin**: their docs do not publish a memory footprint as of this comparison; we list it as "not published" rather than guess.
- **dbmazz**: from [our own benchmark report](benchmarks/2026-04-13-cdc-footprint-multitenant.md) (40 daemons concurrent on a single 2 vCPU / 4 GB worker, ~11 MB RSS each).

---

## 🤔 Why dbmazz?

In a 3-day production load test we ran **40 dbmazz daemons in parallel on a single 2 vCPU / 4 GB worker**, each replicating its own PostgreSQL database to a shared StarRocks instance. Total worker memory used: 522 MB out of 4 GB. Total worker CPU: 15 % average, 32 % peak. Per-daemon footprint converges on roughly **1 % of one CPU core and ~11 MB of RSS**, regardless of whether the source is producing 1 000 inserts/sec or 1 insert/minute. dbmazz overhead is **fixed-cost per daemon, not load-dependent**. ([Full benchmark report](benchmarks/2026-04-13-cdc-footprint-multitenant.md))

dbmazz is the alternative to running a streaming platform: a single Rust binary that connects to your Postgres source on one side and your sink on the other. No brokers, no clusters, no orchestration. Run it, point it at two databases, and you're replicating.

### Where dbmazz wins

- **⚡ Real-time streaming, not scheduled batch syncs.** dbmazz delivers WAL events as they happen, with sub-second replication lag in steady state. No sync windows, no scheduled jobs, no waiting for the next batch. If freshness matters to your downstream, that's the difference between an *operational replica* and a *day-old data lake*.
- **🪶 Multi-tenant CDC at minimal cost** — dozens of independent replication jobs on a single small worker. The benchmark shows 40 daemons on a 2 vCPU / 4 GB box with ~70 % memory headroom still free.
- **🏔️ Postgres → analytical warehouse without a streaming platform** — direct sink writes with no Kafka in between.
- **🌱 Edge or resource-constrained environments** — runs comfortably on a `t3.micro`, a Raspberry Pi, or as a sidecar to your application container.

---

## 🚀 Try it in 2 minutes

dbmazz is operated through the `ez-cdc` CLI. Install it with one command:

```bash
curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh
```

Then point it at a PostgreSQL source and a sink:

```bash
ez-cdc datasource init                                    # write a starter config
ez-cdc datasource add                                     # interactive wizard for source + sink
ez-cdc quickstart --source my-pg --sink my-warehouse      # spin up dbmazz + live dashboard
```

The CLI runs on Linux and macOS (`amd64` and `arm64`). Press `t` in the dashboard to generate live traffic, `q` to quit.

Full CLI reference: [`e2e-cli/README.md`](e2e-cli/README.md).

### Run the verification suite

The CLI also ships with a 13-check end-to-end test harness that validates every supported sink: schema, snapshot integrity, CDC `INSERT`/`UPDATE`/`DELETE`, TOAST handling, NULL roundtrip, and idempotency drift over time.

```bash
ez-cdc verify --source my-pg --sink my-warehouse           # full suite
ez-cdc verify --source my-pg --sink my-warehouse --quick   # skip slow checks
```

Use `--json-report results.json` to wire it into CI.

---

## 🔌 Supported sources & sinks

| Source | Sink | Status |
|---|---|:---:|
| PostgreSQL 12+ | **StarRocks** | ✅ Stable |
| PostgreSQL 12+ | **PostgreSQL 15+** | ✅ Stable |
| PostgreSQL 12+ | **Snowflake** | ✅ Stable |
| PostgreSQL 12+ | S3 / Iceberg | 🚧 Roadmap |
| MySQL | * | 🚧 Roadmap |

To add a new source or sink, see [`docs/contributing-connectors.md`](docs/contributing-connectors.md).

---

## 📊 Performance

We publish reproducible benchmarks with full hardware specs and methodology. Numbers below come from real runs, not synthetic projections.

### Read this first — Snapshot vs CDC

dbmazz operates in two modes with **fundamentally different resource profiles**. Don't conflate them.

- **Snapshot** (one-time backfill) is a heavy parallel workload: it reads millions of rows from the source, serializes them, and pushes them in batches. The CPU and memory you see in the snapshot benchmark below reflect this *workload* — not steady-state daemon overhead. Snapshot runs once.

- **CDC streaming** (steady state) is an entirely different beast. Memory is bounded and the per-daemon cost is nearly constant regardless of source rate. This is what you run 99% of the time, and it costs almost nothing.

> **TL;DR**: if you see "1.7 GB RSS" in the snapshot benchmark and assume that's what dbmazz uses on your production CDC pipeline — that's the wrong takeaway. CDC steady state runs in a fraction of the resources.

### Snapshot — TPC-DS 1 TB (partial run)

We published a partial TPC-DS 1 TB snapshot run alongside the source code so you can see exactly what we measured, on what hardware, and where the bottleneck is.

- **Source**: PostgreSQL 16 on RDS (`us-west-2`), gp3 storage
- **Sink**: StarRocks on EC2 (same region)
- **Worker**: c5.2xlarge — 8 vCPU, 16 GB RAM
- **Dataset**: 25 tables, ~6.35 B rows total
- **Result**: ~110 K rows/sec sustained, ~1.7 GB RSS, 82–91 % CPU sustained
- **Estimated total time** (full 1 TB): ~16 hours

> **Status**: this is a *partial* run (2,176 of 113,129 chunks completed at the time of recording). A complete end-to-end run is in progress. Full report: [`benchmarks/2026-03-07-tpcds-1tb-snapshot.md`](benchmarks/2026-03-07-tpcds-1tb-snapshot.md).

**Where's the bottleneck?** All 8 vCPUs of the worker are saturated — the bottleneck is *worker compute*, not PostgreSQL or StarRocks. Larger instances scale further until you hit the source DB's read IOPS ceiling (typically ~12 K IOPS on RDS gp3).

### CDC streaming — per-daemon footprint

In a 3-day production load test, **40 dbmazz daemons ran concurrently on a single ~t3.medium worker** (2 vCPU / 4 GB RAM), split across three workload tiers that differed by 4 orders of magnitude in configured insert rate. Per-daemon footprint was nearly constant across all three tiers:

| Tier | Configured rate (per source) | Jobs | CPU avg | RSS avg |
|---|---|:---:|:---:|:---:|
| **High-rate** | 500–1 000 inserts/sec | 10 | **11.3 millicores** | **11.0 MB** |
| **Moderate-rate** | 50–100 inserts/sec | 20 | **10.7 millicores** | **10.7 MB** |
| **Low-rate** | 1–5 inserts/min | 10 | **12.3 millicores** | **10.7 MB** |

> **The headline finding**: dbmazz overhead is **fixed-cost per daemon**, not load-dependent. Whether the source is producing 1 000 inserts/sec or 1 insert/minute, a daemon converges on roughly **1 % of one CPU core and ~11 MB of RSS**. The marginal cost per event is invisible at this scale.

The same worker reported **15 % total CPU and 522 MB total RAM used (12.7 % of capacity)** with all 40 daemons running. dbmazz scales horizontally on a single host.

Full setup, per-tier breakdown, methodology, queries, and honest caveats: [`benchmarks/2026-04-13-cdc-footprint-multitenant.md`](benchmarks/2026-04-13-cdc-footprint-multitenant.md).

**What this does NOT measure**: maximum sustained throughput per daemon (no daemon was anywhere close to saturated), end-to-end lag percentiles (the workload was light enough that lag stayed below the metric's reporting threshold), or behaviour under a sink slowdown. A reproducible single-daemon throughput benchmark with `pgbench`-driven sustained load is tracked in [issue #71](https://github.com/ez-cdc/dbmazz/issues/71).

---

## 🐳 Production deployment

For managed BYOC deployment with auto-healing workers, centralized monitoring, RBAC, audit logs, and a web portal — running dbmazz in your own AWS or GCP account via Terraform — see **[EZ-CDC Cloud](https://ez-cdc.com)**.

---

## 👩‍💻 For developers

```bash
cargo build --release
cargo test
cargo fmt -- --check
cargo clippy -- -D warnings
```

Requires Rust 1.91.1+. System deps: `protobuf-compiler`, `musl-tools`, `pkg-config`, `perl`, `make`.

The end-to-end suite lives in [`e2e-cli/`](e2e-cli/) — it runs the dbmazz daemon against a configured source and sink and validates the result with the 13-check verification harness.

To add a new source or sink connector, see [`docs/contributing-connectors.md`](docs/contributing-connectors.md).

---

## 💬 Community

- **GitHub Discussions** — questions, ideas, show & tell: [github.com/ez-cdc/dbmazz/discussions](https://github.com/ez-cdc/dbmazz/discussions)
- **Issues** — bug reports and feature requests: [github.com/ez-cdc/dbmazz/issues](https://github.com/ez-cdc/dbmazz/issues)
- **Changelog** — [`CHANGELOG.md`](CHANGELOG.md)

We're a small project. PRs and issues are read by humans, not bots.

---

## 🤝 Contributing

dbmazz welcomes contributions.

1. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup, conventions, and the PR checklist.
2. To add a new source or sink connector, see [`docs/contributing-connectors.md`](docs/contributing-connectors.md).

By contributing you agree your contributions are licensed under the Elastic License v2.0, the same license as the project.

---

## 📄 License

[Elastic License v2.0](LICENSE).

In plain English: **dbmazz is free for commercial and non-commercial use**, including running it in production, embedding it in your own product, or modifying it for internal use. The only restriction is that you cannot offer dbmazz to third parties as a managed service. Self-hosting is unrestricted.

---

## ☁️ About EZ-CDC

**dbmazz is the open-source CDC engine maintained by [EZ-CDC](https://ez-cdc.com).** We're a small team building modern data replication tools for teams that want streaming Postgres CDC without operating a streaming platform — and dbmazz is the daemon at the heart of everything we ship.

The same team also runs **[EZ-CDC Cloud](https://ez-cdc.com)**: a managed BYOC platform that deploys dbmazz into your own AWS or GCP account via Terraform.
