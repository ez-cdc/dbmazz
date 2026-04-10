<div align="center">

<a href="https://ez-cdc.com" target="_blank">
  <img src="assets/ez-cdc-banner.png" alt="EZ-CDC — Real-time Change Data Capture" width="100%">
</a>

<br>

**Real-time data replication, radically simplified**

Sub-second latency · 5 MB memory · Zero config · One binary · Written in Rust

[![License](https://img.shields.io/badge/license-ELv2-blue.svg)](LICENSE)

[Quickstart](#-quickstart) · [Test a sink](#-test-a-sink-end-to-end) · [Why dbmazz?](#why-dbmazz) · [Performance](#performance) · [EZ-CDC Cloud](#️-scale-with-ez-cdc-cloud) · [Reference](#-reference)

</div>

---

## 🚀 Quickstart

Clone the repo, install the `ez-cdc` CLI, and launch an interactive dashboard:

```bash
git clone https://github.com/ez-cdc/dbmazz.git
cd dbmazz

# First time only — install the test harness CLI
cd e2e
python3 -m venv venv
source venv/vin/activate
pip install -e e2e/

# Optional but recommended: tab completion for your shell
ez-cdc --install-completion

# Launch dbmazz with a sink of your choice
ez-cdc quickstart                 # interactive menu
ez-cdc quickstart starrocks       # direct: PG → StarRocks
ez-cdc quickstart pg-target       # direct: PG → PG
```

`ez-cdc quickstart` spins up the selected sink, waits for replication to
start, and opens a live terminal dashboard showing stage, lag, throughput,
and source → target row counts in real time. Press `q` or `Ctrl+C` to
exit, and the CLI will ask whether to stop and destroy the stack.

> StarRocks takes ~60s to initialize on first run.

### Use your own databases

Run the binary directly and configure everything from the web UI at
`http://localhost:8080`:

```bash
cargo build --release --features http-api
./target/release/dbmazz
```

A setup wizard lets you test connections, discover tables, and start
replication with one click. No config files needed. See
[`docs/configuration.md`](docs/configuration.md) for the full list of
environment variables if you prefer to configure via env vars.

---

## 🧪 Test a sink end-to-end

The `ez-cdc` CLI provides a single entry point for running the full
e2e validation suite against any supported sink. It verifies snapshot,
CDC (INSERT/UPDATE/DELETE), TOAST handling, schema consistency, and
more — see [`docs/contributing-connectors.md`](docs/contributing-connectors.md)
for the full list of checks.

| Profile | Source | Target | Requirements |
|---------|--------|--------|--------------|
| `starrocks` | PostgreSQL | StarRocks | Docker only |
| `pg-target` | PostgreSQL | PostgreSQL | Docker only |
| `snowflake` | PostgreSQL | Snowflake (cloud) | Snowflake account |

### Run verify for one sink

```bash
ez-cdc verify pg-target           # full suite (tier 1 + 2), ~2 min
ez-cdc verify starrocks --quick   # tier 1 only, ~30s
ez-cdc verify snowflake           # requires e2e/.env.snowflake
```

### Run verify for all sinks

```bash
ez-cdc verify --all               # auto-detects snowflake if .env.snowflake exists
```

### Snowflake credentials (one-time)

Snowflake is cloud-only — no Docker container. Configure credentials once:

```bash
cp e2e/.env.snowflake.example e2e/.env.snowflake
# Edit e2e/.env.snowflake with your account, warehouse, user, password
```

Free 30-day trial at [signup.snowflake.com](https://signup.snowflake.com).

### Other commands

```bash
ez-cdc up starrocks               # just bring the stack up (no tests, no dashboard)
ez-cdc down starrocks             # stop and destroy
ez-cdc logs starrocks             # tail compose logs
ez-cdc status starrocks           # one-shot dbmazz /status snapshot
ez-cdc --help                     # see everything
```

> Adding a new sink? See [`e2e/README.md`](e2e/README.md) and
> [`docs/contributing-connectors.md`](docs/contributing-connectors.md)
> for the step-by-step checklist.

---

## Why dbmazz?

Other CDC tools need Kafka, ZooKeeper, JVM clusters, or multi-container orchestration. dbmazz is a single binary — download, run, replicate.

|  |  |
|--|--|
| ⚡ **Fast** | 300K+ events/sec. Sub-second replication lag. |
| 🪶 **Tiny** | ~5 MB memory footprint. Runs on the smallest EC2 instance or a Raspberry Pi. |
| 🚀 **Simple** | One binary, zero dependencies. No Kafka, no JVM, no cluster. Up and running in minutes. |
| 🔒 **Reliable** | At-least-once delivery via LSN checkpointing. No data loss. |
| 📸 **Snapshot** | Backfill existing data with zero downtime — runs concurrently with CDC. |
| 🔧 **Zero config** | Auto-creates publications, replication slots, sink tables, and audit columns. |
| 📊 **Observable** | Built-in dashboard, Prometheus metrics, and gRPC API out of the box. |

### Supported databases

| Source | Sink | Status |
|:-------|:-----|:-------|
| PostgreSQL | StarRocks | Stable |
| PostgreSQL | PostgreSQL | In development |
| PostgreSQL | Snowflake | In development |

MongoDB, S3, and more on the [roadmap](.plans/multi-sink-roadmap.md).

---

## Performance

### CDC Replication

dbmazz reads the PostgreSQL WAL directly and streams changes to StarRocks. Single binary, no JVM, no Kafka, no intermediate queues.

```
  Throughput   ████████████████████████████████████████  300,000+ events/sec
  Latency      █                                        < 1 second
  Memory       ▏                                        ~ 5 MB
  CPU          ▏                                        < 2%
```

|  | dbmazz | Traditional CDC tools |
|:--|:--:|:--|
| **Runtime** | Single binary (5 MB RSS) | JVM + Kafka + ZooKeeper, or managed SaaS |
| **Deployment** | One process, zero dependencies | Multi-container orchestration |
| **Scaling model** | Vertical (one instance per job) | Horizontal (brokers, connectors, workers) |

---

### Snapshot (Initial Backfill)

Snapshot loads all existing rows before CDC takes over. It runs concurrently with the WAL consumer — no downtime, no data loss.

<table>
<tr><td>

**Test environment** — TPC-DS 1TB (25 tables, 6.35 billion rows)

| | |
|--|--|
| **Source** | PostgreSQL 16 — AWS RDS `db.r6i.2xlarge` (8 vCPU, 64 GB) |
| **Worker** | EC2 `c6i.4xlarge` (16 vCPU, 32 GB) — same AZ |
| **Storage** | gp3 (3,000 baseline IOPS, burst to 16,000) |

</td></tr>
</table>

| Workers | Chunk size | Narrow tables | Wide tables | Worker CPU | Worker RAM |
|:-------:|:----------:|---------------:|-------------:|:----------:|:----------:|
| 6 | 150K | **130K rows/s** | 11K rows/s | 25–35% | ~2 GB |
| 12 | 200K | **107K rows/s** | 22K rows/s | 60% | ~2 GB |
| 20 | 50K | 93K rows/s | 15K rows/s | 87–97% | ~2 GB |

<sup>Narrow: `catalog_returns` (27 cols, 144M rows). Wide: `catalog_sales` (34 cols, 1.44B rows).</sup>

> **Where's the bottleneck?** On wide tables the worker sits idle at 3–7% CPU while RDS DiskQueueDepth hits 12 and ReadIOPS maxes at 12,000. The bottleneck is PostgreSQL disk I/O, not the CDC tool. Every CDC tool that reads from PostgreSQL hits this same ceiling.

---

## ☁️ Scale with EZ-CDC Cloud

dbmazz is the open-source CDC engine at the core of **<a href="https://ez-cdc.com" target="_blank">EZ-CDC</a>**. It's fast, reliable, and free to use.

But running CDC in production means managing multiple jobs, monitoring them, handling failures, and keeping everything running 24/7. That's what EZ-CDC Cloud does.

|  | **dbmazz** (open source) | **EZ-CDC Cloud** |
|--|--------------------------|-------------------|
| **Engine** | Full CDC engine (this repo) | Same engine, fully managed |
| **Jobs** | 1 instance = 1 job | Unlimited jobs, one dashboard |
| **Deployment** | You build, deploy, maintain | BYOC — deploys in your AWS/GCP via Terraform |
| **Availability** | Manual restarts | Auto-healing workers, zero downtime |
| **Monitoring** | Per-instance dashboard | Centralized metrics, alerting, historical dashboards |
| **Security** | You manage credentials | AES-256 encryption, RBAC, audit logs, API keys |
| **Web portal** | Status page at `:8080` | Full management portal for your team |
| **API** | HTTP + gRPC per instance | REST API + MCP server (Claude, Cursor) |
| **Support** | GitHub Issues | Enterprise SLAs |
| **Cost** | Free | Pay per deployment |

### When to use dbmazz

- Single PostgreSQL → StarRocks pipeline
- You're comfortable managing the process yourself
- You want to embed the CDC engine in your own tooling

### When to use EZ-CDC

- Multiple replication jobs across databases
- Zero-downtime with auto-healing and restarts
- Centralized observability for all jobs
- BYOC deployment with Terraform automation
- Web portal for your team — no CLI needed
- Enterprise security — encrypted configs, RBAC, API keys

<p align="center">
  <br>
  <a href="https://ez-cdc.com" target="_blank"><strong>Get started with EZ-CDC Cloud →</strong></a>
  <br><br>
</p>

---

## 📖 Reference

<details>
<summary><strong>🐳 Docker deployment</strong></summary>

The preferred way to run dbmazz locally is through the `ez-cdc` CLI
(see the Quickstart at the top of this README). You can also drive
`docker compose` directly if you prefer:

### StarRocks (batteries included)

```bash
docker compose -f e2e/compose.yml --profile starrocks up -d
```

### With initial snapshot (backfill existing data)

Set environment variables in your shell or via a `.env` file next to
`e2e/compose.yml`:

```bash
DO_SNAPSHOT=true
SNAPSHOT_CHUNK_SIZE=50000
docker compose -f e2e/compose.yml --profile starrocks up -d
```

### Stop

```bash
docker compose -f e2e/compose.yml --profile starrocks down -v
```

</details>

<details>
<summary><strong>⚙️ Configuration</strong></summary>

Configured via environment variables. See [`docs/configuration.md`](docs/configuration.md) for a full reference with all variables organized by section.

When built with `--features http-api`, all connection variables are optional — you can configure everything from the browser instead.

| Variable | Default | Description |
|----------|---------|-------------|
| `SOURCE_URL` | — | PostgreSQL connection string (`?replication=database` required) |
| `SOURCE_SLOT_NAME` | `dbmazz_slot` | Logical replication slot name |
| `SOURCE_PUBLICATION_NAME` | `dbmazz_pub` | Publication name |
| `TABLES` | `orders,order_items` | Comma-separated list of tables to replicate |
| `SINK_URL` | — | StarRocks FE HTTP URL (e.g. `http://starrocks:8030`) |
| `SINK_PORT` | `9030` | StarRocks FE MySQL port |
| `SINK_DATABASE` | — | Target database in StarRocks |
| `SINK_USER` | `root` | StarRocks user |
| `SINK_PASSWORD` | *(empty)* | Sink password |
| `SINK_SNOWFLAKE_ACCOUNT` | — | Snowflake account identifier (e.g. `xy12345.us-east-1`) |
| `SINK_SNOWFLAKE_WAREHOUSE` | — | Snowflake warehouse for COPY/MERGE |
| `SINK_SNOWFLAKE_ROLE` | *(empty)* | Snowflake role (optional) |
| `SINK_SNOWFLAKE_PRIVATE_KEY_PATH` | *(empty)* | Path to RSA key for JWT auth (optional) |
| `SINK_SNOWFLAKE_SOFT_DELETE` | `true` | Soft delete mode (`true`/`false`) |
| `FLUSH_SIZE` | `10000` | Max events per batch |
| `FLUSH_INTERVAL_MS` | `5000` | Max ms before flushing a batch |
| `GRPC_PORT` | `50051` | gRPC server port |
| `HTTP_API_PORT` | `8080` | HTTP API port (`--features http-api`) |
| `RUST_LOG` | `info` | Log level |
| `DO_SNAPSHOT` | `false` | Enable initial snapshot/backfill of existing data |
| `SNAPSHOT_CHUNK_SIZE` | `50000` | Rows per snapshot chunk (min: 1) |
| `SNAPSHOT_PARALLEL_WORKERS` | `2` | Reserved for future use (currently sequential) |

</details>

<details>
<summary><strong>🌐 HTTP API</strong></summary>

Build with `--features http-api` to enable the web UI and HTTP endpoints.

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

```bash
curl http://localhost:8080/healthz
curl -X POST http://localhost:8080/pause
curl -X POST http://localhost:8080/resume
```

</details>

<details>
<summary><strong>📸 Snapshot / Backfill</strong></summary>

Snapshot loads all existing rows from PostgreSQL into StarRocks before CDC takes over. It runs concurrently with the WAL consumer — no downtime, no data loss.

### Enable on startup

Set `DO_SNAPSHOT=true` to run a full snapshot when the daemon starts:

```bash
DO_SNAPSHOT=true SNAPSHOT_CHUNK_SIZE=50000 ./target/release/dbmazz
```

The snapshot divides each table into PK-range chunks and processes them sequentially. Progress is tracked in a `dbmazz_snapshot_state` table in PostgreSQL, so interrupted snapshots resume from the last completed chunk.

### Trigger on-demand (via gRPC)

You can trigger a snapshot at any time while CDC is running:

```bash
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/StartSnapshot
```

### How it works

Uses the [Flink CDC concurrent snapshot algorithm](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/#scan-incremental-snapshot):

1. For each chunk: emit low-watermark (LW) → SELECT rows → emit high-watermark (HW) → Stream Load to StarRocks
2. The WAL consumer checks `should_emit()` for each event — events within a completed chunk's PK range with LSN <= HW are suppressed (already loaded by snapshot)
3. Events outside chunk ranges or with LSN > HW are emitted normally

This ensures consistent delivery even with concurrent writes during the snapshot.

### Monitor progress

```bash
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcStatusService/GetStatus
# snapshot_active: true, snapshot_chunks_total: 100, snapshot_chunks_done: 42, snapshot_rows_synced: 21000000
```

</details>

<details>
<summary><strong>🔌 gRPC API</strong></summary>

gRPC with reflection enabled — `grpcurl` works without `.proto` files.

```bash
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check
grpcurl -plaintext -d '{"interval_ms": 2000}' localhost:50051 dbmazz.CdcMetricsService/StreamMetrics
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Pause
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Resume
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/StartSnapshot
```

</details>

<details>
<summary><strong>🏗️ Architecture</strong></summary>

```
PostgreSQL (source)            dbmazz                         Sink (target)
┌──────────────┐            ┌──────────────────┐          ┌──────────────┐
│  WAL         │  logical   │ WAL Handler      │          │ StarRocks    │
│  (INSERT,    │  replic.   │   ▼              │          │ PostgreSQL   │
│   UPDATE,    │ ─────────▶ │ CdcRecord        │  write   │ Snowflake    │
│   DELETE)    │ (pgoutput) │   ▼              │──batch──▶│ ...          │
│              │            │ Pipeline         │          │              │
│              │ ◀──────────│ Checkpoint (LSN) │          │              │
└──────────────┘  confirm   └──────────────────┘          └──────────────┘
                                 ~5 MB RAM                     <1s lag
```

dbmazz reads the PostgreSQL Write-Ahead Log via logical replication, converts events to generic `CdcRecord` types, batches them in a pipeline, and writes to any supported sink via the `Sink` trait. Each instance handles one replication job.

The sink is fully responsible for its loading strategy (Stream Load, COPY, S3 staging, etc.). The engine and pipeline are sink-agnostic.

See [docs/architecture.md](docs/architecture.md) for the full data flow, module map, and how to add new sinks.

</details>

<details>
<summary><strong>🔨 Build from source</strong></summary>

```bash
cargo build --release                    # Minimal binary (no HTTP)
cargo build --release --features http-api # With web UI + HTTP API
```

</details>


---

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for general guidelines and [docs/contributing-connectors.md](docs/contributing-connectors.md) for adding new connectors.

## 📄 License

[Elastic License v2.0](LICENSE) — free for commercial and non-commercial use. Cannot be offered as a managed service.
