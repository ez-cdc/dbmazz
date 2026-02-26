<div align="center">

<a href="https://ez-cdc.com">
  <img src="assets/ez-cdc-logo.svg" alt="EZ-CDC" width="360">
</a>

<br><br>

**Blazing fast PostgreSQL to StarRocks replication**

Sub-second latency · 5MB memory · Zero config · Written in Rust

[![License](https://img.shields.io/badge/license-ELv2-blue.svg)](LICENSE)

[Quickstart](#quickstart) · [Why dbmazz?](#why-dbmazz) · [EZ-CDC Cloud](#scale-with-ez-cdc-cloud) · [Reference](#reference)

</div>

---

## Quickstart

Clone and run — PostgreSQL, StarRocks, and sample data included:

```bash
git clone https://github.com/ez-cdc/dbmazz.git
cd dbmazz
docker compose -f docker-compose.production.yml --profile quickstart up -d
```

Open **[http://localhost:8080](http://localhost:8080)** — you'll see a live dashboard with real-time metrics, throughput chart, and replication controls.

That's it. Data is already flowing from PostgreSQL to StarRocks.

```bash
# Verify it's running
curl -s http://localhost:8080/healthz
# {"status":"ok","stage":"cdc","uptime_secs":42}
```

> StarRocks takes ~60s to initialize on first run. The dashboard is available immediately.

### Use your own databases

Run without env vars and configure everything from the browser:

```bash
cargo build --release --features http-api
./target/release/dbmazz
```

Open **[http://localhost:8080](http://localhost:8080)** — a setup wizard lets you test connections, discover tables, and start replication with one click. No config files needed.

---

## Why dbmazz?

|  |  |
|--|--|
| **Fast** | 300K+ events/sec. Sub-second replication lag. |
| **Tiny** | ~5MB memory footprint. Runs on the smallest EC2 instance or a Raspberry Pi. |
| **Reliable** | At-least-once delivery via LSN checkpointing. No data loss. |
| **Snapshot** | Backfill existing data with zero downtime — runs concurrently with CDC. |
| **Zero config** | Auto-creates publications, replication slots, sink tables, and audit columns. |
| **Observable** | Built-in dashboard, Prometheus metrics, and gRPC API out of the box. |

---

## Scale with EZ-CDC Cloud

dbmazz is the open-source CDC engine at the core of **[EZ-CDC](https://ez-cdc.com)**. It's fast, reliable, and free to use.

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
  <a href="https://ez-cdc.com"><strong>Get started with EZ-CDC Cloud →</strong></a>
  <br><br>
</p>

---

## Reference

<details>
<summary><strong>Docker deployment</strong></summary>

### Quickstart (batteries included)

```bash
docker compose -f docker-compose.production.yml --profile quickstart up -d
```

### Production (bring your own databases)

```bash
cp .env.example .env    # fill in your connection details
docker compose -f docker-compose.production.yml up -d
```

### With initial snapshot (backfill existing data)

```bash
# Add to your .env:
DO_SNAPSHOT=true
SNAPSHOT_CHUNK_SIZE=500000

docker compose -f docker-compose.production.yml up -d
```

### Stop

```bash
docker compose -f docker-compose.production.yml --profile quickstart down
```

</details>

<details>
<summary><strong>Configuration</strong></summary>

Configured via environment variables. See [`.env.example`](.env.example) for a full reference.

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
| `SINK_PASSWORD` | *(empty)* | StarRocks password |
| `FLUSH_SIZE` | `10000` | Max events per batch |
| `FLUSH_INTERVAL_MS` | `5000` | Max ms before flushing a batch |
| `GRPC_PORT` | `50051` | gRPC server port |
| `HTTP_API_PORT` | `8080` | HTTP API port (`--features http-api`) |
| `RUST_LOG` | `info` | Log level |
| `DO_SNAPSHOT` | `false` | Enable initial snapshot/backfill of existing data |
| `SNAPSHOT_CHUNK_SIZE` | `500000` | Rows per snapshot chunk (min: 1) |
| `SNAPSHOT_PARALLEL_WORKERS` | `2` | Reserved for future use (currently sequential) |

</details>

<details>
<summary><strong>HTTP API</strong></summary>

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
<summary><strong>Snapshot / Backfill</strong></summary>

Snapshot loads all existing rows from PostgreSQL into StarRocks before CDC takes over. It runs concurrently with the WAL consumer — no downtime, no data loss.

### Enable on startup

Set `DO_SNAPSHOT=true` to run a full snapshot when the daemon starts:

```bash
DO_SNAPSHOT=true SNAPSHOT_CHUNK_SIZE=500000 ./target/release/dbmazz
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
<summary><strong>gRPC API</strong></summary>

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
<summary><strong>Architecture</strong></summary>

```
PostgreSQL                  dbmazz                    StarRocks
┌──────────┐    WAL     ┌──────────────┐  Stream  ┌──────────┐
│  Tables   │──────────▶│  Pipeline    │─────────▶│  Tables   │
│           │  logical  │  - Batching  │  Load    │  + audit  │
│  INSERT   │  replic.  │  - Schema    │  HTTP    │  columns  │
│  UPDATE   │           │  - Checkpoint│          │           │
│  DELETE   │           └──────────────┘          └──────────┘
└──────────┘               5MB RAM                    <1s lag
```

dbmazz reads the PostgreSQL Write-Ahead Log via logical replication, transforms events into batches, and loads them into StarRocks via the Stream Load HTTP API. Each instance handles one replication job.

| Source | Method | Status |
|--------|--------|--------|
| **PostgreSQL** | Logical Replication (pgoutput) | Stable |

| Sink | Method | Status |
|------|--------|--------|
| **StarRocks** | Stream Load HTTP API | Stable |

</details>

<details>
<summary><strong>Build from source</strong></summary>

```bash
cargo build --release                    # Minimal binary (no HTTP)
cargo build --release --features http-api # With web UI + HTTP API
```

</details>

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for general guidelines and [CONTRIBUTING_CONNECTORS.md](CONTRIBUTING_CONNECTORS.md) for adding new connectors.

## License

[Elastic License v2.0](LICENSE) — free for commercial and non-commercial use. Cannot be offered as a managed service.
