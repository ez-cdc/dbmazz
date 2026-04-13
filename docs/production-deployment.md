# Production Deployment Guide

This guide covers running dbmazz in production as a self-hosted Docker
container. If you want to run multiple pipelines, need HA, or want a
managed portal, see [EZ-CDC Cloud](https://ez-cdc.com).

## Prerequisites

Before deploying dbmazz in production you need:

1. **Docker** (or any OCI-compatible runtime: containerd, Podman, etc.)
   on the host that will run the daemon.
2. **PostgreSQL source** with:
   - Logical replication enabled (`wal_level = logical`)
   - A role with `REPLICATION` privilege
   - Permission to `CREATE TABLE` in the source database (dbmazz creates
     two internal tables, `dbmazz_checkpoints` and `dbmazz_snapshot_state`,
     for LSN checkpointing and resumable snapshot state)
3. **Sink** (StarRocks, PostgreSQL, or Snowflake) reachable from the
   container network with the appropriate credentials.
4. A place to store secrets — do **not** put passwords in your
   `docker-compose.yml` or task definition. See the [Secrets](#secrets)
   section below.

## Quickstart — `docker run`

The simplest production deployment is a single `docker run` command:

```bash
docker run -d \
  --name dbmazz \
  --restart unless-stopped \
  -p 8080:8080 \
  -p 50051:50051 \
  --env-file /etc/dbmazz/env \
  ghcr.io/ez-cdc/dbmazz:1
```

Where `/etc/dbmazz/env` is a file like:

```bash
SOURCE_URL=postgres://dbmazz_user:PASSWORD@prod-pg.internal:5432/app?replication=database
TABLES=orders,order_items,customers
SINK_TYPE=starrocks
SINK_URL=http://starrocks-fe.internal:8030
SINK_DATABASE=analytics
SINK_USER=root
SINK_PASSWORD=STARROCKS_PASSWORD
DO_SNAPSHOT=true
FLUSH_SIZE=10000
FLUSH_INTERVAL_MS=5000
RUST_LOG=info
```

**Tag strategy**: pin to a specific version (`ghcr.io/ez-cdc/dbmazz:1.5.2`)
in production for reproducibility. `:1` and `:1.5` follow the latest
patch/minor of a major/minor track. `:latest` exists but you should not
use it in production.

The daemon exposes:
- `:8080` — HTTP API (health check, Prometheus metrics, status, pause/resume/drain-stop)
- `:50051` — gRPC control plane (pause/resume/snapshot)

## Docker Compose

For any deployment more complex than a single container, use
`docker compose`. The following example includes a healthcheck, restart
policy, and uses an external env file for secrets:

```yaml
services:
  dbmazz:
    image: ghcr.io/ez-cdc/dbmazz:1.5.2
    container_name: dbmazz
    restart: unless-stopped
    ports:
      - "8080:8080"
      - "50051:50051"
    env_file:
      - ./dbmazz.env     # gitignored, NOT checked into the repo
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/healthz"]
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 30s
```

`dbmazz.env` contains the same variables shown in the `docker run`
example. Make sure it is gitignored.

Start the daemon:

```bash
docker compose up -d
docker compose logs -f dbmazz
```

## AWS ECS Fargate

A minimal task definition for ECS Fargate. Note that secrets are pulled
from AWS Secrets Manager via the `secrets:` block, not embedded in
`environment:`.

```json
{
  "family": "dbmazz",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "arn:aws:iam::123456789012:role/ecsTaskExecutionRole",
  "containerDefinitions": [
    {
      "name": "dbmazz",
      "image": "ghcr.io/ez-cdc/dbmazz:1.5.2",
      "essential": true,
      "portMappings": [
        { "containerPort": 8080, "protocol": "tcp" },
        { "containerPort": 50051, "protocol": "tcp" }
      ],
      "environment": [
        { "name": "TABLES", "value": "orders,order_items" },
        { "name": "SINK_TYPE", "value": "starrocks" },
        { "name": "SINK_URL", "value": "http://starrocks-fe.internal:8030" },
        { "name": "SINK_DATABASE", "value": "analytics" },
        { "name": "SINK_USER", "value": "root" },
        { "name": "DO_SNAPSHOT", "value": "true" },
        { "name": "FLUSH_SIZE", "value": "10000" },
        { "name": "FLUSH_INTERVAL_MS", "value": "5000" }
      ],
      "secrets": [
        {
          "name": "SOURCE_URL",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:dbmazz/source-url"
        },
        {
          "name": "SINK_PASSWORD",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:dbmazz/sink-password"
        }
      ],
      "healthCheck": {
        "command": ["CMD-SHELL", "curl -sf http://localhost:8080/healthz || exit 1"],
        "interval": 10,
        "timeout": 3,
        "retries": 3,
        "startPeriod": 30
      },
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/dbmazz",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "dbmazz"
        }
      }
    }
  ]
}
```

Notes:
- 256 CPU units + 512 MB is enough for most workloads. dbmazz uses ~5 MB
  RSS in steady state — Fargate cannot go lower than 256/512.
- The task execution role needs `secretsmanager:GetSecretValue` for the
  secrets listed.
- Fargate handles container restarts and health checks automatically.
- Use an ALB target group pointing to port 8080 if you want to expose
  the Prometheus metrics endpoint outside the VPC.

## Secrets

**Never** put database passwords or API tokens in:

- the `environment:` block of `docker-compose.yml`
- the `environment` array of an ECS task definition
- the command line of `docker run -e VAR=secret`
- the repository

Instead:

| Runtime | Mechanism |
|---|---|
| `docker run` | `--env-file /etc/dbmazz/env` with restrictive perms (`chmod 600`) |
| Docker Compose | `env_file:` entry pointing to a gitignored file |
| Docker Compose (secrets-enabled) | `secrets:` + `DOCKER_SECRETS_PATH` and read them in entrypoint |
| ECS Fargate | `secrets:` pulling from Secrets Manager or SSM Parameter Store |
| Kubernetes | `envFrom: secretRef:` or volume-mounted secret |
| Nomad | `template` block with Vault integration |

If you need to rotate a credential, update the secret store and restart
the container. dbmazz resumes from its checkpoint in PostgreSQL so no
data is lost.

## Monitoring

dbmazz exposes a Prometheus endpoint at `/metrics/prometheus`:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'dbmazz'
    scrape_interval: 10s
    static_configs:
      - targets: ['dbmazz:8080']
    metrics_path: /metrics/prometheus
```

Useful metrics to alert on:

| Metric | Meaning | Alert threshold |
|---|---|---|
| `dbmazz_replication_lag_seconds` | PG WAL lag in seconds | > 60s for 5 min |
| `dbmazz_events_total` | Total events processed (counter) | rate drops to 0 unexpectedly |
| `dbmazz_flush_errors_total` | Sink write failures | any increase |
| `dbmazz_snapshot_active` | Snapshot in progress (0/1) | no alert — informational |

The `/status` endpoint returns the full state as JSON for ad-hoc
inspection:

```bash
curl -s http://dbmazz:8080/status | jq
```

## Operations

The HTTP API exposes operational endpoints:

| Endpoint | Method | Description |
|---|---|---|
| `/healthz` | GET | Container health probe |
| `/status` | GET | Full metrics JSON |
| `/metrics/prometheus` | GET | Prometheus scrape target |
| `/pause` | POST | Pause replication (WAL position frozen) |
| `/resume` | POST | Resume replication |
| `/drain-stop` | POST | Graceful stop — flush pending events, then exit |

Example — graceful drain for planned maintenance:

```bash
curl -X POST http://dbmazz:8080/drain-stop
# wait for container to exit, then run docker compose up -d
```

### Upgrading dbmazz

1. Pin to an immutable version tag (e.g., `:1.5.3`).
2. `docker compose pull` to fetch the new image.
3. `docker compose up -d` to recreate the container.

Because checkpoints live in the source PostgreSQL, the new container
resumes exactly where the old one left off — no data loss, no
re-snapshot.

Downgrading works the same way as long as the checkpoint format is
compatible (patch and minor versions are always compatible; major
versions may not be — check the release notes).

## Troubleshooting

### Daemon fails to start with "relation does not exist"

dbmazz creates its internal tables (`dbmazz_checkpoints`,
`dbmazz_snapshot_state`, publication, replication slot) on first run.
The PG user in `SOURCE_URL` needs `CREATE TABLE` privilege plus the
`REPLICATION` role attribute.

### Replication slot disappears after a restart

If the container is killed before it can disconnect cleanly, the
replication slot remains but dbmazz will reconnect to it on the next
start. If you see "replication slot does not exist" errors on startup,
someone dropped the slot externally. dbmazz will re-create it and
start from `CURRENT_WAL_LSN`, so any events since the last checkpoint
are lost. Monitor the `dbmazz_checkpoint_lag_seconds` metric to catch
this early.

### `docker pull` returns "unauthorized"

The first time the image is published to GHCR it may be private. If
`docker pull ghcr.io/ez-cdc/dbmazz:latest` returns unauthorized, the
package needs to be marked public in the package settings of the
GitHub repo by a maintainer.

### Binary size or memory footprint

The dbmazz daemon binary is ~30 MB (musl-static). At steady state it
uses ~5 MB RSS. If you see much higher memory usage, check:

- `FLUSH_SIZE` set unusually high (batches sit in memory longer)
- Snapshot is running in parallel — snapshot workers each buffer one
  chunk of rows
- The sink is slow and events are backing up in the channel

## When to graduate to EZ-CDC Cloud

dbmazz as a self-hosted daemon is the right fit for:

- A single replication pipeline
- A team comfortable managing one long-running Docker container
- Embedding CDC inside existing tooling

If you need any of the following, consider
[EZ-CDC Cloud](https://ez-cdc.com):

- Multiple replication pipelines across databases
- Auto-healing, zero-downtime restarts, and centralized health
- A web portal for non-technical team members
- Fine-grained RBAC, audit logs, and encrypted config storage
- BYOC deployment with Terraform automation in your AWS/GCP account
- REST API + MCP server for automation and AI-native integrations
