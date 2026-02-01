<p align="center">
  <a href="https://ez-cdc.com">
    <img src="assets/ez-cdc-logo.svg" alt="EZ-CDC" width="280">
  </a>
</p>

<h1 align="center">dbmazz</h1>

<p align="center">
  High-performance CDC daemon for streaming PostgreSQL changes to StarRocks
</p>

<p align="center">
  <a href="https://ez-cdc.com">Website</a> |
  <a href="#quick-start">Quick Start</a> |
  <a href="#configuration">Configuration</a> |
  <a href="#grpc-api">API</a> |
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

---

## Overview

**dbmazz** is a Rust-based Change Data Capture (CDC) daemon that enables real-time replication from PostgreSQL to StarRocks. It reads PostgreSQL's Write-Ahead Log (WAL) using logical replication and streams changes to StarRocks using the Stream Load API.

dbmazz is the core replication engine of the [EZ-CDC](https://ez-cdc.com) platform, a complete CDC solution with a BYOC (Bring Your Own Cloud) deployment model. While EZ-CDC provides a full control plane, web portal, and deployment automation, dbmazz can be used independently as a standalone CDC daemon.

---

## Features

- **Real-time CDC**: Streams INSERT, UPDATE, and DELETE operations with sub-second latency
- **Logical Replication**: Uses PostgreSQL's native logical replication protocol (pgoutput)
- **Exactly-once Delivery**: LSN-based checkpointing ensures no data loss or duplication
- **Schema Evolution**: Automatically detects and handles schema changes
- **Automatic Setup**: Zero-configuration deployment - creates publications, replication slots, and audit columns automatically
- **gRPC API**: Remote control and monitoring via gRPC with reflection support
- **Metrics**: Real-time throughput, lag, and health metrics
- **TOAST Support**: Efficiently handles large column values using StarRocks Partial Update
- **Soft Deletes**: Converts PostgreSQL DELETEs to soft deletes in StarRocks
- **High Performance**: SIMD-optimized parsing, zero-copy operations, and connection pooling

---

## Architecture

```
┌──────────────────┐
│   PostgreSQL     │
│                  │
│  Logical WAL     │
│  (pgoutput)      │
└────────┬─────────┘
         │
         │ Replication Protocol
         │
         ▼
┌──────────────────────────────────────────┐
│             dbmazz                       │
│                                          │
│  ┌──────────┐   ┌────────┐   ┌────────┐ │
│  │WAL Reader│──▶│Pipeline│──▶│  Sink  │ │
│  └──────────┘   └────────┘   └────────┘ │
│       ▲             │             │      │
│       │             │             │      │
│  ┌────┴─────┐  ┌───▼────┐   ┌────▼────┐ │
│  │Checkpoint│  │ Schema │   │ Metrics │ │
│  │  Store   │  │ Cache  │   │         │ │
│  └──────────┘  └────────┘   └─────────┘ │
│                                          │
│  ┌──────────────────────────────────┐   │
│  │       gRPC Server (Tonic)        │   │
│  │  - Health    - Control           │   │
│  │  - Metrics   - Status            │   │
│  └──────────────────────────────────┘   │
└──────────────────┬───────────────────────┘
                   │
                   │ Stream Load API
                   │
                   ▼
         ┌──────────────────┐
         │    StarRocks     │
         │                  │
         │  Target Tables   │
         │  + Audit Columns │
         └──────────────────┘
```

---

## Requirements

- **PostgreSQL**: Version 12 or higher with `wal_level = 'logical'`
- **StarRocks**: Version 2.5 or higher
- **Rust**: Version 1.70 or higher (for building from source)

---

## Quick Start

The fastest way to try dbmazz is using the included demo:

```bash
cd demo
./demo-start.sh
```

This will:
- Start PostgreSQL and StarRocks in Docker
- Create 3 sample tables with real-time replication
- Launch a metrics dashboard
- Process 300K+ events to demonstrate performance

To stop the demo:
```bash
./demo-stop.sh
```

For detailed demo documentation, see [demo/README.md](demo/README.md).

---

## Installation

### Build from Source

```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone the repository
git clone https://github.com/ez-cdc/dbmazz.git
cd dbmazz

# Build the release binary
cargo build --release

# Binary will be available at
./target/release/dbmazz
```

### PostgreSQL Configuration

Enable logical replication in PostgreSQL:

```sql
ALTER SYSTEM SET wal_level = 'logical';
-- Restart PostgreSQL for the change to take effect
```

All other PostgreSQL configuration (publications, replication slots, REPLICA IDENTITY) is handled automatically by dbmazz.

### StarRocks Configuration

Create your target tables in StarRocks with the basic schema:

```sql
CREATE TABLE my_table (
    id INT,
    name VARCHAR(100),
    email VARCHAR(255)
    -- ... your columns ...
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id);
```

dbmazz will automatically add the following audit columns:
- `dbmazz_op_type` (TINYINT): Operation type (0=INSERT, 1=UPDATE, 2=DELETE)
- `dbmazz_is_deleted` (BOOLEAN): Soft delete flag
- `dbmazz_synced_at` (DATETIME): CDC synchronization timestamp
- `dbmazz_cdc_version` (BIGINT): PostgreSQL LSN at time of change

---

## Configuration

dbmazz is configured entirely through environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | - | PostgreSQL connection string with `replication=database` parameter |
| `SLOT_NAME` | Yes | - | Replication slot name (created automatically if missing) |
| `PUBLICATION_NAME` | Yes | - | Publication name (created automatically if missing) |
| `TABLES` | Yes | - | Comma-separated list of tables to replicate (e.g., "orders,order_items") |
| `STARROCKS_URL` | Yes | - | StarRocks BE HTTP endpoint (e.g., "http://localhost:8040") |
| `STARROCKS_DB` | Yes | - | Target database name in StarRocks |
| `STARROCKS_USER` | Yes | - | StarRocks username |
| `STARROCKS_PASS` | No | "" | StarRocks password |
| `FLUSH_SIZE` | No | 1500 | Number of events per batch before flushing to StarRocks |
| `FLUSH_INTERVAL_MS` | No | 5000 | Maximum time (ms) between flushes |
| `GRPC_PORT` | No | 50051 | gRPC server port for control and monitoring |

### Example Configuration

```bash
# PostgreSQL source
export DATABASE_URL="postgres://user:pass@localhost:5432/mydb?replication=database"
export SLOT_NAME="dbmazz_slot"
export PUBLICATION_NAME="dbmazz_pub"
export TABLES="orders,order_items,customers"

# StarRocks target
export STARROCKS_URL="http://localhost:8040"
export STARROCKS_DB="analytics"
export STARROCKS_USER="root"
export STARROCKS_PASS="mypassword"

# Performance tuning (optional)
export FLUSH_SIZE="2000"
export FLUSH_INTERVAL_MS="3000"

# gRPC API (optional)
export GRPC_PORT="50051"

# Start the daemon
./target/release/dbmazz
```

---

## gRPC API

dbmazz exposes a gRPC API for remote control and monitoring. The server has **gRPC Reflection** enabled, so tools like `grpcurl` work without `.proto` files.

### Services

#### 1. HealthService

Check daemon health and lifecycle stage:

```bash
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check
```

Response (healthy):
```json
{
  "status": "SERVING",
  "stage": "STAGE_CDC",
  "stageDetail": "Replicating",
  "errorDetail": ""
}
```

Response (error):
```json
{
  "status": "NOT_SERVING",
  "stage": "STAGE_SETUP",
  "stageDetail": "Setup failed",
  "errorDetail": "Table 'orders' not found in PostgreSQL. Verify the table exists and is accessible."
}
```

Lifecycle stages:
- `STAGE_INIT`: Initializing daemon
- `STAGE_SETUP`: Configuring PostgreSQL and StarRocks
- `STAGE_CDC`: Actively replicating

#### 2. CdcControlService

Control daemon behavior:

```bash
# Pause replication
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Pause

# Resume replication
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Resume

# Hot-reload configuration
grpcurl -plaintext -d '{"flush_size": 2000}' localhost:50051 \
  dbmazz.CdcControlService/ReloadConfig

# Graceful shutdown
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/DrainAndStop
```

#### 3. CdcMetricsService

Stream real-time metrics:

```bash
# Stream metrics every 2 seconds
grpcurl -plaintext -d '{"interval_ms": 2000}' localhost:50051 \
  dbmazz.CdcMetricsService/StreamMetrics
```

Response:
```json
{
  "eventsPerSecond": 287.5,
  "lagBytes": "1024",
  "lagEvents": "15",
  "memoryBytes": "15360",
  "totalEventsProcessed": "150000",
  "totalBatchesSent": "100"
}
```

#### 4. CdcStatusService

Get current replication status:

```bash
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcStatusService/GetStatus
```

Response:
```json
{
  "state": "RUNNING",
  "currentLsn": "2610650456",
  "confirmedLsn": "2610596368",
  "pendingEvents": "10",
  "slotName": "dbmazz_slot",
  "tables": ["orders", "order_items"]
}
```

### API Discovery

Explore the API using gRPC reflection:

```bash
# List all services
grpcurl -plaintext localhost:50051 list

# Describe a service
grpcurl -plaintext localhost:50051 describe dbmazz.HealthService

# Describe a message type
grpcurl -plaintext localhost:50051 describe dbmazz.HealthCheckResponse
```

---

## Monitoring

### Metrics

dbmazz exposes the following metrics via gRPC:

| Metric | Description |
|--------|-------------|
| `eventsPerSecond` | Throughput of events processed |
| `lagBytes` | Replication lag in bytes |
| `lagEvents` | Number of events pending processing |
| `memoryBytes` | Current memory usage |
| `totalEventsProcessed` | Cumulative events processed since start |
| `totalBatchesSent` | Cumulative batches sent to StarRocks |

### Health Checks

Health checks return detailed error messages for troubleshooting:

```bash
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check
```

Common error scenarios:
- Table not found in PostgreSQL or StarRocks
- Connection failures
- Permission issues
- Replication slot conflicts

### Performance Characteristics

Measured under real-world conditions:

| Metric | Value |
|--------|-------|
| Throughput | 300K+ events processed |
| CPU Usage | ~25% (1 core) at 287 events/sec |
| Memory Usage | ~5MB |
| Replication Lag | <1KB under normal load |
| p99 Latency | <5 seconds |

---

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

This project is licensed under the Elastic License v2.0. See the [LICENSE](../LICENSE) file for details.

The Elastic License v2.0 allows you to:
- Use dbmazz for commercial and non-commercial purposes
- Modify and distribute the source code
- Use dbmazz as part of a larger application

With the following limitations:
- You may not provide dbmazz as a managed service to third parties
- You may not remove or obscure licensing, copyright, or trademark notices

For questions about licensing, please contact the project maintainers.

---

## Related Projects

dbmazz is the core replication engine of the **[EZ-CDC](https://ez-cdc.com)** platform:

| Component | Description |
|-----------|-------------|
| **[EZ-CDC Platform](https://ez-cdc.com)** | Complete CDC solution with control plane, web portal, and BYOC deployment |
| **worker-agent** | Rust agent that orchestrates dbmazz daemons in customer VPCs |
| **control-plane** | Go-based API server for managing CDC deployments |

If you need a complete managed CDC solution with multi-tenancy, authentication, and Terraform-based deployment, visit [ez-cdc.com](https://ez-cdc.com). If you need a standalone CDC daemon, dbmazz is the right choice.

---

## Support

- **Website**: [ez-cdc.com](https://ez-cdc.com)
- **Issues**: [GitHub Issues](https://github.com/ez-cdc/dbmazz/issues)
- **Documentation**: [ARCHITECTURE.md](ARCHITECTURE.md) | [CONTRIBUTING.md](CONTRIBUTING.md)
