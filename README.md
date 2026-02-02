# dbmazz

High-performance CDC daemon with pluggable source and sink connectors

---

## Overview

dbmazz is a Rust-based Change Data Capture (CDC) daemon that enables real-time replication between databases. It uses a pluggable connector architecture where sources and sinks can be easily added to support new databases.

dbmazz is a core component of the EZ-CDC platform, a complete CDC solution with a BYOC (Bring Your Own Cloud) deployment model. While EZ-CDC provides a full control plane, web portal, and deployment automation, dbmazz can be used independently as a standalone CDC daemon.

## Supported Connectors

### Sources

| Source | CDC Method | Status | Documentation |
|--------|------------|--------|---------------|
| PostgreSQL | Logical Replication (pgoutput) | Stable | [postgres/README.md](src/connectors/sources/postgres/README.md) |

### Sinks

| Sink | Loading Method | Status | Documentation |
|------|----------------|--------|---------------|
| StarRocks | Stream Load HTTP API | Stable | [starrocks/README.md](src/connectors/sinks/starrocks/README.md) |

Want to add a new connector? See [CONTRIBUTING_CONNECTORS.md](CONTRIBUTING_CONNECTORS.md) for a complete guide.

---

## Features

- **Pluggable Architecture**: Trait-based connectors for easy addition of new sources and sinks
- **Real-time CDC**: Streams INSERT, UPDATE, and DELETE operations with sub-second latency
- **Exactly-once Delivery**: Position-based checkpointing ensures no data loss or duplication
- **Schema Evolution**: Automatically detects and handles schema changes
- **Automatic Setup**: Zero-configuration deployment - creates publications, replication slots, and audit columns automatically
- **gRPC API**: Remote control and monitoring via gRPC with reflection support
- **Metrics**: Real-time throughput, lag, and health metrics
- **Normalized Data Model**: Database-agnostic CdcRecord format for interoperability
- **High Performance**: SIMD-optimized parsing, zero-copy operations, and connection pooling

---

## Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Source Connectors                            │
│  (Postgres, MySQL, Oracle, etc.)                                │
│                                                                  │
│  Each implements: Source trait                                  │
│  Each emits: CdcRecord                                          │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ CdcRecord (normalized format)
                 │
                 ▼
        ┌─────────────────┐
        │   Pipeline      │
        │                 │
        │ - Batching      │
        │ - Schema cache  │
        │ - Checkpointing │
        └─────────┬───────┘
                  │
                  │ CdcRecord batches
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Sink Connectors                              │
│  (StarRocks, ClickHouse, Snowflake, etc.)                       │
│                                                                  │
│  Each implements: Sink trait                                    │
│  Each accepts: Vec<CdcRecord>                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Pluggable Connector Architecture

dbmazz uses a trait-based architecture for connectors:

- **Source Trait**: Defines interface for reading CDC events from databases
- **Sink Trait**: Defines interface for writing CDC events to databases
- **CdcRecord**: Normalized, database-agnostic representation of change events
- **Factory Pattern**: Sources and sinks are instantiated via factory functions

All connectors exchange data using the `CdcRecord` format, which includes:
- INSERT, UPDATE, DELETE operations
- Schema changes
- Transaction boundaries (BEGIN/COMMIT)
- Heartbeats for lag tracking

This design allows:
- Easy addition of new sources and sinks
- Mix-and-match any source with any sink
- Protocol and type-mapping isolation
- Testability and maintainability

For details on adding connectors, see [CONTRIBUTING_CONNECTORS.md](CONTRIBUTING_CONNECTORS.md).

### Detailed Architecture (PostgreSQL → StarRocks Example)

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
│  │  Source  │──▶│Pipeline│──▶│  Sink  │ │
│  │ (Trait)  │   │        │   │(Trait) │ │
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

- **Source Database**: See connector-specific requirements in [Sources](src/connectors/sources/)
- **Sink Database**: See connector-specific requirements in [Sinks](src/connectors/sinks/)
- **Rust**: Version 1.70 or higher (for building from source)

### Example: PostgreSQL → StarRocks

- **PostgreSQL**: Version 12 or higher with `wal_level = 'logical'`
- **StarRocks**: Version 2.5 or higher

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
git clone https://github.com/your-org/dbmazz.git
cd dbmazz

# Build the release binary
cargo build --release

# Binary will be available at
./target/release/dbmazz
```

### Source Configuration

Configuration depends on which source connector you're using. See connector-specific documentation:

- [PostgreSQL Source Configuration](src/connectors/sources/postgres/README.md)

### Sink Configuration

Configuration depends on which sink connector you're using. See connector-specific documentation:

- [StarRocks Sink Configuration](src/connectors/sinks/starrocks/README.md)

---

## Configuration

dbmazz is configured entirely through environment variables. The configuration uses a generic format that works with any source/sink connector combination.

### Common Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOURCE_TYPE` | No | `postgres` | Source connector type (postgres, mysql, etc.) |
| `SOURCE_URL` | Yes | - | Source database connection URL |
| `SINK_TYPE` | No | `starrocks` | Sink connector type (starrocks, clickhouse, etc.) |
| `SINK_URL` | Yes | - | Sink database connection URL |
| `SINK_DATABASE` | Yes | - | Target database name |
| `TABLES` | Yes | - | Comma-separated list of tables to replicate |
| `FLUSH_SIZE` | No | 10000 | Number of events per batch |
| `FLUSH_INTERVAL_MS` | No | 5000 | Maximum time (ms) between flushes |
| `GRPC_PORT` | No | 50051 | gRPC server port for control and monitoring |

### Connector-Specific Variables

Each connector may have additional configuration variables. See connector documentation:

- **PostgreSQL Source**: `SOURCE_SLOT_NAME`, `SOURCE_PUBLICATION_NAME` ([details](src/connectors/sources/postgres/README.md))
- **StarRocks Sink**: `SINK_PORT`, `SINK_USER`, `SINK_PASSWORD` ([details](src/connectors/sinks/starrocks/README.md))

### Example Configuration (PostgreSQL → StarRocks)

```bash
# Source configuration
export SOURCE_TYPE="postgres"
export SOURCE_URL="postgres://user:pass@localhost:5432/mydb"
export SOURCE_SLOT_NAME="dbmazz_slot"
export SOURCE_PUBLICATION_NAME="dbmazz_pub"

# Sink configuration
export SINK_TYPE="starrocks"
export SINK_URL="http://localhost:8040"
export SINK_PORT="9030"
export SINK_DATABASE="analytics"
export SINK_USER="root"
export SINK_PASSWORD="mypassword"

# Tables and performance
export TABLES="orders,order_items,customers"
export FLUSH_SIZE="2000"
export FLUSH_INTERVAL_MS="3000"

# Optional: gRPC API
export GRPC_PORT="50051"

# Start the daemon
./target/release/dbmazz
```

### Legacy Environment Variables

For backward compatibility, the following legacy variable names are still supported but deprecated:

- `DATABASE_URL` → Use `SOURCE_URL`
- `SLOT_NAME` → Use `SOURCE_SLOT_NAME`
- `PUBLICATION_NAME` → Use `SOURCE_PUBLICATION_NAME`
- `STARROCKS_URL` → Use `SINK_URL`
- `STARROCKS_DB` → Use `SINK_DATABASE`
- `STARROCKS_USER` → Use `SINK_USER`
- `STARROCKS_PASS` → Use `SINK_PASSWORD`

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

Contributions are welcome!

- **General contributions**: See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines
- **Adding new connectors**: See [CONTRIBUTING_CONNECTORS.md](CONTRIBUTING_CONNECTORS.md) for a complete guide to adding source and sink connectors

---

## License

This project is licensed under the Elastic License v2.0. See the [LICENSE](../LICENSE) file for details.

The Elastic License v2.0 allows you to:
- Use dbmazz for commercial and non-commercial purposes
- Modify and distribute the source codevamos con opcion 1

- Use dbmazz as part of a larger application

With the following limitations:
- You may not provide dbmazz as a managed service to third parties
- You may not remove or obscure licensing, copyright, or trademark notices

For questions about licensing, please contact the project maintainers.

---

## Related Projects

dbmazz is part of the **EZ-CDC** ecosystem:

- **[EZ-CDC](https://github.com/your-org/ez-cdc)**: Complete CDC platform with control plane, web portal, and BYOC deployment automation
- **worker-agent**: Rust agent that orchestrates dbmazz daemons in customer VPCs
- **control-plane**: Go-based API server for managing CDC deployments

If you need a complete managed CDC solution with multi-tenancy, authentication, and Terraform-based deployment, check out EZ-CDC. If you need a standalone CDC daemon, dbmazz is the right choice.

---

## Support

For questions, bug reports, or feature requests, please open an issue on GitHub.
