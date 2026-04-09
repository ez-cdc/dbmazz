# Architecture

## Overview

dbmazz is a CDC (Change Data Capture) daemon written in Rust. It reads the PostgreSQL Write-Ahead Log (WAL) via logical replication and streams changes to any supported sink. Each instance handles one replication job.

```
PostgreSQL (source)             dbmazz                          Sink (target)
┌──────────────┐             ┌────────────────────┐          ┌──────────────┐
│ WAL          │   logical   │ WAL Handler        │          │ StarRocks    │
│ (INSERT,     │  replication│   │                │          │ PostgreSQL   │
│  UPDATE,     │ ──────────▶ │   ▼                │          │ Snowflake    │
│  DELETE)     │  (pgoutput) │ source/converter   │          │ (+ future)   │
│              │             │   │                │          │              │
│              │             │   ▼                │          │              │
│              │             │ Pipeline           │  write   │              │
│              │             │   │ batch + flush   │──batch──▶│              │
│              │             │   ▼                │          │              │
│              │             │ Checkpoint (LSN)   │          │              │
│              │ ◀───────────│   confirm to PG    │          │              │
└──────────────┘             └────────────────────┘          └──────────────┘
                                   ~5 MB RAM
                                   <1s latency
```

## Data Flow — Step by Step

### 1. Setup Phase

```
Engine::run()
  ├── Start gRPC server (health checks must respond immediately)
  ├── Connect to source PostgreSQL
  │     ├── Verify tables exist
  │     ├── Set REPLICA IDENTITY FULL
  │     ├── Create publication (dbmazz_pub)
  │     └── Create replication slot (dbmazz_slot)
  ├── Connect to sink
  │     └── validate_connection() + sink-specific setup
  ├── Load checkpoint (last confirmed LSN)
  ├── Start replication stream from checkpoint LSN
  └── Initialize Pipeline
```

### 2. CDC Phase (main loop)

The engine reads WAL messages and processes them through three stages:

**Stage A — WAL Handler** (`replication/wal_handler.rs`)
```
pgoutput bytes
  ├── Parse: PgOutputParser → CdcMessage (PG-specific)
  ├── Update SchemaCache (on Relation messages)
  ├── Snapshot dedup: should_emit() check (if snapshot active)
  └── Convert: CdcMessage → CdcRecord (generic, via source/converter.rs)
```

**Stage B — Pipeline** (`pipeline/mod.rs`)
```
PipelineEvent { lsn, record: CdcRecord }
  ├── Accumulate records into batch (Vec<CdcRecord>)
  ├── Flush when: batch.len() >= FLUSH_SIZE OR timeout >= FLUSH_INTERVAL_MS
  ├── Call sink.write_batch(records)
  ├── On success: send LSN to feedback channel
  └── On failure: set state to Stopped, halt pipeline
```

**Stage C — Checkpoint** (`engine/mod.rs`)
```
feedback_rx receives confirmed LSN
  ├── Save checkpoint to StateStore (PostgreSQL table)
  ├── Update SharedState.confirmed_lsn
  └── Send StandbyStatusUpdate to PostgreSQL
      (PG can now discard WAL up to this LSN)
```

**Critical invariant**: checkpoint is saved BEFORE confirming to PostgreSQL. If we confirm first and crash, the WAL is gone but we haven't persisted — permanent data loss.

### 3. Snapshot Phase (concurrent with CDC)

Runs in parallel with the WAL consumer. Uses the Flink CDC concurrent snapshot algorithm:

```
For each table:
  chunk_table() → [chunk(0, 50000), chunk(50000, 100000), ...]

For each chunk (N workers in parallel):
  1. Emit low-watermark (LW) via pg_logical_emit_message
  2. SELECT * FROM table WHERE pk >= start AND pk < end
  3. Emit high-watermark (HW) → captures LSN
  4. sink.write_batch(rows as CdcRecord::Insert)
  5. Mark chunk complete in dbmazz_snapshot_state
  6. Register (start_pk, end_pk, hw_lsn) in SharedState

WAL consumer deduplication:
  For each Insert/Update event during snapshot:
    if event.pk is in a completed chunk AND event.lsn <= chunk.hw_lsn:
      → suppress (already loaded by snapshot)
    else:
      → emit normally
```

Progress is tracked in PostgreSQL (`dbmazz_snapshot_state` table), so interrupted snapshots resume from the last completed chunk.

## Key Types

### CdcRecord (the generic boundary)

Everything upstream of CdcRecord is PostgreSQL-specific. Everything downstream is generic.

```rust
enum CdcRecord {
    Insert { table, columns: Vec<ColumnValue>, position },
    Update { table, old_columns, new_columns, position },
    Delete { table, columns, position },
    SchemaChange { table, columns: Vec<ColumnDef>, position },
    Begin { xid },
    Commit { xid, position, commit_timestamp_us },
    Heartbeat { position },
}

enum Value {
    Null, Bool, Int64, Float64, String, Bytes,
    Json, Timestamp, Decimal, Uuid, Unchanged,
}
```

`Value::Unchanged` represents TOAST columns that weren't modified (PostgreSQL doesn't include them in the WAL for partial updates).

### Sink trait

```rust
trait Sink: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> SinkCapabilities;
    async fn validate_connection(&self) -> Result<()>;
    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> { Ok(()) }
    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult>;
    async fn close(&mut self) -> Result<()>;
}
```

6 methods, 1 with default. Modeled after Kafka Connect (start + put + flush + stop). The sink is fully responsible for its loading strategy — the engine doesn't know about Stream Load, COPY protocol, S3 staging, etc.

## Module Map

```
src/
├── main.rs                          Entry point
├── config.rs                        Environment variable loading
├── core/                            Generic abstractions (sink-agnostic)
│   ├── record.rs                    CdcRecord, Value, ColumnValue, TableRef
│   ├── traits.rs                    Sink trait, SinkCapabilities, SourceTableSchema
│   ├── position.rs                  SourcePosition (LSN, offset)
│   └── error.rs                     Error types
├── source/                          Source layer (PostgreSQL-specific)
│   ├── converter.rs                 CdcMessage → CdcRecord conversion
│   ├── parser.rs                    pgoutput protocol parser
│   └── postgres.rs                  Replication connection management
├── pipeline/                        Generic batching + dispatch
│   ├── mod.rs                       Pipeline loop (batch → sink.write_batch)
│   └── schema_cache.rs             Table schema tracking
├── replication/
│   └── wal_handler.rs              WAL message processing + dedup
├── engine/                          Orchestration
│   ├── mod.rs                       CdcEngine (setup → CDC → shutdown)
│   ├── setup/                       Source setup (sink setup is via Sink::setup())
│   │   ├── mod.rs                   SetupManager (source-side only)
│   │   ├── postgres.rs              Replication slot, publication
│   │   └── error.rs                 Setup error types
│   └── snapshot/                    Flink CDC concurrent snapshot
│       ├── mod.rs                   Snapshot coordinator
│       ├── worker.rs                Chunk processing + sink write
│       ├── chunker.rs               PK-range chunking
│       ├── state_store.rs           dbmazz_snapshot_state table
│       └── utils.rs                 Snapshot utilities
├── connectors/
│   └── sinks/
│       ├── mod.rs                   create_sink() factory
│       ├── starrocks/               StarRocks sink
│       │   ├── mod.rs               StarRocksSink (JSON → Stream Load HTTP)
│       │   ├── config.rs            StarRocksSinkConfig
│       │   ├── setup.rs             Table verification, audit columns
│       │   ├── stream_load.rs       HTTP Stream Load client
│       │   └── types.rs             PG → StarRocks type mapping
│       ├── postgres/                PostgreSQL sink
│       │   ├── mod.rs               PostgresSink (COPY → raw table → MERGE)
│       │   ├── raw_table.rs         COPY writer for raw staging table
│       │   ├── normalizer.rs        Async MERGE loop (raw table → target)
│       │   ├── merge_generator.rs   Dynamic MERGE SQL generation
│       │   ├── setup.rs             DDL: raw table, metadata, target tables
│       │   └── types.rs             PG → PG type mapping
│       └── snowflake/               Snowflake sink
│           ├── mod.rs               SnowflakeSink (Parquet → PUT → COPY INTO → MERGE)
│           ├── client.rs            HTTP client (password + JWT auth, SQL execution)
│           ├── config.rs            SnowflakeSinkConfig
│           ├── parquet_writer.rs    CdcRecord → Arrow → Parquet (in-memory)
│           ├── stage.rs             PUT protocol + S3/GCS/Azure upload
│           ├── setup.rs             DDL: TRANSIENT schema, stage, raw table, targets
│           ├── merge_generator.rs   MERGE SQL with VARIANT extraction + TOAST
│           ├── normalizer.rs        Async MERGE loop (raw table → target)
│           └── types.rs             PG → Snowflake type mapping
├── grpc/                            gRPC server
│   ├── mod.rs                       gRPC server startup
│   ├── state.rs                     SharedState (metrics, dedup, control)
│   ├── services.rs                  Health, Control, Status, Metrics
│   └── cpu_metrics.rs               CPU usage metrics collection
├── state_store.rs                   LSN checkpoint persistence
└── utils.rs                         SQL validation, type helpers
```

## Adding a New Sink

### Code (3 steps)

```
1. Create src/connectors/sinks/my_sink/
   ├── mod.rs      Implement Sink trait (6 methods, including setup())
   └── config.rs   MySinkConfig

2. Add SinkType::MySink to src/config.rs

3. Add match arm in create_sink() (src/connectors/sinks/mod.rs)

CDC and snapshot work automatically via write_batch().
Sink-specific setup (table creation, audit columns, etc.) goes in
the Sink::setup() trait method — the engine calls it after source
setup. SetupManager handles source-side setup only (replication
slot, publication).
```

### Testing (3 steps)

```
1. Add a profile to deploy/docker-compose.yml:
   - Target database service (with healthcheck)
   - dbmazz-my-sink service (with SINK_TYPE + connection env vars)

2. Add test_my_sink() to deploy/test-sink.sh:
   - Verify snapshot replicated seed data
   - Verify CDC INSERT, UPDATE, DELETE

3. Run:
   docker compose --profile my-sink up -d
   deploy/test-sink.sh my-sink
```

## Concurrency Model

- **tokio async runtime** — single-threaded by default, multi-threaded for snapshot workers
- **Pipeline**: single task, receives from WAL handler via mpsc channel
- **Snapshot**: N parallel workers (semaphore-bounded), each with its own PG connection and sink instance
- **gRPC server**: spawned as background task
- **SharedState**: `Arc` with atomics for counters, `RwLock` for schema/dedup state

## Checkpointing & Recovery

- LSN checkpoints stored in PostgreSQL (`dbmazz_checkpoints` table in the source DB)
- Checkpoint saved BEFORE confirming to PostgreSQL (critical for at-least-once delivery)
- On restart: load last checkpoint, start replication from that LSN
- Snapshot progress in `dbmazz_snapshot_state` — completed chunks are skipped on resume
