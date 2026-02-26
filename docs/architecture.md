# dbmazz Architecture

High-performance CDC: PostgreSQL → StarRocks.

---

## Flow Diagram

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  PostgreSQL │────▶│   Source    │────▶│   Parser    │────▶│  Pipeline   │
│     WAL     │     │  (postgres) │     │   (SIMD)    │     │  (batching) │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                    ┌─────────────┐     ┌─────────────┐            │
                    │   Schema    │◀────│   Sink      │◀───────────┘
                    │    Cache    │     │ (StarRocks) │
                    └─────────────┘     └──────┬──────┘
                                               │
                    ┌─────────────┐            │
                    │ State Store │◀───────────┘
                    │ (checkpoint)│
                    └─────────────┘

┌─────────────┐     ┌─────────────┐
│    gRPC     │◀───▶│   Engine    │  (lifecycle orchestrator)
│   Server    │     │             │
└─────────────┘     └─────────────┘
```

---

## Modules

| File/Folder | Responsibility |
|-----------------|-----------------|
| `main.rs` | Minimalist entry point (< 30 lines) |
| `config.rs` | Centralized env var loading |
| **`engine/mod.rs`** | **CDC lifecycle orchestrator (INIT → SETUP → CDC)** |
| **`engine/setup/mod.rs`** | **Main automatic setup manager** |
| **`engine/setup/postgres.rs`** | **PostgreSQL setup (REPLICA IDENTITY, Publication, Slot)** |
| **`engine/setup/starrocks.rs`** | **StarRocks setup (validation + audit columns)** |
| **`engine/setup/error.rs`** | **Descriptive error types for control plane** |
| `source/postgres.rs` | Connection and WAL stream reading |
| `source/parser.rs` | Zero-copy parser with SIMD for `pgoutput` |
| `sink/starrocks.rs` | Stream Load logic to StarRocks |
| `sink/curl_loader.rs` | HTTP client with libcurl (100-continue) |
| `pipeline/mod.rs` | Batching, backpressure, flush logic |
| `pipeline/schema_cache.rs` | O(1) schema cache + schema evolution |
| `grpc/services.rs` | 4 services: Health, Control, Status, Metrics |
| `grpc/state.rs` | SharedState with atomics for metrics |
| `replication/wal_handler.rs` | Parsing of WAL messages (XLogData, KeepAlive) |
| `state_store.rs` | Checkpoint persistence in PostgreSQL |

---

## Where to Place New Code

| Type of change | Location |
|----------------|-----------|
| New source (MySQL, MongoDB) | `src/source/<name>.rs` + implement trait |
| New sink (ClickHouse, Kafka) | `src/sink/<name>.rs` + implement `Sink` trait |
| **Setup validation** | `src/engine/setup/postgres.rs` or `src/engine/setup/starrocks.rs` |
| **New setup error type** | `src/engine/setup/error.rs` → enum `SetupError` |
| **Engine logic** | `src/engine/mod.rs` (lifecycle phase) |
| New environment variable | Field in `src/config.rs` → struct `Config` |
| New gRPC service | `src/grpc/services.rs` + `src/proto/dbmazz.proto` |
| Parsing helper | Function in `src/source/parser.rs` |
| Data transformation | `src/pipeline/` (new file if complex) |
| WAL logic | `src/replication/wal_handler.rs` |
| Persistent state | `src/state_store.rs` |

---

## Data Flow

1. **WAL Reader** (`source/postgres.rs`)
   - Connects to PostgreSQL with `replication=database`
   - Reads logical replication stream

2. **Parser** (`source/parser.rs`)
   - Parses `pgoutput` protocol (Begin, Commit, Relation, Insert, Update, Delete)
   - Zero-copy with `bytes::Bytes`
   - SIMD for UTF-8 validation

3. **Pipeline** (`pipeline/mod.rs`)
   - Accumulates events in batches
   - Flush by size (`FLUSH_SIZE`) or time (`FLUSH_INTERVAL_MS`)
   - Backpressure via channel capacity

4. **Schema Cache** (`pipeline/schema_cache.rs`)
   - O(1) schema cache by `relation_id`
   - Detects new columns → schema evolution

5. **Sink** (`sink/starrocks.rs`)
   - Converts to JSON with `sonic-rs`
   - Stream Load via HTTP with `curl`
   - Partial Update for TOAST columns

6. **Checkpoint** (`state_store.rs`)
   - Persists LSN in `dbmazz_checkpoints` table
   - Confirms to PostgreSQL with `StandbyStatusUpdate`

---

## Automatic Setup Flow

The `engine/setup/` module handles automatic configuration in the `SETUP` stage:

### 1. PostgreSQL Setup (`engine/setup/postgres.rs`)

```
Verify Tables Exist
    ↓
Configure REPLICA IDENTITY FULL
    ↓
Create/Verify Publication
    ↓
Add Missing Tables to Publication
    ↓
Create/Verify Replication Slot
    ↓
✅ PostgreSQL Ready
```

**Idempotency**: Detects existing resources (recovery mode) and continues without errors.

### 2. StarRocks Setup (`engine/setup/starrocks.rs`)

```
Verify Connectivity
    ↓
Verify Tables Exist
    ↓
Get Existing Columns
    ↓
Add Missing Audit Columns:
  - dbmazz_op_type
  - dbmazz_is_deleted
  - dbmazz_synced_at
  - dbmazz_cdc_version
    ↓
✅ StarRocks Ready
```

### 3. Error Handling (`engine/setup/error.rs`)

If any step fails:
- Descriptive error saved in `SharedState`
- Health Check returns `NOT_SERVING` with `errorDetail`
- gRPC server keeps running for control plane queries

**Example**:
```rust
SetupError::PgTableNotFound { table: "orders" }
  ↓
errorDetail: "Table 'orders' not found in PostgreSQL. Verify the table exists..."
```

---

## Design Principles

### Zero-copy

```rust
// ✅ Use bytes::Bytes for slices without copying
let val = data.split_to(len);  // Zero-copy slice
```

### SIMD Optimizations

- `memchr`: byte search O(n/32)
- `simdutf8`: UTF-8 validation with AVX2
- `sonic-rs`: SIMD JSON parsing
- Bitmap `u64` for TOAST: POPCNT, CTZ

### Async Everything

```rust
// All I/O is async with tokio
async fn send_batch(&self, rows: Vec<Row>) -> Result<()>
```

### Trait-based Extensibility

```rust
// New sinks implement the trait
#[async_trait]
pub trait Sink: Send + Sync {
    async fn push_batch(&mut self, batch: &[CdcMessage], ...) -> Result<()>;
    async fn apply_schema_delta(&self, delta: &SchemaDelta) -> Result<()>;
}
```

### Configuration via Env Vars

```rust
// Everything configurable, nothing hardcoded
pub struct Config {
    pub database_url: String,      // DATABASE_URL
    pub flush_size: usize,         // FLUSH_SIZE
    pub flush_interval_ms: u64,    // FLUSH_INTERVAL_MS
    // ...
}
```

---

## Directory Structure

```
src/
├── main.rs              # Entry point (delegation to engine)
├── config.rs            # Config::from_env()
├── engine.rs            # CdcEngine (orchestrator)
├── state_store.rs       # Checkpoints
├── source/              # Data sources
│   ├── mod.rs
│   ├── postgres.rs      # PostgreSQL replication
│   └── parser.rs        # pgoutput parser
├── sink/                # Destinations
│   ├── mod.rs           # Sink trait
│   ├── starrocks.rs     # StarRocks Stream Load
│   └── curl_loader.rs   # HTTP client
├── pipeline/            # Processing
│   ├── mod.rs           # Batching + flush
│   └── schema_cache.rs  # Schema cache + evolution
├── grpc/                # Control API
│   ├── mod.rs           # Server setup
│   ├── services.rs      # 4 services
│   └── state.rs         # SharedState
├── replication/         # WAL handling
│   ├── mod.rs
│   └── wal_handler.rs
└── proto/               # Protobuf
    └── dbmazz.proto
```
