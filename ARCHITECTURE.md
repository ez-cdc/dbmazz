# Arquitectura dbmazz

CDC de alto rendimiento: PostgreSQL → StarRocks.

---

## Diagrama de Flujo

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
│    gRPC     │◀───▶│   Engine    │  (orquestador del lifecycle)
│   Server    │     │             │
└─────────────┘     └─────────────┘
```

---

## Módulos

| Archivo/Carpeta | Responsabilidad |
|-----------------|-----------------|
| `main.rs` | Entry point minimalista (< 30 líneas) |
| `config.rs` | Carga de env vars centralizada |
| **`engine/mod.rs`** | **Orquestador del lifecycle CDC (INIT → SETUP → CDC)** |
| **`engine/setup/mod.rs`** | **Manager principal del setup automático** |
| **`engine/setup/postgres.rs`** | **Setup PostgreSQL (REPLICA IDENTITY, Publication, Slot)** |
| **`engine/setup/starrocks.rs`** | **Setup StarRocks (validación + columnas audit)** |
| **`engine/setup/error.rs`** | **Tipos de error descriptivos para control plane** |
| `source/postgres.rs` | Conexión y lectura del WAL stream |
| `source/parser.rs` | Parser zero-copy con SIMD para `pgoutput` |
| `sink/starrocks.rs` | Lógica de Stream Load a StarRocks |
| `sink/curl_loader.rs` | Cliente HTTP con libcurl (100-continue) |
| `pipeline/mod.rs` | Batching, backpressure, flush logic |
| `pipeline/schema_cache.rs` | Cache O(1) de schemas + schema evolution |
| `grpc/services.rs` | 4 servicios: Health, Control, Status, Metrics |
| `grpc/state.rs` | SharedState con atomics para métricas |
| `replication/wal_handler.rs` | Parsing de mensajes WAL (XLogData, KeepAlive) |
| `state_store.rs` | Persistencia de checkpoints en PostgreSQL |

---

## Dónde Colocar Código Nuevo

| Tipo de cambio | Ubicación |
|----------------|-----------|
| Nuevo source (MySQL, MongoDB) | `src/source/<name>.rs` + implementar trait |
| Nuevo sink (ClickHouse, Kafka) | `src/sink/<name>.rs` + implementar trait `Sink` |
| **Validación de setup** | `src/engine/setup/postgres.rs` o `src/engine/setup/starrocks.rs` |
| **Nuevo tipo de error setup** | `src/engine/setup/error.rs` → enum `SetupError` |
| **Lógica del engine** | `src/engine/mod.rs` (fase del lifecycle) |
| Nueva variable de entorno | Campo en `src/config.rs` → struct `Config` |
| Nuevo servicio gRPC | `src/grpc/services.rs` + `src/proto/dbmazz.proto` |
| Helper de parsing | Función en `src/source/parser.rs` |
| Transformación de datos | `src/pipeline/` (nuevo archivo si es complejo) |
| Lógica de WAL | `src/replication/wal_handler.rs` |
| Estado persistente | `src/state_store.rs` |

---

## Flujo de Datos

1. **WAL Reader** (`source/postgres.rs`)
   - Conecta a PostgreSQL con `replication=database`
   - Lee stream de replicación lógica

2. **Parser** (`source/parser.rs`)
   - Parsea protocolo `pgoutput` (Begin, Commit, Relation, Insert, Update, Delete)
   - Zero-copy con `bytes::Bytes`
   - SIMD para validación UTF-8

3. **Pipeline** (`pipeline/mod.rs`)
   - Acumula eventos en batches
   - Flush por tamaño (`FLUSH_SIZE`) o tiempo (`FLUSH_INTERVAL_MS`)
   - Backpressure via channel capacity

4. **Schema Cache** (`pipeline/schema_cache.rs`)
   - Cache O(1) de schemas por `relation_id`
   - Detecta nuevas columnas → schema evolution

5. **Sink** (`sink/starrocks.rs`)
   - Convierte a JSON con `sonic-rs`
   - Stream Load via HTTP con `curl`
   - Partial Update para columnas TOAST

6. **Checkpoint** (`state_store.rs`)
   - Persiste LSN en tabla `dbmazz_checkpoints`
   - Confirma a PostgreSQL con `StandbyStatusUpdate`

---

## Flujo de Setup Automático

El módulo `engine/setup/` maneja la configuración automática en el stage `SETUP`:

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

**Idempotencia**: Detecta recursos existentes (recovery mode) y continúa sin errores.

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

Si cualquier paso falla:
- Error descriptivo guardado en `SharedState`
- Health Check retorna `NOT_SERVING` con `errorDetail`
- gRPC server sigue corriendo para consultas del control plane

**Ejemplo**:
```rust
SetupError::PgTableNotFound { table: "orders" }
  ↓
errorDetail: "Table 'orders' not found in PostgreSQL. Verify the table exists..."
```

---

## Principios de Diseño

### Zero-copy

```rust
// ✅ Usar bytes::Bytes para slices sin copia
let val = data.split_to(len);  // Zero-copy slice
```

### SIMD Optimizations

- `memchr`: búsqueda de bytes O(n/32)
- `simdutf8`: validación UTF-8 con AVX2
- `sonic-rs`: JSON parsing SIMD
- Bitmap `u64` para TOAST: POPCNT, CTZ

### Async Everything

```rust
// Todo I/O es async con tokio
async fn send_batch(&self, rows: Vec<Row>) -> Result<()>
```

### Trait-based Extensibility

```rust
// Nuevos sinks implementan el trait
#[async_trait]
pub trait Sink: Send + Sync {
    async fn push_batch(&mut self, batch: &[CdcMessage], ...) -> Result<()>;
    async fn apply_schema_delta(&self, delta: &SchemaDelta) -> Result<()>;
}
```

### Configuración por Env Vars

```rust
// Todo configurable, nada hardcodeado
pub struct Config {
    pub database_url: String,      // DATABASE_URL
    pub flush_size: usize,         // FLUSH_SIZE
    pub flush_interval_ms: u64,    // FLUSH_INTERVAL_MS
    // ...
}
```

---

## Estructura de Directorios

```
src/
├── main.rs              # Entry point (delegación a engine)
├── config.rs            # Config::from_env()
├── engine.rs            # CdcEngine (orquestador)
├── state_store.rs       # Checkpoints
├── source/              # Fuentes de datos
│   ├── mod.rs
│   ├── postgres.rs      # PostgreSQL replication
│   └── parser.rs        # pgoutput parser
├── sink/                # Destinos
│   ├── mod.rs           # Trait Sink
│   ├── starrocks.rs     # StarRocks Stream Load
│   └── curl_loader.rs   # HTTP client
├── pipeline/            # Procesamiento
│   ├── mod.rs           # Batching + flush
│   └── schema_cache.rs  # Schema cache + evolution
├── grpc/                # API control
│   ├── mod.rs           # Server setup
│   ├── services.rs      # 4 servicios
│   └── state.rs         # SharedState
├── replication/         # WAL handling
│   ├── mod.rs
│   └── wal_handler.rs
└── proto/               # Protobuf
    └── dbmazz.proto
```

