# Fase 0: Refactor de Arquitectura para Multi-Sink — ✅ COMPLETADA

Branch: `feat/multi-sink-architecture`
Resultado: -752 lineas, +270 lineas. 72 tests pasan, clippy limpio.

## Objetivo

Reestructurar dbmazz para que agregar un sink nuevo sea autocontenido y el engine sea completamente sink-agnostic. Este refactor es prerequisito de todos los sinks futuros.

## Estado actual — Problemas

### 1. Dos traits de sink redundantes

```
sink::Sink (legacy)          — recibe CdcMessage (pgoutput-specific)
core::Sink (nuevo)           — recibe CdcRecord (generico)
NewSinkAdapter               — puente entre ambos
```

Pipeline usa el legacy. StarRocksSink implementa el nuevo. El adapter traduce en runtime. Esto agrega complejidad, indirection, y acopla el pipeline a pgoutput.

### 2. Snapshot hardcodeado a StarRocks

`engine/snapshot/worker.rs` lineas 80-87 y 396-412:
```rust
// Construye StreamLoadClient directamente
let sl_client = Arc::new(StreamLoadClient::new(...));

// Envia directo a StarRocks, sin pasar por el Sink trait
sl_client.send(dest_table, body_arc, StreamLoadOptions::default()).await?;
```

Cada sink nuevo requiere reescribir el snapshot worker.

### 3. Setup hardcodeado a StarRocks

`engine/setup/mod.rs` linea 34:
```rust
let sr_setup = starrocks::StarRocksSetup::new(&pool, &self.config);
sr_setup.run().await?;
```

Setup de StarRocks esta en el engine. Deberia ser interno al sink.

### 4. Pipeline recibe tipos PG-specific

`pipeline/mod.rs` linea 13:
```rust
pub struct Pipeline {
    rx: mpsc::Receiver<CdcEvent>,  // CdcEvent contiene CdcMessage (pgoutput)
    ...
    sink: Box<dyn Sink + Send>,    // Sink legacy que recibe CdcMessage
}
```

CdcMessage tiene `relation_id: u32`, `Tuple`, y otros conceptos de pgoutput. Si manana el source es MySQL, Pipeline no lo puede manejar sin cambios.

---

## Arquitectura propuesta

### Nuevo Sink trait (core::Sink v2)

```rust
// src/core/traits.rs

/// Schema de una tabla source — informacion que el sink necesita para setup
#[derive(Debug, Clone)]
pub struct SourceTableSchema {
    pub schema: String,
    pub name: String,
    pub columns: Vec<SourceColumn>,
    pub primary_keys: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SourceColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub pg_type_id: u32,      // OID original (util para sinks que hablan PG)
}

#[async_trait]
pub trait Sink: Send + Sync {
    /// Nombre del sink ("starrocks", "postgres", "snowflake")
    fn name(&self) -> &'static str;

    /// Capacidades del sink
    fn capabilities(&self) -> SinkCapabilities;

    /// Validar que la conexion al destino funciona
    async fn validate_connection(&self) -> Result<()>;

    /// Setup: crear tablas destino, raw tables, stages, etc.
    /// Recibe schemas de las tablas source para generar DDL.
    /// Se llama UNA VEZ al inicio, antes de CDC y snapshot.
    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()>;

    /// CDC: escribir un batch de cambios.
    /// El sink decide internamente como aplicarlos (Stream Load, COPY, stage, etc).
    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult>;

    /// Snapshot: escribir filas bulk.
    /// Default: convierte a CdcRecord::Insert y llama write_batch.
    /// Sinks especializados override para COPY, Stream Load, etc.
    async fn write_snapshot_rows(
        &mut self,
        table: &TableRef,
        rows: Vec<Vec<ColumnValue>>,
        position: SourcePosition,
    ) -> Result<u64> {
        let records: Vec<CdcRecord> = rows
            .into_iter()
            .map(|cols| CdcRecord::Insert {
                table: table.clone(),
                columns: cols,
                position: position.clone(),
            })
            .collect();
        let result = self.write_batch(records).await?;
        Ok(result.records_written as u64)
    }

    /// Flush: esperar que operaciones async internas terminen.
    /// Ej: PostgreSQL espera que el normalizer procese batches pendientes.
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    /// Cerrar conexiones y limpiar recursos.
    async fn close(&mut self) -> Result<()>;
}
```

### Pipeline generico

Pipeline pasa de recibir `CdcEvent` (pgoutput) a recibir `CdcRecord` (generico):

```rust
// src/pipeline/mod.rs

pub struct Pipeline {
    rx: mpsc::Receiver<CdcRecord>,     // CAMBIADO: era CdcEvent
    sink: Box<dyn Sink + Send>,        // CAMBIADO: usa core::Sink directo
    batch_size: usize,
    batch_timeout: Duration,
    feedback_tx: Option<mpsc::Sender<u64>>,
    shared_state: Option<Arc<SharedState>>,
}
```

La conversion de `CdcMessage → CdcRecord` se mueve al source layer.

### Source produce CdcRecord

La logica que hoy esta en `NewSinkAdapter::convert_batch()` se mueve a un nuevo modulo del source:

```rust
// src/source/converter.rs (nuevo)

/// Convierte CdcMessage (pgoutput) a CdcRecord (generico).
/// Esta logica estaba en sink/adapter.rs pero pertenece al source.
pub fn convert_message(
    msg: &CdcMessage,
    schema_cache: &SchemaCache,
    lsn: u64,
) -> Option<CdcRecord> {
    // Misma logica que NewSinkAdapter::convert_message()
    // Movida aqui sin cambios funcionales
}
```

El WAL handler llama a este converter antes de enviar al pipeline.

### Engine sink-agnostic

```rust
// src/engine/mod.rs

impl CdcEngine {
    pub async fn run(mut self) -> Result<()> {
        self.start_grpc_server();
        self.init_state_store().await?;

        // Setup: source prepara su lado, sink prepara el suyo
        self.run_source_setup().await?;
        let source_schemas = self.introspect_source_schemas().await?;
        self.sink.setup(&source_schemas).await?;

        let start_lsn = self.load_checkpoint().await?;
        let source = self.init_source().await?;
        let replication_stream = source.start_replication_from(start_lsn).await?;

        // Validate sink connection
        self.sink.validate_connection().await?;

        // Pipeline: recibe CdcRecord, escribe via sink.write_batch()
        let (tx, feedback_rx) = self.init_pipeline();

        // Snapshot: usa sink.write_snapshot_rows()
        if self.config.do_snapshot {
            self.spawn_snapshot_worker().await;
        }

        // Main loop
        self.run_main_loop(replication_stream, tx, feedback_rx, start_lsn).await?;

        // Cleanup
        self.sink.flush().await?;
        self.sink.close().await?;
        Ok(())
    }
}
```

---

## Plan de PRs

### PR 0-A: Evolucionar core::Sink trait

**Archivos a modificar:**

`src/core/traits.rs`:
- Agregar `SourceTableSchema`, `SourceColumn` structs
- Agregar `setup()` method al trait (required)
- Agregar `write_snapshot_rows()` method con default impl
- Agregar `flush()` method con default impl (no-op)

`src/core/record.rs`:
- Sin cambios — CdcRecord ya es generico

`src/core/mod.rs`:
- Re-exportar nuevos tipos

**Tests:**
- Compilacion limpia
- Mock sink implementa los nuevos methods

---

### PR 0-B: Mover conversion CdcMessage → CdcRecord al source

**Archivos nuevos:**

`src/source/converter.rs`:
- Mover logica de `NewSinkAdapter::convert_message()` aqui
- Mover logica de `NewSinkAdapter::convert_pg_value()` aqui
- Mover `pg_type_to_data_type()` aqui
- Mover `tuple_to_column_values()` aqui

**Archivos a modificar:**

`src/replication/wal_handler.rs`:
- En `handle_xlog_data()`: despues de parsear CdcMessage, convertir a CdcRecord
- Enviar CdcRecord por el channel (en vez de CdcEvent con CdcMessage)

`src/pipeline/mod.rs`:
- Cambiar `rx: mpsc::Receiver<CdcEvent>` a `rx: mpsc::Receiver<CdcRecord>`
- `flush_batch()` llama `sink.write_batch(batch)` directamente (sin adapter)
- Mover deteccion de schema changes: SchemaCache se actualiza en el source (antes de convertir), no en el pipeline

`src/pipeline/schema_cache.rs`:
- SchemaCache sigue existiendo pero se usa en el source layer
- La deteccion de schema deltas se mantiene, pero el sink los recibe como CdcRecord::SchemaChange

**Archivos a eliminar:**

`src/sink/adapter.rs` — toda la logica se fue a source/converter.rs
`src/sink/mod.rs` — el legacy Sink trait ya no existe

**Tests:**
- Conversion de CdcMessage a CdcRecord produce mismos resultados (regression)
- Pipeline recibe CdcRecord y los pasa al sink
- Tests de convert_pg_value() se mueven a source/converter.rs

---

### PR 0-C: Migrar StarRocksSink a nuevo trait + setup

**Archivos a modificar:**

`src/connectors/sinks/starrocks/mod.rs`:
- Implementar `setup()`: mover logica de `engine/setup/starrocks.rs` aqui
  - Verificar tablas existen
  - Asegurar audit columns
  - El StarRocksSink es dueno de su setup
- Implementar `write_snapshot_rows()` override:
  - Mover `serialize_text_rows_to_json()` desde `engine/snapshot/worker.rs`
  - Mover logica de StreamLoad send

`src/engine/setup/mod.rs`:
- Eliminar llamada a `starrocks::StarRocksSetup`
- Setup del sink ahora es via `self.sink.setup(schemas)`
- Solo queda el setup del source (PostgreSQL: replication slot, publication)

`src/engine/setup/starrocks.rs`:
- Eliminar (logica movida a StarRocksSink::setup())

**Tests:**
- StarRocksSink::setup() crea audit columns (mismo comportamiento)
- StarRocksSink::write_snapshot_rows() produce mismo output que serialize_text_rows_to_json

---

### PR 0-D: Snapshot worker generico

**Archivos a modificar:**

`src/engine/snapshot/worker.rs`:

Cambiar `run_snapshot()`:
```rust
// ANTES: construye StreamLoadClient internamente
let sl_client = Arc::new(StreamLoadClient::new(...));

// DESPUES: recibe el sink como parametro
pub async fn run_snapshot(
    config: Arc<Config>,
    shared_state: Arc<SharedState>,
    sink: Arc<Mutex<Box<dyn Sink>>>,
) -> Result<()>
```

Cambiar `process_chunk()`:
```rust
// ANTES: serializa a JSON y envia via StreamLoad
let body = serialize_text_rows_to_json(&rows, col_names, &synced_at, hw_lsn)?;
sl_client.send(dest_table, body_arc, StreamLoadOptions::default()).await?;

// DESPUES: convierte rows a ColumnValue y llama al sink
let col_values: Vec<Vec<ColumnValue>> = rows.iter()
    .map(|row| row_to_column_values(row, col_names))
    .collect();
let table_ref = TableRef::new(Some(schema.to_string()), dest_table.to_string());
let position = SourcePosition::Lsn(hw_lsn);
sink.lock().await.write_snapshot_rows(&table_ref, col_values, position).await?;
```

Nueva funcion helper:
```rust
/// Convertir una Row de tokio_postgres (con columnas ::text) a Vec<ColumnValue>
fn row_to_column_values(
    row: &tokio_postgres::Row,
    col_names: &[String],
) -> Vec<ColumnValue> {
    col_names.iter().enumerate()
        .map(|(i, name)| {
            let val: Option<String> = row.get(i);
            let value = match val {
                Some(s) => Value::String(s),
                None => Value::Null,
            };
            ColumnValue::new(name.clone(), value)
        })
        .collect()
}
```

Eliminar de worker.rs:
- `serialize_text_rows_to_json()` — movido a StarRocksSink
- `write_json_escaped()` — movido a StarRocksSink
- Import de StreamLoadClient — ya no se usa

`src/engine/mod.rs`:
- `run()`: pasar el sink al snapshot worker
- El sink se comparte entre pipeline y snapshot via Arc<Mutex<>>
  (o alternativamente, crear un segundo sink instance para snapshot)

**Decisión de diseño: compartir o duplicar el sink?**

Opcion A — `Arc<Mutex<dyn Sink>>` compartido:
- Pro: un solo recurso, estado consistente
- Con: mutex contention entre pipeline y snapshot worker

Opcion B — dos instancias del sink (una para CDC, otra para snapshot):
- Pro: zero contention, snapshot no bloquea CDC
- Con: dos conexiones al destino, estado separado

**Recomendacion: Opcion B.** El snapshot worker es temporal (termina cuando acaba), y tener dos conexiones es normal. El engine crea dos sinks via `create_sink()`. El pipeline usa uno, el snapshot worker usa otro. Sin mutex.

```rust
// En engine/mod.rs
let cdc_sink = create_sink(&self.config.sink)?;
let snapshot_sink = create_sink(&self.config.sink)?;

// CDC pipeline usa cdc_sink
let pipeline = Pipeline::new(rx, cdc_sink, ...);

// Snapshot usa snapshot_sink
if self.config.do_snapshot {
    tokio::spawn(async move {
        snapshot::run_snapshot(config, shared_state, snapshot_sink).await
    });
}
```

**Tests:**
- Snapshot worker con MockSink recibe write_snapshot_rows() calls
- StarRocksSink::write_snapshot_rows() produce mismo resultado que antes
- Snapshot + CDC pipeline corren en paralelo sin contention

---

### PR 0-E: Cleanup final

**Archivos a eliminar:**
- `src/sink/mod.rs` — legacy trait ya eliminado en 0-B
- `src/sink/adapter.rs` — adapter ya eliminado en 0-B
- `src/engine/setup/starrocks.rs` — movido a StarRocksSink en 0-C

**Archivos a limpiar:**
- `Cargo.toml`: verificar que no queden deps sin usar
- `src/main.rs`: actualizar si hay cambios en inicializacion
- `src/connectors/sinks/mod.rs`: actualizar docs

**Verificar:**
- `cargo build --release` limpio
- `cargo test` pasa
- `cargo clippy -- -D warnings` limpio
- `cargo build --release --features http-api` limpio

---

## Resumen visual del refactor

```
ANTES:
  CdcMessage → Pipeline → NewSinkAdapter → core::Sink → StarRocksSink
  Snapshot: → StreamLoadClient (hardcodeado, bypasea el trait)
  Setup: → engine/setup/starrocks.rs (hardcodeado en engine)

DESPUES:
  CdcRecord → Pipeline → core::Sink → StarRocksSink (o cualquier sink)
  Snapshot: → core::Sink::write_snapshot_rows() (generico)
  Setup: → core::Sink::setup() (cada sink maneja el suyo)
```

## Orden de PRs y dependencias

```
PR 0-A: Evolucionar trait
    │
    ▼
PR 0-B: Mover conversion al source + Pipeline generico
    │
    ▼
PR 0-C: Migrar StarRocksSink al nuevo trait
    │
    ▼
PR 0-D: Snapshot worker generico
    │
    ▼
PR 0-E: Cleanup
    │
    ▼
(Fase 1: PostgreSQL target)
```

Cada PR es deployable. Ningun PR rompe funcionalidad existente. El behavior de StarRocks no cambia en ningun momento.

## Riesgos y mitigaciones

| Riesgo | Mitigacion |
|--------|------------|
| Pipeline + sink contention en snapshot | Dos instancias de sink (Opcion B) |
| Regression en conversion CdcMessage → CdcRecord | Tests de regression en PR 0-B |
| Performance de row_to_column_values() vs serialize_text_rows_to_json() | StarRocksSink override hace la serializacion optima internamente |
| SchemaCache ya no esta en Pipeline | SchemaCache se mueve al source layer, pipeline no la necesita |
| StarRocks setup necesita mysql_async (dependency del sink) | StarRocksSink mantiene su pool MySQL internamente |
