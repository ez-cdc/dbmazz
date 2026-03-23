# Multi-Sink Roadmap

## Vision

dbmazz es un daemon CDC que lee PostgreSQL WAL y escribe a cualquier destino. El objetivo es soportar 25+ sinks, donde cada integracion sea **la mejor posible** en performance y confiabilidad.

La arquitectura debe cumplir dos principios que parecen opuestos pero no lo son:

1. **Agregar un sink basico es trivial** — implementar un metodo y funciona
2. **Cada sink puede ser best-in-class** — override de metodos para optimizar

---

## Sinks — Roadmap de prioridad

### Tier 1 — Core (2026 H1)

| Sink | Estrategia CDC | Estrategia Snapshot | Estado |
|------|---------------|--------------------| -------|
| **StarRocks** | JSON → Stream Load HTTP | JSON → Stream Load HTTP | Implementado |
| **PostgreSQL** | Raw table + MERGE (PG >= 15) | COPY FROM STDIN directo | Pendiente |

### Tier 2 — Cloud Warehouses (2026 H2)

| Sink | Estrategia CDC | Estrategia Snapshot |
|------|---------------|---------------------|
| **Snowflake** | Parquet → S3 stage → COPY INTO | Parquet → S3 stage → COPY INTO |
| **BigQuery** | JSON → BigQuery Storage Write API | Parquet → GCS → Load Job |
| **Redshift** | CSV → S3 → COPY | CSV → S3 → COPY |

### Tier 3 — Databases (2027)

| Sink | Estrategia CDC | Estrategia Snapshot |
|------|---------------|---------------------|
| **ClickHouse** | JSON → HTTP insert | JSON → HTTP insert |
| **MySQL** | Batch REPLACE INTO | LOAD DATA INFILE |
| **MongoDB** | Batch bulkWrite (upsert) | Batch insertMany |
| **DynamoDB** | BatchWriteItem | BatchWriteItem |

### Tier 4 — Streaming & Data Lake (2027+)

| Sink | Estrategia CDC | Estrategia Snapshot |
|------|---------------|---------------------|
| **S3 / GCS** | Parquet/Avro files por tabla | Parquet/Avro files por tabla |
| **Delta Lake** | Delta transaction log | Delta transaction log |
| **Kafka** | Produce JSON/Avro records | N/A |
| **Kinesis** | PutRecords | N/A |

---

## Arquitectura — Un Sink trait con especializacion opcional

```
                    ┌──────────────────────────────────────┐
                    │            Engine (genérico)          │
                    │                                      │
                    │  Setup ──→ sink.setup()               │
                    │  CDC   ──→ sink.write_batch()         │
                    │  Snap  ──→ sink.write_snapshot_rows() │
                    │  End   ──→ sink.flush() + close()     │
                    └──────────────┬───────────────────────┘
                                   │
            ┌──────────────────────┼──────────────────────┐
            │              │              │               │
      ┌─────▼─────┐ ┌─────▼──────┐ ┌────▼──────┐ ┌──────▼──────┐
      │ StarRocks │ │ PostgreSQL │ │ Snowflake │ │ S3 / Lake  │
      │           │ │            │ │           │ │            │
      │ CDC:      │ │ CDC:       │ │ CDC:      │ │ CDC:       │
      │ Stream    │ │ Raw table  │ │ S3 stage  │ │ Parquet    │
      │ Load HTTP │ │ + MERGE    │ │ + COPY    │ │ files      │
      │           │ │            │ │           │ │            │
      │ Snapshot: │ │ Snapshot:  │ │ Snapshot: │ │ Snapshot:  │
      │ Stream    │ │ COPY FROM  │ │ Parquet   │ │ Parquet    │
      │ Load HTTP │ │ STDIN      │ │ + S3      │ │ files      │
      └───────────┘ └────────────┘ └───────────┘ └────────────┘

      Cada sink es dueño de su estrategia.
      El engine no sabe cómo lo hacen.
```

### Sink trait (core::Sink) — IMPLEMENTADO

6 metodos, 1 con default. Equivalente a Kafka Connect (start + put + flush + stop).

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> SinkCapabilities;
    async fn validate_connection(&self) -> Result<()>;

    /// Preparar el destino. Default: no-op.
    async fn setup(&mut self, source_schemas: &[SourceTableSchema]) -> Result<()> { Ok(()) }

    /// Escribir un batch de cambios CDC (y snapshot — misma interfaz).
    async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult>;

    /// Cerrar: flush pending data + limpiar recursos.
    async fn close(&mut self) -> Result<()>;
}
```

Snapshot y CDC usan el mismo `write_batch()`. El sink no necesita saber de donde vienen los records.

### Agregar un sink: paso a paso

```
1. Crear src/connectors/sinks/mi_sink/
   ├── mod.rs      ← implementar Sink (6 métodos)
   └── config.rs   ← MiSinkConfig

2. Agregar SinkType::MiSink en config.rs

3. Agregar match arm en create_sink()

4. Listo. CDC y snapshot funcionan automáticamente.
```

Ejemplo: MongoDB, DynamoDB, Kafka.

Sinks con estrategias CDC complejas (raw table, staging) manejan esa complejidad internamente.
Ejemplo: PostgreSQL (raw table + MERGE), Snowflake (S3 stage), S3 (Parquet files).

---

## Fases de implementacion

### Fase 0: Refactor arquitectura ✅ COMPLETADA
Reordenado dbmazz para soportar multiples sinks. Branch: `feat/multi-sink-architecture`.
- Pipeline recibe CdcRecord (generico, no pgoutput)
- Snapshot worker usa Sink::write_batch() (no StreamLoadClient hardcodeado)
- NewSinkAdapter y legacy sink::Sink eliminados (-752 lineas)
- SetupManager condicional por SinkType
Detalle: [phase-0-sink-architecture.md](phase-0-sink-architecture.md)

### Fase 1: PostgreSQL target ← SIGUIENTE
Primer sink nuevo. Valida la arquitectura multi-sink.
Detalle: [postgres-target.md](postgres-target.md)

### Fase 2: Snowflake target
Cloud warehouse. Introduce el pattern Stage (S3/GCS) + COPY INTO.

### Fase 3: MongoDB + S3
Document store + data lake. Validan la versatilidad del trait.

---

## Familias de carga y patterns reutilizables

A medida que agreguemos sinks, van a emerger patterns compartidos:

| Pattern | Sinks que lo usan | Modulo reutilizable |
|---------|-------------------|---------------------|
| Raw table + normalize | PostgreSQL, MySQL | `sinks::common::raw_table` |
| S3/GCS staging | Snowflake, BigQuery, Redshift, S3 | `sinks::common::cloud_stage` |
| HTTP bulk load | StarRocks, ClickHouse, Doris | `sinks::common::http_loader` |
| Parquet writer | Snowflake, BigQuery, S3, Delta Lake | `sinks::common::parquet` |
| Batch SDK upsert | MongoDB, DynamoDB | (cada uno usa su SDK) |

Estos modulos NO se crean de antemano. Se extraen cuando el segundo sink del mismo pattern lo necesita. Premature abstraction is the root of all evil.

---

## Principios

1. **El engine es sink-agnostic** — nunca menciona Stream Load, COPY, S3, MERGE
2. **Cada sink es autocontenido** — su estrategia CDC, snapshot, setup, cleanup son internos
3. **Una sola interfaz para CDC y snapshot** — write_batch() maneja ambos, sin metodos separados
4. **No abstraer hasta que haya 2 usuarios** — patterns compartidos se extraen, no se predicen
5. **CdcRecord es la frontera** — todo lo que cruza entre engine y sink es CdcRecord, nunca pgoutput
