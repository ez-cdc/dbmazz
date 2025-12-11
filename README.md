# dbmazz

Sistema CDC (Change Data Capture) de alto rendimiento escrito en Rust para replicar PostgreSQL a StarRocks usando replicación lógica y Stream Load v1.

## 🎯 Quick Start: Demo en 2 Minutos

**¿Quieres ver dbmazz en acción?** Ejecuta el demo comercial:

```bash
cd demo
./demo-start.sh
```

### Lo que verás:

1. ✅ PostgreSQL y StarRocks en Docker
2. ✅ Esquema e-commerce con 3 tablas (`orders`, `order_items`, `toast_test`)
3. ✅ dbmazz replicando cambios en tiempo real
4. ✅ Generador de tráfico (3000+ eventos/seg)
5. ✅ Generador de TOAST (JSONs grandes con Partial Update)
6. ✅ Dashboard en vivo con métricas

**Para detener:** Presiona `Ctrl+C` o ejecuta `./demo-stop.sh`

Ver [demo/README.md](demo/README.md) para documentación detallada del demo.

---

## Arquitectura de Alto Rendimiento

`dbmazz` está diseñado para manejar >100k eventos/segundo con optimizaciones clave:

### Componentes

1.  **WAL Reader (Native)**: Conexión nativa a PostgreSQL sin binarios externos
    -   Usa `tokio-postgres` para replicación lógica
    -   Protocolo `pgoutput` nativo de PostgreSQL

2.  **Parser Zero-Copy con SIMD**:
    -   Parser manual del protocolo `pgoutput` usando `bytes::Bytes` (sin copias innecesarias)
    -   SIMD mediante `memchr` para búsquedas ultra-rápidas
    -   SIMD mediante `simdutf8` para validación UTF-8 optimizada

3.  **Schema Cache (O(1))**: 
    -   `hashbrown::HashMap` para lookups instantáneos
    -   Actualización reactiva ante mensajes `Relation`

4.  **Pipeline con Batching**:
    -   Desacopla lectura de escritura
    -   Batching inteligente por tamaño (N eventos) y tiempo (M ms)
    -   Backpressure natural con canales acotados (`tokio::sync::mpsc`)

5.  **Sinks Extensibles (Strategy Pattern)**:
    -   Trait `Sink` para agregar nuevos destinos fácilmente
    -   StarRocks Sink implementado con HTTP Stream Load

6.  **State Store con Checkpointing**:
    -   Tabla PostgreSQL `dbmazz_checkpoints` para persistir LSNs
    -   Recuperación ante fallos ("at-least-once")

## Prerequisitos

1.  **Rust**: Versión reciente de Rust y Cargo instalados.
2.  **PostgreSQL**:
    -   `wal_level = logical` en `postgresql.conf`
    -   **REPLICA IDENTITY FULL** (requerido para soft deletes en StarRocks):
        ```sql
        ALTER TABLE my_table REPLICA IDENTITY FULL;
        ```
        > ⚠️ **Importante**: StarRocks/ClickHouse necesitan todas las columnas (incluyendo particiones) para DELETEs.
        > REPLICA IDENTITY FULL garantiza que el WAL incluya todos los valores de la fila eliminada.
    -   Crear publicación:
        ```sql
        CREATE PUBLICATION dbmazz_pub FOR ALL TABLES;
        ```
3.  **StarRocks**:
    -   Tabla destino con Primary Key y columnas de auditoría CDC:
        ```sql
        CREATE TABLE my_table (
            id INT,
            name STRING,
            -- ... tus columnas ...
            
            -- Columnas de auditoría CDC (agregadas automáticamente por dbmazz)
            dbmazz_op_type TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE',
            dbmazz_is_deleted BOOLEAN COMMENT 'Soft delete flag',
            dbmazz_synced_at DATETIME COMMENT 'Timestamp de sincronización',
            dbmazz_cdc_version BIGINT COMMENT 'PostgreSQL LSN'
        ) PRIMARY KEY (id)
        DISTRIBUTED BY HASH(id);
        ```

## Configuración

Variables de entorno:

```bash
# PostgreSQL
DATABASE_URL="postgres://user:pass@localhost:5432/dbname"
SLOT_NAME="dbmazz_slot"                      # Opcional: default 'dbmazz_slot'
PUBLICATION_NAME="dbmazz_pub"                # Opcional: default 'dbmazz_pub'

# StarRocks
STARROCKS_URL="http://127.0.0.1:8030"
STARROCKS_DB="test_db"
STARROCKS_TABLE="test_table"
STARROCKS_USER="root"                        # Opcional: default 'root'
STARROCKS_PASS=""                            # Opcional: default vacío
```

## Ejecución

```bash
cd dbmazz
cargo run --release  # Importante: usar --release para máximo rendimiento
```

## Estructura del Código

```
src/
├── main.rs                   # Orquestación principal
├── source/
│   ├── mod.rs
│   ├── postgres.rs           # Cliente nativo PostgreSQL
│   └── parser.rs             # Parser pgoutput zero-copy + SIMD
├── pipeline/
│   ├── mod.rs                # Batching y backpressure
│   └── schema_cache.rs       # Cache O(1) de esquemas
├── sink/
│   ├── mod.rs                # Trait Sink
│   └── starrocks.rs          # Implementación StarRocks
└── state_store.rs            # Checkpointing para recovery
```

## Métricas de Rendimiento Esperadas

-   **Throughput**: >100k eventos/segundo
-   **Latencia p99**: <10ms (desde WAL hasta Sink)
-   **Memoria**: <100MB para 100k eventos en buffer
-   **CPU**: 1 core saturado para parsing

## Características Avanzadas

### Soporte TOAST con Partial Update

dbmazz maneja automáticamente columnas TOAST (valores grandes >2KB en PostgreSQL) usando **StarRocks Partial Update**:

- ✅ Detección O(1) con bitmap de 64-bits y SIMD (POPCNT, CTZ)
- ✅ Partial Update para UPDATEs que no modifican columnas grandes
- ✅ Preserva JSONs de hasta 10MB sin pérdida de datos
- ✅ Zero allocations para operaciones de bitmap

Ver [TOAST-IMPLEMENTATION.md](TOAST-IMPLEMENTATION.md) para detalles técnicos.

### Optimizaciones de Performance

- **SIMD**: `sonic-rs` para JSON serialization (85% reducción de lag vs `serde_json`)
- **Connection Pooling**: Reutilización de conexiones HTTP a StarRocks
- **Timestamp Caching**: Una llamada `Utc::now()` por batch
- **Zero-copy**: `bytes::Bytes` para evitar copias de datos

Ver [PERFORMANCE-ANALYSIS.md](PERFORMANCE-ANALYSIS.md) y [SONIC-RS-MIGRATION-RESULTS.md](SONIC-RS-MIGRATION-RESULTS.md).

## Roadmap

-   [x] Checkpointing con recovery automático
-   [x] Soporte TOAST con Partial Update
-   [x] Optimizaciones SIMD para parsing y JSON
-   [ ] Metrics endpoint (Prometheus)
-   [ ] Health checks (`/health`, `/ready`)
-   [ ] Sinks adicionales (Kafka, S3, Webhooks)
-   [ ] Configuración vía YAML
-   [ ] Snapshot inicial (antes de CDC)
