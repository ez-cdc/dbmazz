# Changelog

Todos los cambios notables de dbmazz serán documentados aquí.

## [Unreleased]

### Added
- **Setup Automático PostgreSQL**: Configuración cero, `dbmazz` configura todo automáticamente
  - Verifica que las tablas existen
  - Configura `REPLICA IDENTITY FULL` automáticamente
  - Crea/verifica Publication y Replication Slot
  - Modo recovery: detecta recursos existentes tras caídas
- **Setup Automático StarRocks**: Columnas de auditoría agregadas automáticamente
  - `dbmazz_op_type`, `dbmazz_is_deleted`, `dbmazz_synced_at`, `dbmazz_cdc_version`
  - Valida conectividad y existencia de tablas
  - Idempotente: detecta columnas existentes
- **Error Handling Mejorado**: Mensajes descriptivos para el control plane
  - Nuevo campo `error_detail` en Health Check
  - `status: NOT_SERVING` cuando hay errores de setup
  - gRPC server sigue corriendo para consultas incluso con errores
- **Métricas de CPU en Millicores**: Monitoreo de consumo de CPU consistente entre entornos
  - Nuevo campo `cpu_millicores` en `MetricsResponse`
  - Lectura directa de `/proc/[pid]/stat` (mismo algoritmo que `ps` y `top`)
  - Consistente entre bare metal, Docker y Kubernetes
  - 1000 millicores = 100% de 1 core (estándar Kubernetes)
  - Validado: 3000 ev/s → 45 millicores (4.5% de 1 core)
  - Eficiencia: 66 eventos/milicore en bare metal
- **gRPC Reflection**: Servidor gRPC con reflection habilitado para uso simple de `grpcurl` sin archivos `.proto`
- **Schema Evolution básico**: Detección automática de nuevas columnas y `ALTER TABLE ADD COLUMN` en StarRocks

### Changed
- Migración de `reqwest` a `curl` crate (libcurl bindings) para StarRocks Stream Load
  - Manejo correcto del protocolo `Expect: 100-continue`
  - Soporte nativo para redirects FE → BE con autenticación
- **Redirect Inteligente para Autoscaling**: Stream Load soporta StarRocks FE con autoscaling
  - Conexión al Frontend (puerto 8030) en lugar de Backend directo
  - Detección automática de redirects HTTP 307 a `127.0.0.1`
  - Reescritura de hostname para compatibilidad con Docker/Kubernetes
  - Soporte completo para balanceo de carga y failover entre múltiples BEs
  - Funciona en bare metal, Docker Compose y Kubernetes sin configuración adicional
  - **Nota**: Implementación actual no ha sido validada contra mejores prácticas de libcurl (TODO: benchmarks, connection pooling)
- **Optimización de CPU**: Reducción del overhead del main loop
  - Migración de `RwLock<CdcState>` a `AtomicU8` para acceso lock-free
  - State checks reducidos de cada iteración a cada 256 iteraciones
  - Zero-copy en WAL parsing (`bytes.slice(..)` en lugar de `clone()`)
  - Pre-allocación de estructuras JSON con capacidad conocida
- **BREAKING**: Ya no se requiere configurar PostgreSQL manualmente
  - Publication, Slot y REPLICA IDENTITY ahora son automáticos
  - Simplifica deployment: solo especifica las tablas

### Fixed
- Clarificación comportamiento TOAST:
  - INSERTs siempre reciben datos completos (incluso > 2KB)
  - Solo UPDATEs que no modifican columna TOAST envían marcador 'u'
  - Partial Update preserva valores existentes en StarRocks
- Compatibilidad Docker: Redirects FE→BE que apuntaban a `127.0.0.1` ahora se reescriben al hostname correcto

---

## [0.1.0] - 2025-12-11

### Features Principales

#### 🚀 CDC de Alto Rendimiento
- Replicación nativa PostgreSQL → StarRocks usando protocolo `pgoutput`
- Zero-copy parsing con `bytes::Bytes`
- Optimizaciones SIMD (`memchr`, `simdutf8`, `sonic-rs`)
- Throughput: 300K+ eventos procesados sin degradación

#### 🎮 API gRPC para Control Remoto
- **HealthService**: Health check con lifecycle stages (INIT → SETUP → CDC)
- **CdcControlService**: Pause, Resume, DrainAndStop, Stop, ReloadConfig
- **CdcStatusService**: Estado actual (LSN, tablas, eventos pendientes)
- **CdcMetricsService**: Stream de métricas en tiempo real

#### 🔄 Lifecycle Stages
- `STAGE_INIT`: Inicializando
- `STAGE_SETUP`: Conectando source/sink, validando tablas
- `STAGE_CDC`: Replicando activamente
- Permite al control plane monitorear progreso de inicialización

#### 📦 Soporte TOAST (Columnas Grandes)
- Detección automática de columnas TOAST con bitmap de 64-bits
- StarRocks Partial Update para preservar valores grandes sin enviarlos
- Optimizaciones SIMD (POPCNT, CTZ) para tracking de columnas
- Soporta JSONs hasta 10MB sin pérdida de datos

#### 🎯 Checkpointing Robusto
- Persistencia de LSN en tabla PostgreSQL `dbmazz_checkpoints`
- Recovery automático desde último checkpoint
- Confirmación a PostgreSQL vía `StandbyStatusUpdate`
- Garantía "at-least-once" delivery

#### 📊 Auditoría CDC
- Columnas automáticas en StarRocks:
  - `dbmazz_op_type`: 0=INSERT, 1=UPDATE, 2=DELETE
  - `dbmazz_is_deleted`: Soft delete flag
  - `dbmazz_synced_at`: Timestamp de sincronización
  - `dbmazz_cdc_version`: LSN de PostgreSQL

#### 🏗️ Arquitectura Modular
- Refactorización: `main.rs` de 284 → 28 líneas (-90%)
- Módulos separados: `config`, `engine`, `replication`, `grpc`
- Código testeable y mantenible

### Performance

| Métrica | Valor |
|---------|-------|
| Throughput | 300K+ eventos |
| CPU | ~25% bajo carga (287 eps) |
| Memoria | ~5MB en uso |
| Lag | <1KB en condiciones normales |
| Latencia replicación | <5 segundos p99 |

### Optimizaciones Técnicas

#### JSON Serialization
- Migración de `serde_json` → `sonic-rs`
- Uso de SIMD para parsing JSON ultra-rápido
- Reducción de 85% en lag bajo carga alta

#### Connection Pooling
- Reutilización de conexiones HTTP a StarRocks
- `pool_max_idle_per_host: 10`
- `tcp_keepalive: 60s`

#### Batching Configurable
- `FLUSH_SIZE`: Eventos por batch (default: 10000)
- `FLUSH_INTERVAL_MS`: Intervalo máximo entre flushes (default: 5000ms)
- Ajustable vía gRPC `ReloadConfig`

### Demo Comercial

#### Características
- Setup en 1 comando: `./demo-start.sh`
- PostgreSQL + StarRocks en Docker
- 3 tablas e-commerce: `orders`, `order_items`, `toast_test`
- Generador de tráfico configurable (hasta 3000+ eps)
- Generador de TOAST para probar columnas grandes
- Dashboard TUI en tiempo real con métricas dinámicas
- Cleanup automático para demos limpios

#### Métricas Visibles
- Counts PostgreSQL vs StarRocks
- Registros eliminados (soft deletes)
- Lag de replicación en segundos
- LSN actual
- Última sincronización

### Validaciones

#### REPLICA IDENTITY FULL
- Validación automática en startup
- Warning si no está configurado correctamente
- Requerido para soft deletes en bases analíticas

### Configuración

#### Variables de Entorno
- `DATABASE_URL`: Conexión PostgreSQL con `?replication=database`
- `SLOT_NAME`: Nombre del replication slot (default: `dbmazz_slot`)
- `PUBLICATION_NAME`: Nombre de la publicación (default: `dbmazz_pub`)
- `TABLES`: Lista de tablas separadas por coma (default: `orders,order_items`)
- `STARROCKS_URL`: URL del Backend de StarRocks
- `STARROCKS_DB`: Base de datos destino
- `STARROCKS_USER`: Usuario (default: `root`)
- `STARROCKS_PASS`: Password (default: vacío)
- `FLUSH_SIZE`: Eventos por batch (default: 10000)
- `FLUSH_INTERVAL_MS`: Intervalo de flush (default: 5000)
- `GRPC_PORT`: Puerto gRPC (default: 50051)

### Dependencias Principales

- `tokio-postgres` (Materialize fork): Replicación lógica
- `tonic` + `prost`: Servidor gRPC
- `sonic-rs`: JSON con SIMD
- `curl`: Bindings a libcurl para Stream Load
- `mysql_async`: Cliente MySQL para DDL en StarRocks
- `hashbrown`: HashMap de alto rendimiento
- `memchr` + `simdutf8`: Optimizaciones SIMD
- `libc`: Acceso a syscalls de Linux para métricas de CPU

### Estructura del Proyecto

```
dbmazz/
├── src/
│   ├── main.rs              # Punto de entrada (28 líneas)
│   ├── config.rs            # Configuración desde env vars
│   ├── engine/              # Motor CDC y setup automático
│   │   ├── mod.rs           # Orquestador principal
│   │   └── setup/           # Configuración automática PG/SR
│   ├── grpc/                # API gRPC (4 servicios)
│   │   ├── services.rs      # Implementación de servicios
│   │   ├── state.rs         # Estado compartido (AtomicU8)
│   │   └── cpu_metrics.rs   # Tracker de CPU (/proc)
│   ├── replication/         # Procesamiento WAL
│   ├── pipeline/            # Batching y schema cache
│   ├── sink/                # Destinos (StarRocks)
│   ├── source/              # Fuentes (PostgreSQL)
│   └── state_store.rs       # Checkpointing
├── demo/                    # Demo comercial completo
└── CHANGELOG.md             # Este archivo
```

### Testing

- ✅ Compilación: Sin errores
- ✅ Replicación: 100% de datos (20K+ registros)
- ✅ Checkpoints: Persistiendo correctamente
- ✅ gRPC API: 16/16 tests pasados
- ✅ TOAST: 90+ eventos con partial updates
- ✅ Performance: Sin degradación bajo carga

---

## Roadmap

### v0.2.0 (Planeado)
- [ ] Metrics endpoint Prometheus
- [ ] Sinks adicionales: Kafka, ClickHouse
- [ ] Configuración YAML (además de env vars)
- [ ] Snapshot inicial (antes de CDC)
- [ ] Tests unitarios completos

### v0.3.0 (Futuro)
- [ ] Multi-tenant: múltiples sources → múltiples destinos
- [ ] UI web para monitoreo
- [ ] Alerting integrado (Slack, PagerDuty)
- [x] Schema evolution automático (parcial: agregar columnas funciona, pendiente cambio de tipos y eliminación)
- [ ] Compresión de payloads

