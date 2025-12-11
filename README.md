# dbmazz

**CDC de alto rendimiento en Rust**: Replica datos de PostgreSQL a StarRocks en tiempo real.

---

## 🚀 Quick Start (2 minutos)

```bash
cd demo
./demo-start.sh
```

Verás:
- ✅ PostgreSQL + StarRocks en Docker
- ✅ 3 tablas replicándose en tiempo real
- ✅ Dashboard con métricas en vivo
- ✅ 300K+ eventos procesados

**Para detener**: `Ctrl+C` o `./demo-stop.sh`

---

## 📦 Instalación

### 1. Prerequisitos

```bash
# Instalar Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Compilar dbmazz
cargo build --release
```

### 2. Configurar PostgreSQL

```sql
-- Habilitar replicación lógica
ALTER SYSTEM SET wal_level = 'logical';
-- Reiniciar PostgreSQL

-- Configurar tabla (IMPORTANTE para soft deletes)
ALTER TABLE my_table REPLICA IDENTITY FULL;

-- Crear publicación
CREATE PUBLICATION dbmazz_pub FOR TABLE my_table;
```

### 3. Configurar StarRocks

```sql
-- Crear tabla con columnas de auditoría
CREATE TABLE my_table (
    id INT,
    name VARCHAR(100),
    -- ... tus columnas ...
    
    -- Columnas CDC (agregadas automáticamente por dbmazz)
    dbmazz_op_type TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE',
    dbmazz_is_deleted BOOLEAN COMMENT 'Soft delete',
    dbmazz_synced_at DATETIME COMMENT 'Timestamp CDC',
    dbmazz_cdc_version BIGINT COMMENT 'LSN PostgreSQL'
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id);
```

### 4. Variables de Entorno

```bash
# PostgreSQL
export DATABASE_URL="postgres://user:pass@localhost:5432/db?replication=database"
export SLOT_NAME="dbmazz_slot"
export PUBLICATION_NAME="dbmazz_pub"
export TABLES="orders,order_items"

# StarRocks
export STARROCKS_URL="http://localhost:8040"  # Puerto BE
export STARROCKS_DB="my_db"
export STARROCKS_USER="root"
export STARROCKS_PASS=""

# Pipeline (opcional)
export FLUSH_SIZE="1500"           # Eventos por batch
export FLUSH_INTERVAL_MS="5000"    # Flush cada 5 segundos

# gRPC (opcional)
export GRPC_PORT="50051"
```

### 5. Ejecutar

```bash
./target/release/dbmazz
```

---

## 🎮 API gRPC

dbmazz expone una API gRPC para control y monitoreo:

> **Nota**: El servidor tiene **gRPC Reflection** habilitado, por lo que `grpcurl` funciona sin necesidad de especificar archivos `.proto`.

### Health Check con Lifecycle Stages

```bash
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check
```

**Respuesta**:
```json
{
  "status": "SERVING",
  "stage": "STAGE_CDC",
  "stageDetail": "Replicating"
}
```

**Stages**:
- `STAGE_INIT`: Inicializando
- `STAGE_SETUP`: Conectando y validando
- `STAGE_CDC`: Replicando activamente

### Control Remoto

```bash
# Pausar CDC
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Pause

# Resumir CDC
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Resume

# Recargar configuración en caliente
grpcurl -plaintext -d '{"flush_size": 2000}' localhost:50051 \
  dbmazz.CdcControlService/ReloadConfig

# Detener gracefully
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/DrainAndStop
```

### Métricas en Tiempo Real

```bash
# Stream de métricas cada 2 segundos
grpcurl -plaintext -d '{"interval_ms": 2000}' localhost:50051 \
  dbmazz.CdcMetricsService/StreamMetrics
```

**Respuesta**:
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

### Estado Actual

```bash
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcStatusService/GetStatus
```

**Respuesta**:
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

### Explorar API con Reflection

```bash
# Listar todos los servicios
grpcurl -plaintext localhost:50051 list

# Ver métodos de un servicio
grpcurl -plaintext localhost:50051 describe dbmazz.HealthService

# Ver definición de un mensaje
grpcurl -plaintext localhost:50051 describe dbmazz.HealthCheckResponse
```

---

## 🏗️ Arquitectura

```
PostgreSQL WAL
      ↓
  WAL Reader (tokio-postgres)
      ↓
  Parser (zero-copy + SIMD)
      ↓
  Schema Cache (O(1) lookup)
      ↓
  Pipeline (batching + backpressure)
      ↓
  StarRocks Sink (Stream Load)
      ↓
  Checkpoint (LSN confirmation)
```

### Componentes Principales

| Componente | Tecnología | Propósito |
|------------|------------|-----------|
| **WAL Reader** | `tokio-postgres` | Conexión nativa replicación lógica |
| **Parser** | `bytes` + SIMD | Zero-copy parsing del protocolo `pgoutput` |
| **Schema Cache** | `hashbrown` | Lookup O(1) de definiciones de tablas |
| **Pipeline** | `tokio::mpsc` | Batching y backpressure |
| **Sink** | `reqwest` + pooling | HTTP Stream Load a StarRocks |
| **State Store** | PostgreSQL | Persistencia de checkpoints |
| **gRPC Server** | `tonic` | API de control y métricas |

---

## 🎯 Características Destacadas

### Soporte TOAST (Columnas Grandes)

dbmazz maneja automáticamente columnas TOAST (valores >2KB) usando **StarRocks Partial Update**:

- ✅ Detección con bitmap de 64-bits + SIMD
- ✅ Preserva JSONs hasta 10MB sin re-enviarlos
- ✅ Zero allocations para tracking de columnas

### Soft Deletes

Los DELETEs de PostgreSQL se convierten en soft deletes en StarRocks:

```sql
-- En StarRocks después de DELETE
SELECT * FROM orders WHERE dbmazz_is_deleted = FALSE;  -- Registros activos
SELECT * FROM orders WHERE dbmazz_is_deleted = TRUE;   -- Registros eliminados
```

### Checkpointing Robusto

- ✅ Persiste LSN en tabla `dbmazz_checkpoints`
- ✅ Recovery automático desde último checkpoint
- ✅ Confirma a PostgreSQL para liberar WAL
- ✅ Garantía "at-least-once" delivery

### Optimizaciones de Performance

- **SIMD**: `memchr`, `simdutf8`, `sonic-rs` para operaciones ultra-rápidas
- **Zero-copy**: `bytes::Bytes` para evitar copias innecesarias
- **Connection Pooling**: Reutiliza conexiones HTTP
- **Batching**: Agrupa eventos para reducir overhead

---

## 📊 Performance

Medido en condiciones reales:

| Métrica | Valor |
|---------|-------|
| **Throughput** | 300K+ eventos procesados |
| **CPU** | ~25% (1 core) bajo carga de 287 eps |
| **Memoria** | ~5MB en uso |
| **Lag** | <1KB en condiciones normales |
| **Latencia p99** | <5 segundos |

---

## 🔧 Casos de Uso

### 1. Análisis en Tiempo Real

Replica datos transaccionales (PostgreSQL) a base analítica (StarRocks) para dashboards y reportes en tiempo real.

### 2. Data Lake

Replica a StarRocks como staging area antes de ETL a Data Lake.

### 3. Cache Analytics

Mantén caché de datos históricos en StarRocks para consultas rápidas sin impactar PostgreSQL.

### 4. Multi-Region Sync

Replica datos entre regiones usando StarRocks como destino intermedio.

---

## 🛠️ Control Plane Integration

dbmazz está diseñado para orquestación por control plane:

```bash
# 1. Iniciar instancia con puerto gRPC dinámico
export GRPC_PORT=50051
./dbmazz &

# 2. Esperar a que llegue a CDC
while true; do
  STAGE=$(grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check | jq -r '.stage')
  [ "$STAGE" == "STAGE_CDC" ] && break
  sleep 1
done

# 3. Monitorear en tiempo real
grpcurl -plaintext -d '{"interval_ms": 5000}' localhost:50051 \
  dbmazz.CdcMetricsService/StreamMetrics

# 4. Control dinámico
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Pause
```

---

## 📚 Documentación

- **[CHANGELOG.md](CHANGELOG.md)**: Historial de cambios y features
- **[demo/README.md](demo/README.md)**: Guía completa del demo

---

## 🤝 Soporte

Para preguntas o issues, contactar al equipo de desarrollo.

---

## 📄 Licencia

[Especificar licencia]
