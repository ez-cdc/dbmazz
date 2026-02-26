# Demo de dbmazz

**CDC en vivo**: PostgreSQL → StarRocks en 2 minutos.

---

## 🚀 Iniciar Demo

```bash
./demo-start.sh
```

**Presiona `Ctrl+C` para detener**

---

## 📊 ¿Qué Verás?

El demo incluye:

1. **PostgreSQL**: Base transaccional con tablas de e-commerce
2. **StarRocks**: Base analítica recibiendo datos en tiempo real
3. **dbmazz CDC**: Motor de replicación
4. **Traffic Generator**: Simula 72 ops/seg (287 eventos/seg)
5. **TOAST Generator**: Prueba columnas grandes (JSONs de 100KB)
6. **Monitor Dashboard**: Métricas en vivo

### Dashboard en Vivo

```
┏━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Tabla           ┃ PostgreSQL┃ StarRocks ┃ Estado     ┃
┡━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ orders          │     20309 │     20309 │ ✅ (100%) │
│ order_items     │     61488 │     61488 │ ✅ (100%) │
│ toast_test      │        10 │        10 │ ✅ (100%) │
└─────────────────┴───────────┴───────────┴────────────┘

Lag: 0s | LSN: 0x9B972820 | Sync: hace 2s
```

---

## 🔧 Comandos Útiles

### Ver Logs del CDC

```bash
docker logs -f dbmazz-demo-cdc
```

### Conectarse a PostgreSQL

```bash
docker exec -it dbmazz-demo-postgres psql -U postgres -d demo_db

# Consultas útiles
SELECT COUNT(*) FROM orders;
SELECT * FROM orders LIMIT 5;
```

### Conectarse a StarRocks

```bash
docker exec -it dbmazz-demo-starrocks mysql -h127.0.0.1 -P9030 -uroot -Ddemo_db

# Ver datos replicados
SELECT COUNT(*) FROM orders WHERE dbmazz_is_deleted = FALSE;
SELECT * FROM orders LIMIT 5;
```

### Verificar Sincronización

```bash
# PostgreSQL
docker exec dbmazz-demo-postgres psql -U postgres -d demo_db \
  -c "SELECT COUNT(*) FROM orders"

# StarRocks
docker exec dbmazz-demo-starrocks mysql -h127.0.0.1 -P9030 -uroot \
  -e "SELECT COUNT(*) FROM demo_db.orders WHERE dbmazz_is_deleted = FALSE"
```

Ambos deben retornar el mismo número.

### API gRPC

> **Nota**: Instalar `grpcurl` primero: `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`

```bash
# Health Check (sin necesidad de archivos .proto, reflection habilitado)
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check

# Listar todos los servicios disponibles
grpcurl -plaintext localhost:50051 list

# Estado actual
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcStatusService/GetStatus

# Pausar CDC
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Pause

# Resumir CDC
grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/Resume

# Ver métricas en tiempo real (cada 5 segundos)
grpcurl -plaintext -d '{"interval_ms": 5000}' localhost:50051 \
  dbmazz.CdcMetricsService/StreamMetrics
```

---

## ⚙️ Configuración

### Ajustar Throughput

Editar `docker-compose.demo.yml`:

```yaml
traffic-generator:
  environment:
    TARGET_EVENTS_PER_SECOND: "72"  # Cambiar este valor
```

### Ajustar Batching

Editar `docker-compose.demo.yml`:

```yaml
dbmazz:
  environment:
    FLUSH_SIZE: "1500"           # Eventos por batch
    FLUSH_INTERVAL_MS: "5000"    # Milisegundos entre flushes
```

---

## 🐛 Troubleshooting

### El demo no arranca

```bash
# Verificar Docker
docker --version

# Limpiar contenedores anteriores
docker-compose down -v
./demo-start.sh
```

### StarRocks no responde

```bash
# StarRocks tarda ~30s en arrancar
# Esperar hasta ver: "✅ StarRocks BE is ready"
```

### No hay datos en StarRocks

```bash
# Verificar que dbmazz está corriendo
docker logs dbmazz-demo-cdc | grep "Connected"

# Debe mostrar: "Connected! Streaming CDC events..."
```

### Monitor muestra 0 registros

```bash
# Esperar ~10 segundos para que los datos se repliquen
# Verificar logs: docker logs dbmazz-demo-cdc | grep "Sent"
```

---

## 📁 Estructura del Demo

```
examples/
├── demo-start.sh              # Iniciar todo
├── demo-stop.sh               # Detener todo
├── docker-compose.demo.yml    # Servicios Docker
├── postgres/
│   ├── init.sql               # Schema PostgreSQL
│   └── seed-data.sql          # Datos iniciales
├── starrocks/
│   └── init.sql               # Schema StarRocks
├── monitor/
│   ├── dashboard.py           # Dashboard TUI
│   └── requirements.txt
├── traffic-generator/
│   ├── generate.py            # Generador de tráfico
│   └── requirements.txt
└── toast-generator/
    ├── generate.py            # Generador de TOAST
    └── requirements.txt
```

---

## 🎯 Próximos Pasos

Después de probar el demo:

1. **Evaluar Performance**: ¿Cumple con tus requisitos de throughput?
2. **Probar tus Datos**: Configura con tu schema real
3. **Escalar**: Prueba con volúmenes de producción
4. **Contactar**: Agenda demo personalizada → sales@dbmazz.io

---

**¿Preguntas?** Revisa el [README principal](../README.md) o contacta al equipo de soporte.
