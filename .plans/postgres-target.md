# PostgreSQL como Target — Plan de Implementacion

**Prerequisito:** [Fase 0 — Refactor de arquitectura](phase-0-sink-architecture.md) ✅ completada.

## Resumen

Agregar PostgreSQL como sink target para snapshot y CDC. Usa la metodologia raw table + normalize (probada en PeerDB) adaptada a la arquitectura de dbmazz post-Fase 0.

**Flujo CDC y Snapshot (mismo path — write_batch):**
```
Pipeline / Snapshot worker
    │
    └─→ PostgresSink::write_batch(Vec<CdcRecord>)
              │
              ├─ COPY INTO raw_table (atomico con metadata update)
              └─ Notificar normalizer
                       │
                       ▼ (async tokio task, interno al sink)
                 Normalizer
                       │
                       ├─ MERGE INTO dst (PG >= 15)
                       ├─ DELETE FROM raw_table (cleanup)
                       └─ UPDATE metadata (normalize_batch_id)
```

Tanto CDC como snapshot pasan por `write_batch()`. El sink no necesita saber de donde vienen los records.

---

## PR 1: Config + stub (chico)

**Objetivo:** `SINK_TYPE=postgres` compila y arranca sin funcionalidad.

**`src/config.rs`:**
- Agregar `SinkType::Postgres` al enum
- Parsear `SINK_TYPE=postgres|postgresql`
- `SINK_URL` = PostgreSQL connection string del target
- `SINK_SCHEMA` nueva env var (default "public")

**`src/connectors/sinks/postgres/mod.rs`:**
- Stub `PostgresSink` implementando `core::Sink`
- `write_batch()` retorna Ok (placeholder)
- `validate_connection()` abre y cierra conexion al target

**`src/connectors/sinks/mod.rs`:**
- `SinkType::Postgres => Ok(Box::new(PostgresSink::new(config)?))`

**`src/engine/setup/mod.rs`:**
- Agregar `SinkType::Postgres => {}` al match (skip StarRocks setup)

**Tests:** SinkType parsing, create_sink(), Config::from_env()

---

## PR 2: Setup — raw table + tablas destino (mediano)

**Objetivo:** `PostgresSink::setup()` crea la infraestructura en el target PG.

**`src/connectors/sinks/postgres/setup.rs`:**

1. Conectar al target PG via `tokio-postgres`
2. `CREATE SCHEMA IF NOT EXISTS _dbmazz`
3. Crear raw table (una por job):
   ```sql
   CREATE TABLE IF NOT EXISTS _dbmazz._raw_{slot_name} (
       _uid           UUID    NOT NULL DEFAULT gen_random_uuid(),
       _timestamp     BIGINT  NOT NULL,
       _dst_table     TEXT    NOT NULL,
       _data          JSONB   NOT NULL,
       _record_type   SMALLINT NOT NULL,
       _match_data    JSONB,
       _batch_id      INTEGER,
       _toast_columns TEXT
   );
   CREATE INDEX IF NOT EXISTS idx_{slot_name}_batch
       ON _dbmazz._raw_{slot_name}(_batch_id);
   ```
4. Crear metadata table:
   ```sql
   CREATE TABLE IF NOT EXISTS _dbmazz._metadata (
       job_name           TEXT PRIMARY KEY,
       lsn_offset         BIGINT NOT NULL DEFAULT 0,
       sync_batch_id      BIGINT NOT NULL DEFAULT 0,
       normalize_batch_id BIGINT NOT NULL DEFAULT 0
   );
   ```
5. Introspeccionar source PG → crear tablas destino con type mapping

**`src/connectors/sinks/postgres/types.rs`:**
- PG → PG type mapping (identity excepto serial → integer, user-defined → text)

**Tests:** DDL generation, type mapping unitarios

---

## PR 3: write_batch() — COPY a raw table (mediano)

**Objetivo:** `write_batch()` escribe records a la raw table via COPY protocol.

**Flujo:**
```rust
async fn write_batch(&mut self, records: Vec<CdcRecord>) -> Result<SinkResult> {
    self.current_batch_id += 1;
    let tx = self.client.transaction().await?;

    // COPY INTO raw table
    // Para cada CdcRecord:
    //   Insert  → record_type=0, data=json(columns), match_data=NULL
    //   Update  → record_type=1, data=json(new_columns), match_data=json(old_columns), toast_columns=unchanged
    //   Delete  → record_type=2, data=json(columns), match_data=json(columns)
    //   Otros   → skip

    // UPDATE metadata (lsn_offset, sync_batch_id)
    tx.commit().await?;

    // Notificar normalizer
    let _ = self.normalize_notify_tx.send(self.current_batch_id);
    Ok(SinkResult { ... })
}
```

**Helpers:**
- `columns_to_jsonb()` — serializa ColumnValue a JSONB (omite Unchanged, base64 para Bytes)
- `extract_toast_columns()` — extrae nombres de columnas con Value::Unchanged

**Tests:** Serializar Insert/Update/Delete, TOAST extraction, atomicidad

---

## PR 4: Normalizer — MERGE (grande)

**Objetivo:** Loop async que aplica raw table → tablas destino via MERGE (PG >= 15).

**`src/connectors/sinks/postgres/normalizer.rs`:**
- Spawned por PostgresSink en setup/new
- Espera notificaciones de nuevos batches
- Para cada tabla destino en el batch:
  1. Obtener combinaciones TOAST unicas
  2. Generar MERGE SQL dinamico
  3. Ejecutar dentro de transaccion
  4. Cleanup raw table filas procesadas
  5. Actualizar normalize_batch_id
- Retry infinito con backoff exponencial

**`src/connectors/sinks/postgres/merge_generator.rs`:**
- MERGE SQL con RANK() PARTITION BY PKs ORDER BY _timestamp DESC
- N clausulas WHEN MATCHED por combinacion de TOAST columns
- Expresiones JSONB → columnas tipadas: `(_data->>'col')::type`

**Backpressure:**
- write_batch() chequea `sync_batch_id - normalize_batch_id > buffer_limit`
- Si se excede, bloquea hasta que normalizer avance

**Tests:** MERGE SQL generation (1 PK, composite PK, TOAST), dedup, backpressure

---

## PR 5: close() + integracion (mediano)

**Objetivo:** Graceful shutdown + tests E2E.

**close():**
```rust
async fn close(&mut self) -> Result<()> {
    // 1. Esperar normalizer (flush batches pendientes)
    // 2. Cerrar normalizer task
    // 3. Cleanup raw table
    // 4. Cerrar conexion target PG
}
```

**Tests E2E (contra PG real via dev-stack):**
- SINK_TYPE=postgres arranca sin error
- Setup crea tablas en target PG
- CDC: Insert → aparece en destino
- CDC: Update → actualizado en destino
- CDC: Delete → eliminado de destino
- Snapshot: datos copiados (via write_batch, mismo path que CDC)
- Resume: reinicio continua desde ultimo batch normalizado

---

## Resumen de PRs

| PR | Descripcion | Estimacion |
|----|-------------|------------|
| 1 | Config + stub PostgresSink | Chico |
| 2 | Setup — raw table + tablas destino + types.rs | Mediano |
| 3 | write_batch() — COPY a raw table | Mediano |
| 4 | Normalizer — MERGE generator + async loop + backpressure | Grande |
| 5 | close() + tests E2E | Mediano |

## Decisiones de diseno

1. **Solo PG >= 15 (MERGE)** — fallback UPSERT+DELETE no maneja TOAST correctamente
2. **Sin schema evolution v1** — cambio de schema requiere re-setup
3. **Sin soft delete v1** — DELETE es hard delete
4. **Sin pipe PG→PG v1** — optimizacion de performance, no funcionalidad
5. **Snapshot por write_batch()** — mismo path que CDC, no hay path separado
6. **Raw table cleanup** — dentro de la misma transaccion del MERGE
7. **Backpressure** — configurable via `NORMALIZE_BUFFER_SIZE` (default 100)
8. **Metadata en target PG** — checkpoint LSN sigue en source via StateStore
