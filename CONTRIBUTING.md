# Contribuir a dbmazz

Guía para desarrolladores y contributors.

---

## Setup Rápido

### Prerequisitos

```bash
# Rust 1.70+
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Dependencias del sistema (Debian/Ubuntu)
sudo apt-get install -y protobuf-compiler libssl-dev pkg-config libcurl4-openssl-dev
```

### Compilar

```bash
git clone <repo>
cd dbmazz
cargo build --release
```

### Ejecutar Demo

```bash
cd demo
./demo-start.sh      # Inicia PostgreSQL + StarRocks + dbmazz
./demo-stop.sh       # Detiene todo
```

### Tests

```bash
cargo test                    # Todos los tests
cargo test -- --nocapture     # Con output
cargo clippy -- -D warnings   # Linting
cargo fmt --check             # Verificar formato
```

---

## Convenciones de Código

### Formato y Linting

- **Formato**: Siempre ejecutar `cargo fmt` antes de commit
- **Lint**: `cargo clippy -- -D warnings` sin warnings

### Naming

| Tipo | Convención | Ejemplo |
|------|------------|---------|
| Funciones | `snake_case` | `parse_message()` |
| Tipos/Structs | `PascalCase` | `CdcMessage` |
| Constantes | `SCREAMING_SNAKE_CASE` | `MAX_BATCH_SIZE` |
| Módulos | `snake_case` | `schema_cache` |

### Error Handling

```rust
// ✅ Usar anyhow::Result
pub fn do_something() -> Result<T> {
    let value = risky_operation()?;
    Ok(value)
}

// ❌ Evitar unwrap() en producción
let value = risky_operation().unwrap(); // Solo en tests
```

### Logging

```rust
log::info!("Starting replication from LSN: {:#X}", lsn);
log::warn!("Table {} missing REPLICA IDENTITY", table);
log::error!("Failed to send batch: {}", err);
log::debug!("Parsed tuple: {:?}", tuple);  // Solo para debug
```

### Async

```rust
// ✅ Async para I/O
async fn fetch_data() -> Result<Data> { ... }

// ❌ No async para CPU-bound
fn calculate_hash(data: &[u8]) -> u64 { ... }
```

---

## Proceso de Contribución

### 1. Fork y Branch

```bash
git checkout -b feat/nueva-feature
# o
git checkout -b fix/descripcion-bug
```

### 2. Commits

Usar formato convencional:

```
feat: agregar soporte para ClickHouse
fix: corregir leak de memoria en parser
docs: actualizar README con nuevas env vars
refactor: simplificar lógica de batching
test: agregar tests para schema evolution
```

### 3. Pull Request

- Descripción clara del cambio
- Referencia a issues si aplica
- Tests que validen el cambio

### 4. Review

- Responder a comentarios
- Hacer cambios solicitados
- Mantener PR actualizado con main

---

## Checklist para PR

- [ ] `cargo fmt` ejecutado
- [ ] `cargo clippy -- -D warnings` sin errores
- [ ] `cargo test` pasa
- [ ] CHANGELOG.md actualizado (si aplica)
- [ ] Documentación actualizada (si cambia API/config)
- [ ] Demo probado localmente (si afecta funcionalidad core)

---

## Debugging

```bash
# Logs detallados
RUST_LOG=debug cargo run

# gRPC API
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check

# Logs del demo
docker logs -f dbmazz-demo-cdc
```

