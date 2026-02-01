# Contributing to dbmazz

Guide for developers and contributors.

---

## Quick Setup

### Prerequisites

```bash
# Rust 1.70+
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# System dependencies (Debian/Ubuntu)
sudo apt-get install -y protobuf-compiler libssl-dev pkg-config libcurl4-openssl-dev
```

### Build

```bash
git clone <repo>
cd dbmazz
cargo build --release
```

### Run Demo

```bash
cd demo
./demo-start.sh      # Start PostgreSQL + StarRocks + dbmazz
./demo-stop.sh       # Stop everything
```

### Tests

```bash
cargo test                    # All tests
cargo test -- --nocapture     # With output
cargo clippy -- -D warnings   # Linting
cargo fmt --check             # Check formatting
```

---

## Code Conventions

### Formatting and Linting

- **Format**: Always run `cargo fmt` before committing
- **Lint**: `cargo clippy -- -D warnings` with no warnings

### Naming

| Type | Convention | Example |
|------|------------|---------|
| Functions | `snake_case` | `parse_message()` |
| Types/Structs | `PascalCase` | `CdcMessage` |
| Constants | `SCREAMING_SNAKE_CASE` | `MAX_BATCH_SIZE` |
| Modules | `snake_case` | `schema_cache` |

### Error Handling

```rust
// ✅ Use anyhow::Result
pub fn do_something() -> Result<T> {
    let value = risky_operation()?;
    Ok(value)
}

// ❌ Avoid unwrap() in production
let value = risky_operation().unwrap(); // Only in tests
```

### Logging

```rust
log::info!("Starting replication from LSN: {:#X}", lsn);
log::warn!("Table {} missing REPLICA IDENTITY", table);
log::error!("Failed to send batch: {}", err);
log::debug!("Parsed tuple: {:?}", tuple);  // Only for debug
```

### Async

```rust
// ✅ Async for I/O
async fn fetch_data() -> Result<Data> { ... }

// ❌ No async for CPU-bound
fn calculate_hash(data: &[u8]) -> u64 { ... }
```

---

## Contribution Process

### 1. Fork and Branch

```bash
git checkout -b feat/new-feature
# or
git checkout -b fix/bug-description
```

### 2. Commits

Use conventional format:

```
feat: add ClickHouse support
fix: fix memory leak in parser
docs: update README with new env vars
refactor: simplify batching logic
test: add tests for schema evolution
```

### 3. Pull Request

- Clear description of the change
- Reference to issues if applicable
- Tests that validate the change

### 4. Review

- Respond to comments
- Make requested changes
- Keep PR updated with main

---

## PR Checklist

- [ ] `cargo fmt` executed
- [ ] `cargo clippy -- -D warnings` without errors
- [ ] `cargo test` passes
- [ ] CHANGELOG.md updated (if applicable)
- [ ] Documentation updated (if API/config changes)
- [ ] Demo tested locally (if affecting core functionality)

---

## Debugging

```bash
# Detailed logs
RUST_LOG=debug cargo run

# gRPC API
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 dbmazz.HealthService/Check

# Demo logs
docker logs -f dbmazz-demo-cdc
```

---

## License

This project is licensed under the Elastic License v2.0.

By contributing to this project, you agree that your contributions will be licensed under the same license (Elastic License v2.0). This means that:

- Your contributions may be used, copied, distributed, and modified by the project
- Contributions will be subject to the same limitations of ELv2
- You cannot revoke the license once your contribution is accepted

For more details about the license terms, see the [LICENSE](../LICENSE) file.
