# Sink Architecture — Improvement Plan

Review of the sink factory pattern and trait implementation. Each issue includes context, impact, and a resolution path. Issues are ordered by priority (architectural impact).

---

## Issue 1: SetupManager / Sink::setup() asymmetry

**Status:** pending

**Problem:**
Setup logic is split inconsistently between `SetupManager` (engine/setup/) and `Sink::setup()`:

| Sink | SetupManager | Sink::setup() |
|------|-------------|---------------|
| StarRocks | Creates database + user via MySQL protocol | Not implemented (uses trait default `Ok(())`) |
| PostgreSQL | Skips (`"Sink setup deferred to PostgresSink::setup()"`) | Creates raw table, metadata table, spawns normalizer |

StarRocks setup lives outside the trait, in engine-level code. PostgreSQL setup lives inside the trait. This breaks encapsulation: adding a new sink requires modifying `SetupManager` with sink-specific logic that should belong to the sink itself.

**Files involved:**
- `src/engine/setup/mod.rs` — SetupManager dispatch
- `src/engine/setup/starrocks.rs` — StarRocks database/user creation
- `src/connectors/sinks/starrocks/mod.rs` — StarRocksSink (no setup impl)
- `src/connectors/sinks/starrocks/setup.rs` — StarRocks table creation (used elsewhere?)
- `src/connectors/sinks/postgres/mod.rs` — PostgresSink::setup()
- `src/connectors/sinks/postgres/setup.rs` — raw table + metadata DDL

**Goal:**
Each sink should own its full setup inside `Sink::setup()`. SetupManager should only handle source-side setup (replication slot, publication). Adding a new sink should not require touching SetupManager.

---

## Issue 2: `skip_normalizer` leaks PostgresSink internals into the factory

**Status:** pending

**Problem:**
`create_sink_for_snapshot()` directly accesses `PostgresSink.skip_normalizer`:

```rust
// sinks/mod.rs:94-95
let mut sink = PostgresSink::new(config)?;
sink.skip_normalizer = true;
```

This breaks the `Box<dyn Sink>` abstraction — the factory needs to know that PostgresSink has a background normalizer task. If a third sink also has background tasks, another special case is needed.

**Files involved:**
- `src/connectors/sinks/mod.rs` — factory functions
- `src/connectors/sinks/postgres/mod.rs` — `skip_normalizer` field

**Goal:**
Eliminate the need for sink-specific knowledge in the factory. Options:
- Add a `SinkMode` parameter to the trait or constructor
- Add a `set_snapshot_mode()` method to the trait
- Pass mode via config/builder

---

## Issue 3: Duplicated factory functions

**Status:** pending

**Problem:**
`create_sink()` and `create_sink_for_snapshot()` are nearly identical — same match arms, same construction. The only difference is `skip_normalizer = true` for PostgresSink. Every new sink must add a match arm in both functions.

**Files involved:**
- `src/connectors/sinks/mod.rs`

**Goal:**
Single factory function with a mode parameter (e.g., `SinkMode::Primary` vs `SinkMode::SnapshotWorker`).

---

## Issue 4: `SinkConfig` uses mutually exclusive `Option` fields

**Status:** pending

**Problem:**
```rust
pub struct SinkConfig {
    pub starrocks: Option<StarRocksSinkConfig>,
    pub postgres: Option<PostgresSinkConfig>,
}
```

Each new sink adds another `Option<XConfig>` field. This doesn't enforce mutual exclusivity at the type level — it's possible to have both `Some` or both `None`.

**Files involved:**
- `src/config.rs` — SinkConfig struct and Config::from_env()
- `src/connectors/sinks/mod.rs` — factory reads config
- `src/connectors/sinks/starrocks/config.rs` — StarRocksSinkConfig
- `src/connectors/sinks/postgres/mod.rs` — reads PostgresSinkConfig

**Goal:**
Replace with an enum:
```rust
pub enum SinkSpecificConfig {
    StarRocks(StarRocksSinkConfig),
    Postgres(PostgresSinkConfig),
}
```

---

## Issue 5: Legacy fields in Config

**Status:** pending (known tech debt, planned for v0.3.0)

**Problem:**
`Config` duplicates all sink fields as flat legacy accessors (`starrocks_url`, `starrocks_port`, etc.). Adding a new sink means adding more legacy fields.

**Files involved:**
- `src/config.rs` — Config struct, from_env(), Debug impl

**Goal:**
Remove legacy fields. All consumers should use `config.sink.*` and `config.source.*`. This is already planned for v0.3.0 so it's tracked here for completeness but not prioritized in this effort.
