# EZ-CDC E2E Test Harness

End-to-end test CLI for the dbmazz CDC daemon. Manages Docker infrastructure,
runs a live monitoring dashboard, and executes a structured verification suite
against any supported sink.

---

## Overview

`ez-cdc` is a Rust CLI that wraps the lifecycle of a dbmazz end-to-end
test: it runs the dbmazz daemon container against a configured source/sink
pair (which you bring yourself) and validates the output with a structured
set of checks covering schema correctness, snapshot integrity, CDC
operations, type fidelity, and idempotency.

It has two modes:

- **Interactive** (`ez-cdc` with no subcommand, TTY required): a stateful
  menu that adapts to the current config and infra state. Good for first-time
  setup and ad-hoc exploration.
- **Non-interactive** (explicit subcommands): designed for CI pipelines and
  scripting. Every action has a corresponding subcommand with flags.

---

## Prerequisites

- **Docker Desktop** with `docker compose` (V2). Must be running.
- A PostgreSQL source and at least one sink (StarRocks / PostgreSQL /
  Snowflake) running and reachable from your host — native services,
  your own docker-compose, or remote.

---

## Quick Start

Install the CLI with the one-liner from the repo root README:

```bash
curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh
```

Then:

```bash
# 1. Write a starter config with every dbmazz option documented inline
ez-cdc datasource init

# 2. Add a source and a sink (interactive wizard)
ez-cdc datasource add

# 3. Run the live dashboard — pulls the official dbmazz image from GHCR
ez-cdc quickstart --source <source-name> --sink <sink-name>

# 4. Or run the verification suite
ez-cdc verify --source <source-name> --sink <sink-name>
```

### Developing the CLI itself

If you are hacking on the CLI, run it from source without installing:

```bash
cargo run --manifest-path e2e-cli/Cargo.toml -- datasource init
cargo run --manifest-path e2e-cli/Cargo.toml -- quickstart --source my-pg --sink my-sr
```

### Interactive mode

Run `ez-cdc` with no arguments to open the interactive menu:

```bash
ez-cdc
```

The menu adapts to whether you already have a config file with
datasources or not, and walks you through init, quickstart, verify,
and datasource management.

---

## Configuration

Configuration lives in `e2e-cli/ez-cdc.yaml`. The file is managed by the CLI
but can also be edited by hand. `ez-cdc.yaml` is not included in the repository
at clone time — it is generated on first run.

### Structure

```yaml
settings:
  flush_size: 2000               # Max events per batch before flushing to sink
  flush_interval_ms: 2000        # Max ms to wait before flushing (even if batch not full)
  do_snapshot: true              # Enable initial snapshot (backfill existing rows)
  snapshot_chunk_size: 10000     # Rows per snapshot chunk
  snapshot_parallel_workers: 2   # Parallel snapshot workers
  initial_snapshot_only: false   # If true, exit after snapshot (no CDC)
  rust_log: info                 # Log level passed to the dbmazz daemon
  snowflake_flush_files: 1       # Snowflake: staged files per flush
  snowflake_flush_bytes: 104857600  # Snowflake: max bytes per staged file (100 MB)

sources:
  demo-pg:
    type: postgres
    url: postgres://postgres:postgres@localhost:15432/dbmazz
    seed: postgres-seed.sql           # SQL file in fixtures/ to seed the source DB
    replication_slot: dbmazz_slot
    publication: dbmazz_pub
    tables:
      - orders
      - order_items

sinks:
  demo-pg-target:
    type: postgres
    url: postgres://postgres:postgres@localhost:25432/dbmazz_target
    database: dbmazz_target
    schema: public

  demo-starrocks:
    type: starrocks
    url: http://localhost:18030
    mysql_port: 19030
    database: dbmazz
    user: root
    password: ''
```

### Supported source types

| Type | Field `type` |
|------|-------------|
| PostgreSQL | `postgres` |

### Supported sink types

| Type | Field `type` |
|------|-------------|
| StarRocks | `starrocks` |
| PostgreSQL | `postgres` |
| Snowflake | `snowflake` |

### Snowflake sink fields

```yaml
sinks:
  my-snowflake:
    type: snowflake
    account: xy12345.us-east-1
    user: MY_USER
    password: MY_PASSWORD
    database: MY_DATABASE
    schema: PUBLIC            # optional, defaults to PUBLIC
    warehouse: MY_WAREHOUSE
    role: MY_ROLE             # optional
    private_key_path: /path/to/key.pem  # optional, for JWT auth
    soft_delete: true         # optional, defaults to true
```

### Environment variable interpolation

Values can reference environment variables using `${VAR_NAME}` syntax.
The CLI expands these at load time.

### Datasource name rules

Names must be lowercase alphanumeric, plus hyphens and underscores, 1-64
characters, starting with a letter or digit. Examples: `demo-pg`,
`prod_starrocks`, `my-sink-01`.

---

## Commands

### `ez-cdc` (interactive menu)

Run with no arguments on a TTY to open the interactive menu. The menu adapts
to the current state:

- **No config**: shows "Init config" and "Datasources".
- **Config with datasources, stack stopped**: shows "Start stack",
  "Quickstart", "Verify", "Clean target", "Datasources".
- **Config with datasources, stack running**: shows "Quickstart", "Verify",
  "Clean target", "Logs", "Stop stack", "Datasources".

The menu loops until you select "Exit" or press Ctrl+C.

When run without a TTY (e.g. in a pipe or CI), `ez-cdc` prints help and exits.

---

### Bring Your Own Infrastructure

The CLI does **not** manage source / sink containers. You bring the
source PostgreSQL and sink (StarRocks / Postgres / Snowflake) already
running — either your own infrastructure, or your own Docker Compose
setup. The CLI only manages the `dbmazz` daemon container, which it
pulls from GHCR on demand.

---

### `ez-cdc quickstart [--source X] [--sink Y] [--keep-up] [--rebuild]`

Start the dbmazz daemon for the given source/sink pair and open a live
terminal dashboard showing replication metrics in real time.

```
ez-cdc quickstart --source demo-pg --sink demo-starrocks
ez-cdc quickstart                          # interactive prompts if TTY
ez-cdc quickstart --source demo-pg --sink demo-starrocks --keep-up
```

Config file location (unless overridden with `--config PATH` or
`$EZ_CDC_CONFIG`):
- `$XDG_CONFIG_HOME/ez-cdc/config.yaml`, or
- `~/.config/ez-cdc/config.yaml`, or
- `./ez-cdc.yaml` in the current directory (legacy in-repo dev fallback)

Steps performed:
1. Connectivity preflight check against source and sink.
2. Pull the official `ghcr.io/ez-cdc/dbmazz:<version>` image from GHCR
   if not already present locally (or `$DBMAZZ_IMAGE` if overridden).
3. Start the dbmazz container via Docker compose.
4. Wait for dbmazz HTTP health check to pass.
5. Open the dashboard (blocks until `q` or Ctrl+C).
6. Stop and remove the dbmazz container on exit (unless `--keep-up`).

If `--source` or `--sink` are omitted and only one is configured, it is
auto-selected. If multiple are configured and not on a TTY, the command
fails with a list of available options.

---

### `ez-cdc verify [--source X] [--sink Y] [--all] [--quick] [--skip IDs] [--json-report FILE] [--no-up] [--keep-up] [--rebuild]`

Run the full verification suite against a source/sink pair.

```
ez-cdc verify --source demo-pg --sink demo-starrocks
ez-cdc verify --source demo-pg --sink demo-pg-target
ez-cdc verify --source demo-pg --sink demo-starrocks --quick
ez-cdc verify --source demo-pg --sink demo-starrocks --skip D4,H1
ez-cdc verify --source demo-pg --sink demo-starrocks --json-report report.json
ez-cdc verify --no-up   # skip infra startup (assume daemon already running)
```

Flags:

| Flag | Description |
|------|-------------|
| `--source NAME` | Source datasource name |
| `--sink NAME` | Sink datasource name |
| `--all` | Run all source x sink combinations (reserved, not yet implemented) |
| `--quick` | Skip slow checks (D4 TOAST, H1 idempotency) |
| `--skip IDs` | Comma-separated check IDs to skip, e.g. `--skip C10,D4` |
| `--json-report FILE` | Write a JSON report to the specified file |
| `--no-up` | Skip infra/dbmazz startup; assume the daemon is already running |
| `--keep-up` | Do not stop the dbmazz container after verify completes |
| `--rebuild` | Force `docker pull` of the dbmazz image even if present locally |

The command exits with a non-zero status if any check fails, making it
suitable for CI pipelines. The JSON report preserves the full check list with
pass/fail status and duration for each check.

---

### `ez-cdc status`

Print the current status of the running dbmazz daemon container.

```
ez-cdc status
```

Shows stage (snapshot/cdc), LSN positions, event counts, and snapshot
progress if active.

---

### `ez-cdc logs [SERVICE] [--follow] [--tail N]`

Tail container logs. Defaults to following all infra containers.

```
ez-cdc logs dbmazz            # dbmazz daemon logs, follow
ez-cdc logs dbmazz --tail 50  # last 50 lines, no follow
```

Only the `dbmazz` service is managed by the CLI. For source / sink
containers (running under your own Docker Compose or infra), use
`docker logs <container>` directly.

---

### `ez-cdc clean [--source X] [--sink Y] [--yes]`

Clean the target database: truncate all replicated tables and drop dbmazz
audit columns (`_dbmazz_op_type`, `_dbmazz_is_deleted`, etc.).

```
ez-cdc clean --source demo-pg --sink demo-starrocks
ez-cdc clean --yes   # skip confirmation prompt
```

Use this to reset a sink between test runs without tearing down and rebuilding
the containers.

---

### `ez-cdc datasource` (alias: `ez-cdc ds`)

Subcommands for managing datasource configuration in `ez-cdc.yaml`.

#### `ez-cdc datasource list`

List all configured sources and sinks with their type and (redacted) URLs.

```
ez-cdc datasource list
ez-cdc ds list
```

#### `ez-cdc datasource show NAME [--reveal]`

Show full configuration for a single datasource. Passwords are redacted by
default.

```
ez-cdc datasource show demo-pg
ez-cdc datasource show demo-starrocks --reveal   # show password in plain text
```

#### `ez-cdc datasource add`

Interactive wizard to add a new source or sink. Prompts for type, name,
connection URL, and type-specific options. Validates the spec and writes
it to `ez-cdc.yaml`.

```
ez-cdc datasource add
```

#### `ez-cdc datasource test NAME`

Test connectivity to a datasource by opening a real connection.

```
ez-cdc datasource test demo-pg
ez-cdc datasource test demo-starrocks
```

#### `ez-cdc datasource remove NAME [--yes]`

Remove a datasource from `ez-cdc.yaml`. Prompts for confirmation unless
`--yes` is passed.

```
ez-cdc datasource remove my-old-sink
ez-cdc datasource remove my-old-sink --yes
```

#### `ez-cdc datasource init [--template {blank|demo}]`

Create a starter config file at the resolved config path (default
`$XDG_CONFIG_HOME/ez-cdc/config.yaml`). Refuses to overwrite an
existing file.

```
ez-cdc datasource init                     # default: blank template
ez-cdc datasource init --template blank    # explicit blank
ez-cdc datasource init --template demo     # in-repo demos (dev/e2e)
```

`--template blank` (default) writes a fully commented template with
every dbmazz option inline plus commented examples for PostgreSQL,
StarRocks, PostgreSQL target, and Snowflake sinks. Edit it by hand
or follow up with `ez-cdc datasource add` for a guided wizard.

`--template demo` adds the in-repo `demo-pg` + `demo-starrocks`
(or `demo-pg-target`) datasources used by the e2e test harness.
Only useful when running inside a clone of this repo.

---

## Dashboard

`ez-cdc quickstart` opens a full-screen terminal dashboard built with
[ratatui](https://github.com/ratatui-org/ratatui).

### Layout

- **Top row**: hero metrics — stage (snapshot/cdc), replication lag, total
  events processed, and uptime.
- **Sparkline**: throughput history over the last 60 ticks (one tick per 500ms).
- **Table**: per-table row counts — source vs. target, updated every tick.
- **Status bar**: transient status messages (e.g. traffic generator state).
- **Keybind bar**: available key bindings shown at the bottom.

### Keybindings

| Key | Action |
|-----|--------|
| `t` | Toggle background traffic generator on/off |
| `p` | Pause / resume the traffic generator |
| `l` | Show dbmazz container logs (inline overlay) |
| `s` | (reserved — snapshot trigger, not yet implemented) |
| `q` | Quit dashboard and stop dbmazz container |
| `Ctrl+C` | Same as `q` |

### Traffic generator

The dashboard includes a built-in traffic generator that continuously runs
INSERT (70%), UPDATE (25%), and DELETE (5%) operations against the source
`orders` and `order_items` tables at a configurable rate. Stats (inserts,
updates, deletes, errors) are shown in the dashboard. Use `t` to toggle it
on or off, and `p` to pause without stopping.

---

## Verify Checks

`ez-cdc verify` runs checks in two tiers. A failing precheck stops execution.

### Precheck tier (connectivity and readiness)

| ID | Description |
|----|-------------|
| P1 | dbmazz daemon HTTP health check passes |
| P2 | Source datasource is reachable |
| P3 | Target datasource is reachable |
| P4 | dbmazz has reached the CDC stage (waits up to 120 seconds) |

P4 waits for the daemon to complete its initial snapshot (if `do_snapshot=true`)
and begin streaming WAL events. If the daemon does not reach CDC stage within
120 seconds, the check fails and the suite is aborted.

### Tier 1 — Correctness baseline

Checks run in order. Each check that depends on a prior result (e.g. B2
depends on B1) is skipped if the prerequisite check was not reached.

| ID | Series | Description |
|----|--------|-------------|
| A1 | Schema | All source tables are present in the target |
| A2 | Schema | All source columns exist in target tables (case-insensitive) |
| A3 | Schema | Sink audit columns are present in all target tables |
| A4 | Schema | Metadata table has rows (skipped for sinks that have no metadata table) |
| B1 | Snapshot | Row counts match between source and target after snapshot |
| B1b | Snapshot | Spot-check: PK sets match between source and target |
| B3 | Snapshot | No duplicate PKs exist in any target table |
| CDC | CDC | Single-row CDC flow: D1 INSERT, D2 UPDATE, E1 sequential UPDATEs, D3 DELETE |
| D5 | CDC | Multi-row INSERT: 10 rows in a single transaction, all arrive in target |
| D4 | CDC | TOAST column survives an unrelated UPDATE (9 KB text not lost from WAL) |
| C10 | Types | NULL roundtrip: a NULL value in source arrives as NULL in target |
| B2 | Snapshot | Post-CDC delta: source and target row counts still match after CDC operations |
| H1 | Idempotency | No drift: with no traffic, target row counts do not change over 5 seconds |

#### Notes on specific checks

**CDC (single-row flow)**: Inserts a sentinel row with a unique status value,
waits for it to appear in the target, updates the status twice (E1: sequential
updates), then deletes the row. For sinks that support hard delete (PostgreSQL
target), waits for the row to disappear. For sinks with soft delete (StarRocks,
Snowflake), waits for the row to be marked with `_dbmazz_is_deleted=true`.

**D4 (TOAST)**: PostgreSQL stores large column values out-of-line (TOAST). When
an UPDATE does not touch the large column, the WAL event may not include its
value. dbmazz must preserve the previous value. This check inserts a row with
a 9 KB text value, verifies it arrived in the target, then updates an unrelated
column and verifies the large text is still intact.

**Quick mode** (`--verify --quick`): skips D4 and H1, which are the slowest
checks.

---

## Adding a New Sink

To add support for a new sink backend in the test harness:

1. Create `src/clients/targets/<mysink>.rs` implementing the `TargetBackend`
   trait. The trait requires:
   - `connect()` and `close()`: lifecycle
   - `name()`: display name
   - `capabilities()`: returns `BackendCapabilities` (settle times, whether
     hard delete is supported, whether a metadata table exists)
   - `list_tables()`, `get_columns()`, `count_rows()`: schema and count queries
   - `row_exists()`, `row_is_live()`, `fetch_value()`: row-level assertions
   - `list_primary_keys()`, `count_duplicates_by_pk()`: PK queries
   - `expected_audit_columns()`: which audit columns to check for in A3
   - `metadata_row_count()`: for A4 (return 0 if not applicable)

2. Add the new variant to `SinkSpec` and `SinkType` in
   `src/config/schema.rs`.

3. Add a match arm in `src/instantiate.rs` (`instantiate_backend`) to
   construct the new backend from its spec.

4. Add the corresponding sink spec fields to the `ez-cdc.yaml` schema
   documentation above.

The tier 1 checks run against the `TargetBackend` trait — no changes to
the check logic are needed for a new sink.

---

## CI

The `ez-cdc` CLI is built and tested as part of the standard CI pipeline
(`.github/workflows/ci.yml`). The `cargo build --release` step covers the
entire workspace including the `e2e-cli` crate.

The full e2e verification suite (verify + Docker infrastructure) is not run
in the standard CI pipeline because it requires Docker, a cross-compiled
binary, and a running StarRocks instance. To validate a sink change, run
`ez-cdc verify` locally.

---

## Architecture

```
e2e-cli/
├── src/
│   ├── main.rs             CLI entry point, subcommand dispatch, interactive menu
│   ├── commands/           One module per subcommand
│   │   ├── up.rs           Start infra containers
│   │   ├── down.rs         Stop infra containers
│   │   ├── quickstart.rs   Start daemon + open dashboard
│   │   ├── verify_cmd.rs   Orchestrate verify: up + run + report + down
│   │   ├── datasource.rs   CRUD for datasource config
│   │   ├── status.rs       Print daemon status
│   │   ├── logs.rs         Tail container logs
│   │   └── clean.rs        Truncate target tables
│   ├── clients/
│   │   ├── dbmazz.rs       Client for the dbmazz daemon container
│   │   ├── source_pg.rs    PostgreSQL source client (insert, update, delete, count)
│   │   └── targets/        Target backend implementations
│   │       ├── mod.rs      TargetBackend trait + BackendCapabilities
│   │       ├── starrocks.rs StarRocks (mysql_async over wire MySQL)
│   │       ├── postgres.rs  PostgreSQL (tokio-postgres)
│   │       └── snowflake.rs Snowflake (HTTP API)
│   ├── compose/
│   │   ├── builder.rs      Dynamically generate Docker compose YAML for any pair
│   │   └── runner.rs       Wrap `docker compose up/down/logs/ps`
│   ├── config/
│   │   ├── schema.rs       Serde types for ez-cdc.yaml (SourceSpec, SinkSpec, PipelineSettings)
│   │   ├── store.rs        DatasourceStore: load, save, add, remove
│   │   ├── loader.rs       YAML + env var interpolation
│   │   └── presets.rs      Demo datasource definitions for `datasource init`
│   ├── verify/
│   │   ├── runner.rs       VerifyRunner: orchestrates precheck + tier execution
│   │   ├── tier1.rs        All tier 1 check functions (A, B, C, D, E, H series)
│   │   └── polling.rs      wait_until helper: async polling with timeout
│   ├── tui/
│   │   ├── dashboard.rs    ratatui full-screen dashboard (QuickstartDashboard)
│   │   ├── banner.rs       ASCII banner on startup
│   │   ├── report.rs       Check result formatting (PASS/FAIL/SKIP) and JSON serialization
│   │   ├── prompts.rs      Interactive select prompts (cliclack)
│   │   └── theme.rs        Brand colors
│   ├── load/
│   │   └── generator.rs    Background traffic generator (INSERT/UPDATE/DELETE loop)
│   ├── instantiate.rs      Factory: SourceSpec/SinkSpec → concrete client instances
│   ├── preflight.rs        Connectivity checks before starting the daemon
│   ├── daemon.rs           Daemon lifecycle helpers
│   └── paths.rs            Compile-time and runtime path resolution
├── fixtures/
│   ├── postgres-seed.sql   Schema + 500 seed orders + 1500 order items
│   └── starrocks-init.sh   StarRocks database creation script
└── ez-cdc.yaml             Generated config (not in git)
```

### How the daemon is containerized

`ez-cdc quickstart` and `ez-cdc verify` pull the official `dbmazz` image
from GitHub Container Registry:

1. The image reference is `ghcr.io/ez-cdc/dbmazz:<CLI version>`. The CLI
   version is pinned at compile time via `env!("CARGO_PKG_VERSION")`, and
   the release workflow patches `e2e-cli/Cargo.toml` before building to
   keep daemon and CLI versions aligned.
2. `compose/builder.rs` generates a Docker compose file that references
   the image directly — no bind-mounts, no cross-compile loop.
3. To test a locally-patched daemon, set `DBMAZZ_IMAGE=my-tag:dev` and
   the CLI will use that image instead of the GHCR one. Build it with
   `docker build -t my-tag:dev <path-to-dbmazz>` from the root Dockerfile.

### Networking model

The `dbmazz` container runs on the default docker-compose bridge
network with one extra host entry:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

This makes `host.docker.internal` resolve to the host machine on
macOS, Windows, and Linux (Docker Engine 20.10+). When the CLI
generates the `.env` file for the container, it automatically
rewrites any `localhost`, `127.0.0.1`, or `0.0.0.0` host in your
datasource URLs to `host.docker.internal`, preserving the port and
the rest of the URL. This means you can write:

```yaml
sources:
  my-pg:
    url: postgres://user:pass@localhost:5432/mydb
```

...in your `ez-cdc.yaml`, and the container will reach a postgres
running on the host (either native on the host OS, or in another
container that publishes port 5432). Remote hostnames
(`prod-db.internal`, `xy12345.snowflakecomputing.com`, etc.) are
passed through unchanged.

**Edge case**: if your source or sink lives in another container on
a private docker bridge *without* publishing its port to the host,
`host.docker.internal` cannot reach it. Publish the port with
`-p <host>:<container>` so it becomes visible on the host loopback.

### Compose file generation

`builder.rs` generates one compose file per (source, sink) pair under
`.cache/compose/<src>__<sink>__<hash>/compose.yml`. The file contains
a single `dbmazz` service referencing the official
`ghcr.io/ez-cdc/dbmazz:<version>` image, plus the `extra_hosts` entry
described above.

Environment variables for the pair (all `dbmazz` config) are written
to a parallel `.env` file derived from `PipelineSettings.to_env_lines()`,
with localhost URLs rewritten to `host.docker.internal`.

---

## Building from Source

The preferred way to install the CLI is via the one-liner installer
documented in the repo root README:

```bash
curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh
```

This installer downloads the pre-built binary from the latest GitHub
release. Only build from source if you are developing the CLI itself:

```bash
cargo build --release --manifest-path e2e-cli/Cargo.toml
```

The resulting binary is at `e2e-cli/target/release/ez-cdc`. You can run
it directly or symlink it into your PATH.

### Developing against a patched dbmazz daemon

The CLI pulls `ghcr.io/ez-cdc/dbmazz:<CLI version>` by default. To test
the CLI against a local daemon build:

```bash
# From the repo root, build the daemon and the Docker image locally
cargo build --release --target x86_64-unknown-linux-musl
cp target/x86_64-unknown-linux-musl/release/dbmazz dbmazz-linux-amd64
docker build -t dbmazz-dev:local .

# Point the CLI at the local image
DBMAZZ_IMAGE=dbmazz-dev:local ez-cdc quickstart --source demo-pg --sink demo-starrocks
```

---

## Troubleshooting

### `docker pull` fails with "unauthorized" or "manifest unknown"

The CLI pulls `ghcr.io/ez-cdc/dbmazz:<version>` from GHCR. If the image
is not publicly accessible or the tag does not exist:

- Verify your CLI version matches a published release:
  `ez-cdc --version`, then check
  https://github.com/ez-cdc/dbmazz/pkgs/container/dbmazz
- Override with a known-good image: `DBMAZZ_IMAGE=ghcr.io/ez-cdc/dbmazz:latest`
- Build locally as shown in the "Developing against a patched dbmazz
  daemon" section above.

### "Docker not running" or compose command fails

Ensure Docker Desktop is started. The `ez-cdc` CLI calls `docker compose`
directly. Verify with:

```
docker info
docker compose version
```

### Port conflicts

The demo datasources use non-standard ports to avoid conflicts with locally
installed services:

| Service | Port |
|---------|------|
| Source PostgreSQL | 15432 |
| Target PostgreSQL | 25432 |
| StarRocks HTTP (Stream Load) | 18030 |
| StarRocks MySQL wire | 19030 |

If another process is using one of these ports, start your source or
sink on different ports and update the URLs in `ez-cdc.yaml`
accordingly.

### "source or sink unreachable"

The preflight connectivity check in `quickstart` and `verify` connects
to the source and sink before starting dbmazz. Start your source /
sink infrastructure first (docker-compose, cloud, whatever), then
verify each connection with:

```
ez-cdc datasource test <name>
```

### StarRocks takes too long to start

StarRocks initializes internal metadata on first run, which typically
takes 60-90 seconds. Wait for it to become healthy before running
`ez-cdc verify`, then check progress with `docker logs` against your
StarRocks container.

### Daemon does not reach CDC stage (P4 timeout)

The P4 precheck waits up to 120 seconds for dbmazz to complete snapshot and
enter CDC mode. If it times out, check the daemon logs:

```
ez-cdc logs dbmazz
```

Common causes: snapshot is running slower than expected (increase
`snapshot_chunk_size` or reduce the number of seed rows), or the sink is
returning errors during the initial load.
