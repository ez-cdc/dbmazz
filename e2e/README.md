# EZ-CDC e2e test harness

Interactive CLI for running dbmazz end-to-end: try a sink with a live
terminal dashboard (`quickstart`), run validation suites (`verify`), or
manage the infrastructure stack (`up`, `down`, `logs`).

## Quick start (clone to running tests)

```bash
# 1. Clone and install
git clone <repo> && cd dbmazz
pip install -e e2e/

# 2. Create the config file (ez-cdc.yaml) with demo datasources
ez-cdc                        # interactive menu → "Init config"

# 3. Start all infrastructure containers
ez-cdc up

# 4. Run the validation suite against StarRocks
ez-cdc verify --source demo-pg --sink demo-starrocks

# 5. Or open a live dashboard against PostgreSQL target
ez-cdc quickstart --source demo-pg --sink demo-pg-target

# 6. Stop all containers when done
ez-cdc down
```

> **Tip:** Use a virtualenv (`python -m venv .venv && source .venv/bin/activate`)
> if you want to isolate the deps from your system Python.

## How it works

The CLI manages two things independently:

1. **Infrastructure** (`ez-cdc up/down/logs`) -- Docker containers for all
   datasources defined in `ez-cdc.yaml`. One compose stack for everything.
   No per-pair containers.

2. **dbmazz runs** (`quickstart/verify/clean/status`) -- Build and run
   the dbmazz daemon for a specific `--source X --sink Y` pair. Infra
   must be running first. Preflight checks validate connectivity and
   schema before starting the daemon.

## Configuration: ez-cdc.yaml

The file `e2e/ez-cdc.yaml` holds all datasources and pipeline settings.
It does **not** exist at clone time. The CLI creates it on first run
via the interactive menu ("Init config") or via `ez-cdc datasource init-demos`.

All datasources have explicit connection details (host, port, user,
password, or a URL). Docker compose services are generated from these.

### Example

```yaml
settings:
  flush_size: 2000
  flush_interval_ms: 2000
  do_snapshot: true

sources:
  demo-pg:
    type: postgres
    url: postgres://postgres:postgres@localhost:15432/dbmazz
    seed: postgres-seed.sql
    tables:
      - orders
      - order_items

sinks:
  demo-starrocks:
    type: starrocks
    url: http://localhost:18030
    mysql_port: 19030
    database: dbmazz
    user: root
    password: ""

  demo-pg-target:
    type: postgres
    url: postgres://postgres:postgres@localhost:25432/dbmazz_target
    database: dbmazz_target
    schema: public
```

### Variable interpolation

Connection strings support `${VAR}` interpolation from environment variables:

```yaml
sources:
  my-pg:
    type: postgres
    host: ${PG_HOST}
    port: ${PG_PORT:-5432}
    database: ${PG_DATABASE}
    user: ${PG_USER}
    password: ${PG_PASSWORD}
```

## Datasources

### Bundled demos

The CLI ships with three demo datasources that run in Docker:

| Name | Role | Connection |
|---|---|---|
| `demo-pg` | source (postgres) | `postgres://postgres:postgres@localhost:15432/dbmazz` |
| `demo-starrocks` | sink (starrocks) | HTTP `localhost:18030`, MySQL `localhost:19030` |
| `demo-pg-target` | sink (postgres) | `postgres://postgres:postgres@localhost:25432/dbmazz_target` |

These are created when you run `ez-cdc datasource init-demos` or when
the interactive menu detects no config file exists.

### Your own databases

Add your own datasources with the interactive wizard:

```bash
ez-cdc datasource add
```

The wizard supports PostgreSQL sources, PostgreSQL sinks, StarRocks
sinks, and Snowflake sinks. Credentials are stored in the YAML file.

### Datasource subcommands

| Command | Description |
|---|---|
| `ez-cdc datasource list` | Show all datasources in a table |
| `ez-cdc datasource add` | Interactive wizard to add a new datasource |
| `ez-cdc datasource remove NAME` | Remove a datasource by name |
| `ez-cdc datasource test NAME` | Test connectivity |
| `ez-cdc datasource init-demos` | Create demo datasources |

## CLI subcommands

| Command | Description |
|---|---|
| `ez-cdc` | Interactive menu (contextual based on state) |
| `ez-cdc up` | Start all infra containers from ez-cdc.yaml |
| `ez-cdc down` | Stop all infra containers |
| `ez-cdc quickstart --source X --sink Y` | Preflight + dbmazz + live dashboard |
| `ez-cdc verify --source X --sink Y` | Preflight + dbmazz + e2e test suite |
| `ez-cdc clean --source X --sink Y` | Clean target DB (TRUNCATE + DROP audit cols) |
| `ez-cdc logs` | Tail infra container logs |
| `ez-cdc status --source X --sink Y` | One-shot dbmazz status |
| `ez-cdc --install-completion` | Install shell tab completion |
| `ez-cdc --version` | Show version |

In an interactive terminal, `--source` and `--sink` can be omitted and
the CLI will prompt with a selector. In non-interactive mode (CI, piped
stdout), omitting them is a hard error.

### `quickstart` dashboard

The `quickstart` command validates connectivity, starts dbmazz, waits
for the CDC stage, and opens an interactive live dashboard showing stage,
replication lag, throughput, and source-to-target row counts.

| Key | Action |
|---|---|
| `q` / `Ctrl+C` | Exit the dashboard and stop dbmazz |
| `l` | Tail container logs |
| `p` | Pause the daemon |
| `r` | Resume the daemon |
| `t` | Toggle traffic generator |
| `s` | Trigger a snapshot |

### `verify` flags

| Flag | Effect |
|---|---|
| `--quick` | Run tier 1 only (~30s per pair), skip tier 2 |
| `--skip IDS` | Skip specific check IDs, comma-separated (e.g. `--skip C3,D7`) |
| `--json-report PATH` | Write a structured JSON report (for CI) |
| `--no-color` | Disable colored output |
| `--non-interactive` | Disable prompts |

## Pipeline settings

The `settings:` section in `ez-cdc.yaml` controls how dbmazz behaves
during replication. Each field maps 1:1 to a dbmazz environment variable.

| Setting | Env var | Default | Description |
|---|---|---|---|
| `flush_size` | `FLUSH_SIZE` | `2000` | Max events per batch before flushing to the sink |
| `flush_interval_ms` | `FLUSH_INTERVAL_MS` | `2000` | Max milliseconds before flushing a partial batch |
| `do_snapshot` | `DO_SNAPSHOT` | `true` | Run initial snapshot (backfill) on startup |
| `snapshot_chunk_size` | `SNAPSHOT_CHUNK_SIZE` | `10000` | Rows per snapshot chunk (PK-range based) |
| `snapshot_parallel_workers` | `SNAPSHOT_PARALLEL_WORKERS` | `2` | Concurrent snapshot workers (1-32) |
| `initial_snapshot_only` | `INITIAL_SNAPSHOT_ONLY` | `false` | Exit after snapshot completes -- no CDC streaming |
| `rust_log` | `RUST_LOG` | `info` | Log level filter (`info`, `debug`, `dbmazz=debug`, etc.) |
| `snowflake_flush_files` | `SINK_SNOWFLAKE_FLUSH_FILES` | `1` | Snowflake: trigger COPY INTO after N staged files (production: 20) |
| `snowflake_flush_bytes` | `SINK_SNOWFLAKE_FLUSH_BYTES` | `104857600` | Snowflake: trigger COPY INTO after N bytes staged (default 100MB) |

## Supported sink types

| Sink Type | Demo name | Docker port(s) | Notes |
|---|---|---|---|
| StarRocks | `demo-starrocks` | 18030 (HTTP), 19030 (MySQL) | All-in-one container |
| PostgreSQL | `demo-pg-target` | 25432 | PostgreSQL 16 as target |
| Snowflake | (add manually) | N/A (cloud) | Requires your Snowflake account |

## Validation tiers

The `verify` subcommand runs a tiered suite of checks. Every check has
a two-letter ID (e.g., `A1`, `D4`) that you can skip individually.

### Tier 1 -- Correctness baseline (~30s per sink, runs with `--quick`)

**Precheck:** before any check runs, verify confirms the target tables
are empty (no stale data from a previous run). If dirty tables are found,
verify exits with a clear message listing what to clean.

| ID | Check |
|---|---|
| A1 | Target tables present |
| A2 | Source columns in target |
| A3 | Audit columns present |
| A4 | Metadata table has rows (skipped for sinks without one) |
| B1 | Snapshot row counts match (source vs target per table) |
| B1b | Snapshot content matches (PK sets + spot-check column values) |
| B3 | Zero duplicates by PK |
| D1 | INSERT replicated |
| D2 | UPDATE replicated |
| E1 | Sequential UPDATEs, last wins |
| D3 | DELETE replicated |
| D5 | Multi-row INSERT in single TX |
| **D4** | **TOAST UPDATE preserves unchanged value** |
| C10 | NULL roundtrip |
| B2 | Post-CDC delta matches |
| H1 | Restart without traffic is no-op |

### Tier 2 -- Extended validation (~2 min)

A5 schema evolution, A6 type compatibility, B4 no orphan rows,
C1 row-level hash compare, C2-C7 type fidelity fixture, D6 mixed TX
atomicity, D7 PK UPDATE, E2 transactional consistency,
E3 LSN monotonicity, H2 crash replay idempotency.

### Tier 3 -- Nightly (future)

F1-F6 resilience tests (kill dbmazz / target / source during CDC and
snapshot) will run as a nightly pipeline. Tier 3 is not part of the
standard `verify` run.

## Snowflake testing

Snowflake has no Docker container. Add your Snowflake datasource
manually, then run the tests with only the source container running:

```bash
# Add via interactive wizard
ez-cdc datasource add

# Or edit ez-cdc.yaml directly with your Snowflake credentials

# Start infra (only source-pg will have a container, Snowflake is cloud)
ez-cdc up

# Run tests
ez-cdc verify --source demo-pg --sink my-snowflake
```

## CI integration

```bash
ez-cdc datasource init-demos
ez-cdc up
ez-cdc verify --source demo-pg --sink demo-starrocks
ez-cdc verify --source demo-pg --sink demo-pg-target
ez-cdc down
```

For CI pipelines, use `--non-interactive`, `--no-color`, and
`--json-report`:

```bash
ez-cdc verify --source demo-pg --sink demo-starrocks \
    --non-interactive \
    --no-color \
    --json-report /tmp/verify-starrocks.json
```

Exit codes:

| Code | Meaning |
|---|---|
| 0 | All checks passed |
| 1 | At least one check failed |
| 2 | Setup error (compose failed, daemon unreachable, creds missing) |
| 3 | Invalid invocation (unknown datasource, bad flags) |
| 130 | User interrupted (Ctrl+C) |

## Adding a new sink

Adding a sink takes ~3 files and zero changes to the verify runner,
because the runner only calls methods on `TargetBackend`. See
[`docs/contributing-connectors.md`](../docs/contributing-connectors.md#end-to-end-tests-required-for-sink-connectors)
for the full checklist. Briefly:

1. **`e2e/src/ez_cdc_e2e/backends/my_sink.py`** -- subclass `TargetBackend`,
   implement its abstract methods. Look at `backends/postgres.py` or
   `backends/snowflake.py` for reference implementations.

2. **`e2e/src/ez_cdc_e2e/datasources/schema.py`** -- add a `MySinkSpec`
   pydantic model and register it in the `SinkSpec` discriminated union.

3. **`e2e/src/ez_cdc_e2e/compose_builder.py`** -- add a `_sink_services_*`
   function to generate the docker compose services for your sink.

Then `ez-cdc verify --source demo-pg --sink my-sink` runs the full
tier 1 suite against your sink without writing any test code.

## Troubleshooting

**`ez-cdc: command not found`** -- Run `pip install -e e2e/` from the
repo root.

**`psycopg2-binary` fails to install** -- On some Linux distros you need
`libpq-dev` installed first. On macOS, `psycopg2-binary` should install
from a wheel without extra setup.

**Snowflake tests fail with missing credentials** -- Add your Snowflake
datasource via `ez-cdc datasource add` and fill in account, user,
password, warehouse, database, and schema.

**dbmazz never reaches CDC stage** -- Check `ez-cdc logs` to see what
the daemon is stuck on. Common causes: the target is unreachable, the
source publication is misconfigured, or StarRocks hasn't finished its
60s warmup.

**Banner looks broken** -- Your terminal is probably narrower than 56
columns. Widen the terminal or use `--no-banner`.

**Target has stale data** -- Run `ez-cdc clean --source X --sink Y` to
TRUNCATE target tables and DROP audit columns before retrying.

## Design notes

- **Python, not Rust.** Snowflake's Python driver is the only mature
  client library, and the e2e harness needs to talk to every target
  sink. Trading the Rust mono-lingualism for `pip install
  snowflake-connector-python` was the right call.

- **No Makefile.** The `ez-cdc` console script is short enough that
  wrapping it in a Makefile would just add indirection.

- **`rich` + `questionary` + `typer`.** Rich for display, questionary
  for prompts, typer as the CLI framework (built on click).

- **Datasources as first-class entities.** All connection configs live in
  `ez-cdc.yaml` -- a single file for all sources, sinks, and pipeline
  settings, managed via `ez-cdc datasource` subcommands. Docker compose
  files are generated programmatically and cached at `e2e/.cache/compose/`.
