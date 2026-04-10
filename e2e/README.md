# EZ-CDC e2e test harness

Interactive CLI for running dbmazz end-to-end: try a sink with a live
terminal dashboard (`quickstart`), run validation suites (`verify`), or
manage the docker compose stack (`up`, `down`, `logs`, `status`).

Replaces the old `deploy/test-sink.sh` + `deploy/load-test.py` scripts
with a single unified Python CLI (`ez-cdc`).

## First time setup

```bash
# From the repo root
pip install -e e2e/

# Optional but recommended — enables Tab completion in your shell
ez-cdc --install-completion
```

> `pip install -e` installs the package in editable mode, so changes to
> `e2e/src/ez_cdc_e2e/` take effect without reinstalling. Use a virtualenv
> (`python -m venv .venv && source .venv/bin/activate`) if you want to
> isolate the deps from your system Python.

## Try dbmazz in 30 seconds

```bash
ez-cdc quickstart --source demo-pg --sink demo-starrocks
```

Or just run `ez-cdc` for the interactive menu — it will guide you
through selecting a source and sink, creating demo datasources if needed,
and launching the dashboard.

The CLI will:
1. Generate a docker compose stack for the selected source/sink pair.
2. Build dbmazz from source and start all containers.
3. Wait for dbmazz to reach the CDC stage.
4. Open an interactive live dashboard showing stage, replication lag,
   throughput, and source → target row counts, updated twice per second.

Keybindings in the dashboard:

| Key | Action |
|---|---|
| `q` / `Ctrl+C` | Exit the dashboard |
| `l` | Tail `docker compose logs -f` for the stack |
| `p` | Pause the daemon (`POST /pause`) |
| `r` | Resume the daemon (`POST /resume`) |
| `t` | Toggle traffic generator (paused by default) |
| `s` | Trigger a snapshot |

When you exit, the CLI asks whether to stop and destroy the stack. Say
yes to clean up, no to leave it running for further inspection.

## Datasources

All source/sink configurations live in `e2e/ez-cdc.yaml`. Each
datasource has a name, a type (source or sink), and connection details.

### Bundled demos (Docker-managed)

The CLI ships with three demo datasources that run entirely in Docker:

| Name | Type | Description |
|---|---|---|
| `demo-pg` | source (postgres) | PostgreSQL 16 with WAL logical + seed data |
| `demo-starrocks` | sink (starrocks) | StarRocks all-in-one |
| `demo-pg-target` | sink (postgres) | PostgreSQL 16 as replication target |

These are auto-created when you run `ez-cdc datasource init-demos` or
when the interactive menu detects no datasources exist.

### Your own databases (BYOD)

Add your own datasources with the interactive wizard:

```bash
ez-cdc datasource add
```

The wizard supports PostgreSQL sources, PostgreSQL sinks, StarRocks
sinks, and Snowflake sinks. Credentials are stored in the YAML file —
no separate `.env` files needed.

### Datasource subcommands

```
ez-cdc datasource list          # show all datasources in a table
ez-cdc datasource show NAME     # full details of one datasource
ez-cdc datasource add           # interactive wizard to add a new datasource
ez-cdc datasource remove NAME   # remove a datasource by name
ez-cdc datasource test NAME     # test connectivity (user-managed only)
ez-cdc datasource init-demos    # create bundled demo datasources
```

### Variable interpolation

Connection strings in the YAML support `${VAR}` interpolation from
environment variables:

```yaml
sources:
  my-pg:
    type: postgres
    managed: false
    host: ${PG_HOST}
    port: ${PG_PORT:-5432}
    database: ${PG_DATABASE}
    user: ${PG_USER}
    password: ${PG_PASSWORD}
```

## Subcommands

```
ez-cdc                                          # interactive main menu
ez-cdc quickstart --source SRC --sink SINK      # launch + live dashboard
ez-cdc verify     --source SRC --sink SINK      # run validation tests
ez-cdc verify     --all                         # run all managed pairs
ez-cdc up         --source SRC --sink SINK      # just bring the stack up
ez-cdc down       --source SRC --sink SINK      # stop and destroy
ez-cdc logs       --source SRC --sink SINK      # tail compose logs
ez-cdc status     --source SRC --sink SINK      # one-shot /status snapshot
ez-cdc datasource ...                           # manage datasources (see above)
ez-cdc --install-completion                     # install shell tab completion
ez-cdc --version
ez-cdc --help
```

All subcommands accept `--source` and `--sink` options. If you omit
them in an interactive terminal, the CLI will prompt with a selector.
In non-interactive mode (CI, piped stdout), omitting them is a hard error.

### `verify` flags

| Flag | Effect |
|---|---|
| `--quick` | Run tier 1 only (~30s per pair), skip tier 2 |
| `--skip IDS` | Skip specific check IDs, comma-separated, e.g. `--skip C3,D7` |
| `--all` | Run all (managed source × managed sink) pairs |
| `--json-report PATH` | Write a structured JSON report (for CI) |
| `--keep-up` | Don't run `compose down` at the end (for debugging) |
| `--no-up` | Assume the compose stack is already running |

## Supported sink types

| Sink Type | Source | Target | Requirements |
|---|---|---|---|
| StarRocks | PostgreSQL | StarRocks | Docker only (demo) or your own instance |
| PostgreSQL | PostgreSQL | PostgreSQL | Docker only (demo) or your own instance |
| Snowflake | PostgreSQL | Snowflake (cloud) | Your Snowflake account credentials |

## Validation tiers

The `verify` subcommand runs a tiered suite of checks. Every check has
a two-letter ID (e.g., `A1`, `D4`) that you can skip individually.

### Tier 1 — Correctness baseline (~30s per sink, runs with `--quick`)

| ID | Check |
|---|---|
| A1 | Target tables present |
| A2 | Source columns in target |
| A3 | Audit columns present |
| A4 | Metadata table has rows (skipped for sinks without one) |
| B1 | Snapshot counts match |
| B3 | Zero duplicates by PK |
| C10 | NULL roundtrip |
| D1 | INSERT replicated |
| D2 | UPDATE replicated |
| D3 | DELETE replicated |
| **D4** | **TOAST UPDATE preserves unchanged value** — bug #1 in PG CDCs |
| D5 | Multi-row INSERT in single TX |
| E1 | Sequential UPDATEs, last wins |
| B2 | Post-CDC delta matches |
| H1 | Restart without traffic is no-op |

### Tier 2 — Extended validation (~2 min, runs by default in PR 2)

A5 schema evolution · A6 type compatibility · B4 no orphan rows ·
C1 row-level hash compare · C2–C7 type fidelity fixture · D6 mixed TX
atomicity · D7 PK UPDATE · E2 transactional consistency ·
E3 LSN monotonicity · H2 crash replay idempotency.

Tier 2 lands in PR 2. Until then, `verify` runs tier 1 only.

### Tier 3 — Nightly (future)

F1–F6 resilience tests (kill dbmazz / target / source during CDC and
snapshot) will run as a nightly pipeline. Tier 3 is not part of the
standard `verify` run.

## Adding a new sink

Adding a sink takes ~3 files and zero changes to the verify runner,
because the runner only calls methods on `TargetBackend`. See
[`docs/contributing-connectors.md`](../docs/contributing-connectors.md#end-to-end-tests-required-for-sink-connectors)
for the full checklist. Briefly:

1. **`e2e/src/ez_cdc_e2e/backends/my_sink.py`** — subclass `TargetBackend`,
   implement its abstract methods. Look at `backends/postgres.py` or
   `backends/snowflake.py` for reference implementations.

2. **`e2e/src/ez_cdc_e2e/datasources/schema.py`** — add a `MySinkSpec`
   pydantic model and register it in the `SinkSpec` discriminated union.

3. **`e2e/src/ez_cdc_e2e/compose_builder.py`** — add a `_sink_services_*`
   function to generate the docker compose services for your sink.

Then `ez-cdc verify --source demo-pg --sink my-sink` runs the full
Tier 1 suite against your sink without you writing any test code.

## CI integration

The `--non-interactive` and `--json-report` flags are designed for CI:

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

A GitHub Actions workflow for this (matrix over sinks) is planned for a
follow-up PR — see `.plans/e2e-refactor.md` section 11.

## Troubleshooting

**`ez-cdc: command not found`** — you haven't installed the CLI yet.
Run `pip install -e e2e/` from the repo root.

**`psycopg2-binary` fails to install** — on some Linux distros you need
`libpq-dev` installed first. On macOS, `psycopg2-binary` should install
from a wheel without extra setup.

**Snowflake tests fail with missing credentials** — add your Snowflake
datasource via `ez-cdc datasource add` and fill in account, user,
password, warehouse, database, and schema.

**dbmazz never reaches CDC stage** — check `ez-cdc logs --source X --sink Y`
to see what the daemon is stuck on. Common causes: the target is
unreachable, the source publication is misconfigured, or StarRocks
hasn't finished its 60s warmup.

**Banner looks broken** — your terminal is probably narrower than 56
columns. Widen the terminal or use `--no-banner`.

## Design notes

- **Python, not Rust.** Snowflake's Python driver is the only mature
  client library, and the e2e harness needs to talk to every target
  sink. Trading the Rust mono-lingualism for `pip install
  snowflake-connector-python` was the right call. See
  `.plans/e2e-refactor.md` D1 for the full rationale.

- **No Makefile.** The `ez-cdc` console script is short enough (`ez-cdc
  verify --source X --sink Y` vs `make e2e-verify SINK=starrocks`) that
  wrapping it in a Makefile would just add indirection.

- **`rich` + `questionary` + `typer`.** Rich for display, questionary
  for prompts, typer as the CLI framework (built on click). This
  combination is what the Python CLI community has settled on — see
  `stripe-cli`, `poetry`, `gh cli` for similar patterns.

- **Datasources as first-class entities.** All connection configs live in
  `e2e/ez-cdc.yaml` — a single file for all sources, sinks, and pipeline settings,
  managed via `ez-cdc datasource` subcommands. Docker compose files are
  generated programmatically per (source, sink) pair and cached at
  `e2e/.cache/compose/`.

- **Banner colors are real brand colors.** Pulled from
  `dev-workspace/brand/colors.json` (Tailwind Blue primary palette).
  If you're looking at a gradient and wondering why that specific blue,
  it's because that's the EZ-CDC logo color.
