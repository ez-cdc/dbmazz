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
ez-cdc quickstart starrocks
```

The CLI will:
1. Start the docker compose stack for the `starrocks` profile.
2. Wait for dbmazz to reach the CDC stage.
3. Open an interactive live dashboard showing stage, replication lag,
   throughput, and source → target row counts, updated twice per second.

Keybindings in the dashboard:

| Key | Action |
|---|---|
| `q` / `Ctrl+C` | Exit the dashboard |
| `l` | Tail `docker compose logs -f` for the stack |
| `p` | Pause the daemon (`POST /pause`) |
| `r` | Resume the daemon (`POST /resume`) |
| `s` | Trigger a snapshot (currently requires `grpcurl` fallback) |

When you exit, the CLI asks whether to stop and destroy the stack. Say
yes to clean up, no to leave it running for further inspection.

## Subcommands

```
ez-cdc                          # interactive main menu (banner + prompts)
ez-cdc quickstart [SINK]        # launch a sink + live dashboard
ez-cdc verify    [SINK]         # run e2e validation tests (tier 1 + 2 default)
ez-cdc verify    --all          # run all runnable sinks (auto-detects snowflake)
ez-cdc load      [SINK]         # load test (PR 3, not yet implemented)
ez-cdc up        [SINK]         # just bring the stack up
ez-cdc down      [SINK]         # stop and destroy
ez-cdc logs      [SINK]         # tail compose logs
ez-cdc status    [SINK]         # one-shot dbmazz /status snapshot
ez-cdc --install-completion     # install shell tab completion
ez-cdc --version
ez-cdc --help
```

All subcommands accept the sink as a positional argument. If you omit
it and you're in an interactive terminal, the CLI will prompt with a
selector. In non-interactive mode (CI, piped stdout), omitting the sink
is a hard error.

### `verify` flags

| Flag | Effect |
|---|---|
| `--quick` | Run tier 1 only (~30s per sink), skip tier 2 |
| `--tier N` | Run only a specific tier (1, 2, 3) |
| `--skip IDS` | Skip specific check IDs, comma-separated, e.g. `--skip C3,D7` |
| `--all` | Run all runnable sinks; auto-detects snowflake if `e2e/.env.snowflake` exists |
| `--json-report PATH` | Write a structured JSON report (for CI) |
| `--keep-up` | Don't run `compose down` at the end (for debugging) |
| `--no-up` | Assume the compose stack is already running |

## Supported profiles

| Profile | Source | Target | Requirements |
|---|---|---|---|
| `starrocks` | PostgreSQL | StarRocks | Docker only |
| `pg-target` | PostgreSQL | PostgreSQL | Docker only |
| `snowflake` | PostgreSQL | Snowflake (cloud) | `e2e/.env.snowflake` |

### Snowflake credentials

Snowflake runs in the cloud, not in Docker. Configure credentials once:

```bash
cp e2e/.env.snowflake.example e2e/.env.snowflake
# Edit e2e/.env.snowflake with your account, user, password, warehouse
```

After that, `ez-cdc quickstart snowflake` and `ez-cdc verify snowflake`
work the same as the other profiles. `ez-cdc verify --all` also picks
snowflake up automatically when the file is present.

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

Adding a sink takes ~4 files and zero changes to the verify runner,
because the runner only calls methods on `TargetBackend`. See
[`docs/contributing-connectors.md`](../docs/contributing-connectors.md#end-to-end-tests-required-for-sink-connectors)
for the full checklist. Briefly:

1. **`e2e/src/ez_cdc_e2e/backends/my_sink.py`** — subclass `TargetBackend`,
   implement its abstract methods. Look at `backends/postgres.py` or
   `backends/snowflake.py` for reference implementations.

2. **`e2e/src/ez_cdc_e2e/profiles.py`** — add a `ProfileSpec` entry
   mapping the sink name to your backend class.

3. **`e2e/compose.yml`** — add a service with `profiles: ["my-sink"]`
   that runs dbmazz with `SINK_TYPE=my-sink` and the right connection
   env vars.

4. **`src/connectors/sinks/my_sink/README.md`** — user-facing
   documentation for the new sink (follows the template in
   `src/connectors/sinks/_template/README.md`).

Then `ez-cdc verify my-sink` runs the full Tier 1 suite against your
sink without you writing any test code.

## CI integration

The `--non-interactive` and `--json-report` flags are designed for CI:

```bash
ez-cdc verify starrocks \
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
| 3 | Invalid invocation (unknown sink, bad flags) |
| 130 | User interrupted (Ctrl+C) |

A GitHub Actions workflow for this (matrix over sinks) is planned for a
follow-up PR — see `.plans/e2e-refactor.md` section 11.

## Troubleshooting

**`ez-cdc: command not found`** — you haven't installed the CLI yet.
Run `pip install -e e2e/` from the repo root.

**`psycopg2-binary` fails to install** — on some Linux distros you need
`libpq-dev` installed first. On macOS, `psycopg2-binary` should install
from a wheel without extra setup.

**Snowflake tests fail with `SINK_SNOWFLAKE_ACCOUNT is required`** — you
haven't created `e2e/.env.snowflake`. Copy the example and fill it in.

**dbmazz never reaches CDC stage** — check `ez-cdc logs <sink>` to see
what the daemon is stuck on. Common causes: the target is unreachable,
the source publication is misconfigured, or StarRocks hasn't finished
its 60s warmup. Bumping the timeout via `--verbose` output often reveals
the root cause.

**Banner looks broken** — your terminal is probably narrower than 56
columns, and the fallback banner D should be used instead. Widen the
terminal or use `--no-banner`.

## Design notes

- **Python, not Rust.** Snowflake's Python driver is the only mature
  client library, and the e2e harness needs to talk to every target
  sink. Trading the Rust mono-lingualism for `pip install
  snowflake-connector-python` was the right call. See
  `.plans/e2e-refactor.md` D1 for the full rationale.

- **No Makefile.** The `ez-cdc` console script is short enough (`ez-cdc
  verify starrocks` vs `make e2e-verify SINK=starrocks`) that wrapping
  it in a Makefile would just add indirection. Control-plane and
  worker-agent have Makefiles because their build/test surface is
  larger.

- **`rich` + `questionary` + `typer`.** Rich for display, questionary
  for prompts, typer as the CLI framework (built on click). This
  combination is what the Python CLI community has settled on — see
  `stripe-cli`, `poetry`, `gh cli` for similar patterns.

- **Banner colors are real brand colors.** Pulled from
  `dev-workspace/brand/colors.json` (Tailwind Blue primary palette).
  If you're looking at a gradient and wondering why that specific blue,
  it's because that's the EZ-CDC logo color.
