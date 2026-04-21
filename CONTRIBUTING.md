# Contributing to dbmazz

Thanks for your interest in contributing. This guide is the single source
of truth for how to land a change in dbmazz — from environment setup
through opening a mergeable PR.

If you only want to add a new source or sink connector, skim this file
for the process rules, then jump to
[`docs/contributing-connectors.md`](docs/contributing-connectors.md) for
the connector API.

---

## 1. Contribution workflow at a glance

Every change follows the same four steps:

1. **Open a GitHub issue describing the change.** A maintainer will
   confirm scope (or push back) before you invest coding time. Typo
   fixes and doc-only tweaks can skip this step.
2. **Make the change on a feature branch and build a local dbmazz Docker
   image** containing your fix or feature.
3. **Run the full test suite against your image** using the `ez-cdc`
   CLI: `quickstart`, `run`, and `verify` must all pass against a real
   source and a real sink before you open the PR.
4. **Open a PR that references the issue** and includes evidence of the
   tests you ran (console output, screenshots of the dashboard, or the
   JSON report from `ez-cdc verify`).

The rest of this document is the detail on how to do each of those
steps correctly.

---

## 2. Prerequisites

### Toolchain

```bash
# Rust 1.91.1 or newer (required by transitive deps — see Cargo.toml).
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# System dependencies (Debian/Ubuntu).
sudo apt-get install -y \
  protobuf-compiler musl-tools pkg-config perl make \
  libssl-dev libcurl4-openssl-dev
```

macOS (Homebrew): `brew install protobuf`. `musl-tools` is not needed
for local `cargo build` — it's only used by the release workflow for
the static Linux binary cross-compile.

You'll also need:

- **Docker** (with `buildx`) — to build and run the dbmazz image locally.
- **[`ez-cdc` CLI](https://github.com/ez-cdc/ez-cdc-cli-releases)** — the
  end-to-end test harness:
  ```bash
  curl -sSL https://raw.githubusercontent.com/ez-cdc/ez-cdc-cli-releases/main/install.sh | sh
  ```

---

## 3. Build & test

```bash
cargo build --release         # full release build
cargo test                    # unit tests
cargo fmt -- --check          # formatting (CI enforces this)
cargo clippy -- -D warnings   # lint (CI enforces this)
```

Unit tests live inline in each module (`#[cfg(test)]` blocks). There is
no `tests/` directory — end-to-end validation is done through the
`ez-cdc` CLI (see step 4.7 below).

---

## 4. The step-by-step for a contribution

### 4.1 Open an issue

For any change beyond a typo, open an issue first:

- Describe what you want to change and why.
- If it's a bug, include a reproducer.
- If it's a feature, sketch the user-visible surface (new env var? new
  sink method? new observable behavior?).

A maintainer will reply with either "go ahead" (with any scope
adjustments) or "let's talk first." **Wait for that response before
opening a PR.** This keeps us from rejecting work at review time for
reasons that could have been flagged upfront.

### 4.2 Branch from `main`

All changes land on `main` through a reviewed PR — `main` is
protected, so a direct push is rejected.

```bash
git checkout main
git pull
git checkout -b <type>/<short-description>
```

`<type>` is one of `feat`, `fix`, `refactor`, `perf`, `docs`, `test`,
`chore`, `security`. Examples:

- `feat/clickhouse-sink`
- `fix/pg-sink-toast-unchanged`
- `perf/batch-flush-zero-copy`

### 4.3 Make the change

Conventions CI and review will enforce:

- **Format**: `cargo fmt`. CI runs `cargo fmt -- --check`.
- **Lint**: `cargo clippy -- -D warnings`. No warnings allowed.
- **Logging**: use the `tracing` crate (`tracing::info!`, `tracing::warn!`,
  etc.). Never `println!`, `eprintln!`, or the `log` crate directly.
- **No `unwrap()` in production code paths.** Tests are fine. Use `?`
  with `anyhow::Context` for error propagation.
- **Credential safety**: never log connection strings, passwords, or
  auth tokens. Structs holding sensitive fields must not derive
  `Debug` without redacting.
- **Async**: tokio. Never block the runtime — use `spawn_blocking` for
  CPU-heavy work.
- **Naming**: `snake_case` for functions/modules, `PascalCase` for
  types, `SCREAMING_SNAKE_CASE` for constants.

### 4.4 Update `CHANGELOG.md` (mandatory for user-visible changes)

Every change that affects user-visible behavior **must** update
`CHANGELOG.md` under the `[Unreleased]` section in the same PR. Follow
[Keep a Changelog](https://keepachangelog.com/) format — use `### Added`,
`### Changed`, `### Deprecated`, `### Removed`, `### Fixed`, `### Security`.

What counts as user-visible:

- New features, new sinks, new sources, new env vars.
- Bug fixes that change observable behavior.
- Performance improvements with measurable impact.
- Breaking changes.
- Security fixes.

What does **not** count (skip the changelog):

- Pure internal refactors with no behavior change.
- Test additions.
- CI/build pipeline changes that don't affect what ships.
- This file and other contributor-facing docs.

### 4.5 Bump `Cargo.toml` if the change is user-visible

Releases are driven by `Cargo.toml`. If your PR ships a user-visible
change, bump `[package].version` in the same PR following Semantic
Versioning:

- **MAJOR**: breaking changes (config removed/renamed, trait method
  added, behavior change that breaks existing deployments).
- **MINOR**: new features (backward-compatible).
- **PATCH**: bug fixes, dependency updates, small improvements.

Then regenerate the lockfile:

```bash
cargo check
```

Commit the updated `Cargo.toml` and `Cargo.lock` together with your code
change.

**If you skip the bump on a user-visible change, the release workflow
will silently no-op on merge** and your change won't ship in a new
version until someone else bumps the file. Don't rely on a maintainer
catching this in review — bump it yourself.

### 4.6 Build a local dbmazz image

dbmazz ships a `Dockerfile.dev` designed for exactly this workflow. It
compiles dbmazz inside Docker and produces a runtime image with the
same entrypoint, ports, and healthcheck as the official image, so the
`ez-cdc` CLI treats it as a drop-in replacement.

```bash
docker build -f Dockerfile.dev -t my-dbmazz:dev .
```

### 4.7 Run the full test harness against your image

Point the `ez-cdc` CLI at your local image with `--dbmazz-image` (or
set `DBMAZZ_IMAGE`) and exercise the three commands that matter:

```bash
# Tell the CLI to use your image on every subcommand.
export DBMAZZ_IMAGE=my-dbmazz:dev

# Configure a source and a sink. The wizard walks you through it; the
# fastest option is --demo on the commands below, which spins up a
# self-contained source + sink stack alongside dbmazz.
ez-cdc datasource init      # first run only
ez-cdc datasource add       # interactive wizard (skip if using --demo)

# 1. Quickstart — launches dbmazz + live dashboard. Watch CDC replicate
#    INSERT/UPDATE/DELETE against your source and sink in real time.
ez-cdc quickstart --source <src> --sink <sink>
#    Or, with a self-contained demo stack (no datasources required):
ez-cdc quickstart --demo

# 2. Run — same as quickstart, but without the TUI. Good for tailing
#    logs while you reproduce a bug.
ez-cdc run --source <src> --sink <sink>

# 3. Verify — end-to-end test suite: schema, snapshot, CDC
#    INSERT/UPDATE/DELETE, TOAST handling, NULL roundtrip, idempotency.
#    This is the one that gates a PR.
ez-cdc verify --source <src> --sink <sink> --json-report verify.json
```

**All three must pass against at least one source + sink combination
before you open the PR.** If you touched a specific sink (e.g. the PG
sink), run `verify` against that sink specifically, and also against
any other sink whose code path your change might have affected.

Save the output — you'll attach it to the PR.

### 4.8 Commit

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <description>
```

Rules:

- `<type>` is one of `feat`, `fix`, `refactor`, `perf`, `docs`, `test`,
  `chore`, `security`.
- Description in **English, imperative, lowercase** after the type
  (`feat: add clickhouse sink`, not `feat: Added Clickhouse sink.`).
- First line **< 72 characters**. Longer explanation goes in the commit
  body after a blank line.
- **Do not use a scope** in parentheses — `feat: ...`, not
  `feat(pg-sink): ...`. The repo already indicates the component.
- **Do not add `Co-Authored-By` or "Generated with X" footers.** One
  author per commit.

Good:

```
feat: add per-table snapshot progress tracking
fix: handle StarRocks partial failure on Stream Load
security: strip passwords from sink config in API responses
```

Not good:

```
Added a feature          # no type
feat(pg): ...            # has scope
feat: Add X.             # capitalized, trailing period
feat: adds X             # wrong tense
```

### 4.9 Push and open the PR

```bash
git push -u origin <your-branch>
gh pr create --title "<type>: <description>" --body "..."
```

The PR body must include:

- **Summary** — what changed and why, linked to the triaging issue
  (`Closes #123`).
- **Test evidence** — at minimum, console output from `ez-cdc verify`
  or the JSON report. For features that change dashboard behavior,
  a screenshot. For perf claims, before/after numbers and the
  hardware.
- **Breaking-change call-out** — if this is a MAJOR bump, spell out
  exactly what breaks and how users upgrade.

### 4.10 Review

- Respond to review comments rather than force-pushing silently.
- Rebase on `main` if the PR falls behind.
- Never use `--no-verify` or `--force` unless a maintainer explicitly
  asks for it.
- Once approved, a maintainer merges. Tags and releases are cut from
  `main` by maintainers — contributors don't tag.

---

## 5. PR checklist

Copy this into your PR description and check each box:

- [ ] Issue opened and triaged before coding started (skip for typos/docs)
- [ ] `cargo fmt -- --check` passes
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo test` passes
- [ ] `CHANGELOG.md` updated under `[Unreleased]` (or N/A — note why)
- [ ] `Cargo.toml` version bumped per semver + `Cargo.lock` regenerated
      (or N/A — note why)
- [ ] Local image built with
      `docker build -f Dockerfile.dev -t my-dbmazz:dev .`
- [ ] `ez-cdc quickstart`, `ez-cdc run`, `ez-cdc verify` all pass
      against the local image (evidence attached)
- [ ] PR description links the original issue

---

## 6. Review rules (what reviewers look for)

These are patterns that have caused real bugs or are critical for data
integrity. Internalize them before touching the relevant code.

### Data integrity

- LSN tracking must be accurate. `confirmed_lsn` only advances **after**
  data is committed to the sink. Premature confirmation means data loss.
- Checkpoint is saved **before** confirming to PostgreSQL. Always. See
  `handle_checkpoint_feedback()` in the engine.
- Never drop a replication slot without confirming the consumer is done.
  A lost slot means a re-snapshot.
- Sink responses must be fully checked. StarRocks returns HTTP 200 even
  for partial failures.

### Pipeline & sink

- The pipeline only sees `CdcRecord` — never `CdcMessage` or pgoutput
  types. The PG↔generic conversion happens in `src/source/converter.rs`.
- Sink implementations are self-contained. Their internal loading
  strategy (raw table, staging, `COPY`, Stream Load) is not exposed via
  the trait.
- `write_batch()` handles both CDC and snapshot records — there is no
  separate snapshot path.

### PostgreSQL replication

- The WAL parser must handle all pgoutput message types. Unknown types
  → log and skip, never panic.
- Keepalive messages must be responded to promptly or the replication
  slot disconnects.
- Column type mappings must be exhaustive. An unmapped type is a clear
  error, never a silent data drop.

### Credential safety

- Never log connection strings, passwords, or auth tokens.
- Structs with sensitive fields must not derive `Debug` without
  redacting.

### Rust patterns

- Prefer `?` over `match` for error propagation. Use `.context()` from
  anyhow.
- `Arc<T>` for shared ownership. Prefer atomics over `RwLock` for
  counters/flags.
- `tokio::select!` with `biased;` when one branch is a cancellation
  signal.
- Hot-path serialization: byte-level ops, `Vec::with_capacity()`,
  `extend_from_slice`.

---

## 7. Adding a source or sink connector

Adding a new connector follows all the rules above, plus a
connector-specific API. See
[`docs/contributing-connectors.md`](docs/contributing-connectors.md)
for the trait surface, the required files per connector, and the
type-mapping tables.

---

## 8. License

By contributing, you agree your contribution is licensed under the
[Elastic License v2.0](LICENSE), the same license as the project.
Contributions cannot revoke the license once accepted.
