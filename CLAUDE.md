# CLAUDE.md — dbmazz

Guidance for working on the dbmazz codebase.

## Project Overview

dbmazz is a Rust-based CDC (Change Data Capture) daemon. It reads from PostgreSQL via logical replication and streams changes to a configured sink. One instance per replication job.

## Git Rules

- **NEVER** add `Co-Authored-By` to commits. The only author is the user.
- **NEVER** add "Generated with Claude Code" or similar footers to PRs, commits, or code.
- Don't use `--no-verify` or `--force` without explicit authorization.
- **NEVER** commit or push directly to `main`. All changes go through a feature branch + PR.

### Branching Workflow

```
1. git checkout -b <type>/<short-description>   # Create branch from main
2. (make changes, commits)
3. git push -u origin <branch>
4. gh pr create --title "..." --body "..."
```

Branch names: `<type>/<short-description>` where type is `feat|fix|refactor|chore|docs|perf`.

Examples: `feat/new-sink`, `fix/handle-timeout`, `chore/update-deps`.

### Conventional Commits

All commits MUST use:

```
<type>: <description>
```

| Type | When to use |
|------|------------|
| `feat` | New functionality |
| `fix` | Bug fix |
| `refactor` | Restructuring without behavior change |
| `perf` | Performance improvement |
| `docs` | Documentation |
| `test` | Adding or modifying tests |
| `chore` | Maintenance (deps, CI, cleanup) |
| `security` | Security fix |

Rules:
- Description in English, imperative, lowercase after the type
- First line < 72 characters
- Extra context goes in the body
- Don't use scope in parentheses

## Build & Test

```bash
cargo build --release
cargo test
cargo fmt -- --check
cargo clippy -- -D warnings
```

Requires Rust 1.91.1+.

## Code Conventions

- **Format**: always run `cargo fmt` before committing
- **Lint**: `cargo clippy -- -D warnings` with no warnings
- **Naming**: `snake_case` for functions/modules, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants
- **Errors**: prefer `?` with `anyhow::Context` over `match`. No `unwrap()` in production code paths.
- **Logging**: use `tracing`. Never `println!` or `eprintln!`.
- **Async**: use tokio. Never block the runtime — use `spawn_blocking` for CPU-heavy work.
- **Credentials**: NEVER log connection strings, passwords, or auth tokens. Structs with sensitive fields MUST NOT derive `Debug` without redacting.

## Versioning

dbmazz uses Semantic Versioning: `vMAJOR.MINOR.PATCH`. Tags only on `main`.

- **MAJOR**: breaking changes
- **MINOR**: new features (backward-compatible)
- **PATCH**: bug fixes, dependency updates

## Changelog Discipline

Every change that affects user-visible behavior MUST update `CHANGELOG.md` under the `[Unreleased]` section in the same PR. Follow [Keep a Changelog](https://keepachangelog.com/) format.

What counts as user-visible:
- New features, new sinks, new sources, new env vars
- Bug fixes that change observable behavior
- Performance improvements with measurable impact
- Breaking changes
- Security fixes

What does NOT count:
- Internal refactors with no behavior change
- Test additions
- README/docs reorganisation
- CI/build pipeline changes that don't affect what gets shipped

## License

dbmazz is licensed under the Elastic License v2.0. Contributions are accepted under the same license.
