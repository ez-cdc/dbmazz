# Git Hooks

This directory contains shared git hooks for the repository.

## Setup (one-time)

```bash
git config core.hooksPath .githooks
```

## Hooks

### pre-push

Blocks direct pushes to `main`. All changes must go through a feature branch + PR.

- Pushing to `main` → **BLOCKED**
- Pushing a feature branch → allowed
- Pushing tags (`v*`) → allowed
- Emergency bypass: `git push --no-verify` (requires explicit justification)
