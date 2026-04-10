"""Path resolution for the e2e harness.

The CLI can be invoked from anywhere in the filesystem (via the ez-cdc
console script), so we need a reliable way to locate the repo root and
the fixtures directory.

Strategy: the package is installed in editable mode (`pip install -e e2e/`),
so __file__ always points inside <repo>/e2e/src/ez_cdc_e2e/. We walk up
from there to find the e2e dir and the repo root.
"""

from __future__ import annotations

from pathlib import Path


# ez_cdc_e2e/paths.py → .../e2e/src/ez_cdc_e2e/paths.py
_PACKAGE_FILE = Path(__file__).resolve()

# .../e2e/src/ez_cdc_e2e
PACKAGE_DIR = _PACKAGE_FILE.parent

# .../e2e/src
SRC_DIR = PACKAGE_DIR.parent

# .../e2e
E2E_DIR = SRC_DIR.parent

# .../ (repo root; dbmazz)
REPO_ROOT = E2E_DIR.parent


# ── Frequently-used paths ────────────────────────────────────────────────────

FIXTURES_DIR = E2E_DIR / "fixtures"

POSTGRES_SEED_SQL = FIXTURES_DIR / "postgres-seed.sql"
TYPES_FIXTURE_SQL = FIXTURES_DIR / "types.sql"  # created in PR 2
