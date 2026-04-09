"""Profile registry: maps sink names to their compose profile, backend class,
and metadata.

A "profile" is the unit the user selects: `starrocks`, `pg-target`,
`snowflake`. Each profile ties together:

  - the compose profile name (what to pass to `docker compose --profile`)
  - the source database DSN (reachable from the host during test runs)
  - the target backend class (the TargetBackend implementation)
  - the dbmazz daemon HTTP URL (for /status, /healthz, etc.)
  - a user-facing description
  - an optional env file (Snowflake needs .env.snowflake)

Adding a new sink requires adding an entry here + a backend class +
a compose service. See e2e/README.md for the full checklist.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from .paths import ENV_SNOWFLAKE_FILE


@dataclass(frozen=True)
class ProfileSpec:
    """Static description of a test profile (sink)."""
    name: str                       # user-facing name: "starrocks", "pg-target", "snowflake"
    compose_profile: str            # docker compose --profile value
    description: str                # short description for menus and help
    source_dsn: str                 # source PG connection, from host
    dbmazz_http_url: str            # dbmazz daemon /status endpoint base URL
    tables: tuple[str, ...]         # tables replicated by this profile
    backend_import: str             # dotted path: "ez_cdc_e2e.backends.postgres:PostgresTarget"
    requires_env_file: Optional[Path] = None  # e.g., Snowflake needs .env.snowflake


# ── Profile definitions ──────────────────────────────────────────────────────

_COMMON_SOURCE_DSN = "postgres://postgres:postgres@localhost:15432/dbmazz"
_COMMON_DBMAZZ_URL = "http://localhost:8080"
_COMMON_TABLES = ("orders", "order_items")


PROFILES: dict[str, ProfileSpec] = {
    "starrocks": ProfileSpec(
        name="starrocks",
        compose_profile="starrocks",
        description="PostgreSQL → StarRocks (batteries included)",
        source_dsn=_COMMON_SOURCE_DSN,
        dbmazz_http_url=_COMMON_DBMAZZ_URL,
        tables=_COMMON_TABLES,
        backend_import="ez_cdc_e2e.backends.starrocks:StarRocksTarget",
    ),
    "pg-target": ProfileSpec(
        name="pg-target",
        compose_profile="pg-target",
        description="PostgreSQL → PostgreSQL",
        source_dsn=_COMMON_SOURCE_DSN,
        dbmazz_http_url=_COMMON_DBMAZZ_URL,
        tables=_COMMON_TABLES,
        backend_import="ez_cdc_e2e.backends.postgres:PostgresTarget",
    ),
    "snowflake": ProfileSpec(
        name="snowflake",
        compose_profile="snowflake",
        description="PostgreSQL → Snowflake (requires credentials)",
        source_dsn=_COMMON_SOURCE_DSN,
        dbmazz_http_url=_COMMON_DBMAZZ_URL,
        tables=_COMMON_TABLES,
        backend_import="ez_cdc_e2e.backends.snowflake:SnowflakeTarget",
        requires_env_file=ENV_SNOWFLAKE_FILE,
    ),
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_profile(name: str) -> ProfileSpec:
    """Return the ProfileSpec for `name`, or raise KeyError with a friendly message."""
    if name not in PROFILES:
        available = ", ".join(sorted(PROFILES.keys()))
        raise KeyError(
            f"Unknown profile '{name}'. Available profiles: {available}"
        )
    return PROFILES[name]


def list_profiles() -> list[ProfileSpec]:
    """Return all profiles in a stable order."""
    # Stable order: starrocks first (default), pg-target, snowflake last (needs creds).
    order = ["starrocks", "pg-target", "snowflake"]
    return [PROFILES[name] for name in order if name in PROFILES]


def list_runnable_profiles() -> list[ProfileSpec]:
    """Return profiles that can actually run right now.

    Profiles that require an env file (like snowflake) are excluded if the
    env file doesn't exist. Used by `ez-cdc verify --all` to auto-detect
    which sinks to include.
    """
    return [
        p for p in list_profiles()
        if p.requires_env_file is None or p.requires_env_file.exists()
    ]


def load_backend_class(spec: ProfileSpec) -> Callable:
    """Lazily import and return the TargetBackend class for a profile.

    We use lazy imports so that `ez-cdc --help` doesn't pay the cost of
    importing snowflake-connector-python etc. just to show help text.
    """
    module_path, class_name = spec.backend_import.split(":")
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
