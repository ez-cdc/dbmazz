"""Programmatic docker-compose YAML generation for a (source, sink) pair.

Replaces the static `e2e/compose.yml` with a dynamically generated file
per source/sink combination. This unlocks the BYOD matrix:

  managed source + managed sink   → both containers + dbmazz
  managed source + user sink      → source container + dbmazz with env vars
  user source    + managed sink   → sink container + dbmazz with env vars
  user source    + user sink      → only dbmazz container (pure BYOD)

Files written per pair (under `e2e/.cache/compose/<source>__<sink>__<hash>/`):

  compose.yml   → docker compose definition for this pair
  .env          → SINK_TYPE / SOURCE_URL / etc. for the dbmazz service

The cache directory is keyed by both names plus a short hash so that
repeating the same pair always lands in the same dir (idempotent), and
distinct pairs never collide. `down` finds the same dir to tear down.

Path strategy:
  Volume mounts use absolute paths (resolved from e2e/fixtures/) so the
  compose file is portable to any working directory and the user doesn't
  have to think about relative paths.

This module replaces the role of `profiles.py`'s static profile registry
in PR 1 — that file is removed in chunk 9 (PR4-9).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

import yaml

from .datasources.schema import (
    PostgresSinkSpec,
    PostgresSourceSpec,
    SinkSpec,
    SnowflakeSinkSpec,
    SourceSpec,
    StarRocksSinkSpec,
)
from .paths import E2E_DIR, FIXTURES_DIR


# ── Cache layout ─────────────────────────────────────────────────────────────

CACHE_ROOT = E2E_DIR / ".cache" / "compose"


def cache_dir_for(source_name: str, sink_name: str) -> Path:
    """Return the cache directory for a (source_name, sink_name) pair.

    The directory name encodes both names plus a short SHA256 prefix of
    the combined names — this avoids filesystem collisions if two
    different pairs happen to share names with hyphens, and keeps the
    directory grep-friendly.
    """
    key = f"{source_name}|{sink_name}".encode("utf-8")
    digest = hashlib.sha256(key).hexdigest()[:8]
    return CACHE_ROOT / f"{source_name}__{sink_name}__{digest}"


# ── Top-level builder ────────────────────────────────────────────────────────

def build_compose_for_pair(
    source_name: str,
    source: SourceSpec,
    sink_name: str,
    sink: SinkSpec,
    *,
    project_name: Optional[str] = None,
) -> tuple[Path, Path]:
    """Generate compose.yml + .env for a (source, sink) pair.

    The compose file is written to a stable cache directory keyed by both
    names. Re-running with the same pair overwrites the previous output
    (so config changes take effect).

    Args:
        source_name: the user-facing name of the source datasource.
        source: the SourceSpec.
        sink_name: the user-facing name of the sink datasource.
        sink: the SinkSpec.
        project_name: optional docker compose project name. Defaults to
            "ez-cdc-{source_name}-{sink_name}".

    Returns:
        (compose_path, env_path): paths to the generated files. Pass
        compose_path to `docker compose -f <path> up`, and pass env_path
        as the env_file (already wired up inside the compose).
    """
    cache = cache_dir_for(source_name, sink_name)
    cache.mkdir(parents=True, exist_ok=True)

    if project_name is None:
        # Use a deterministic project name so docker compose ps/down/logs
        # can find the right containers across invocations.
        project_name = f"ez-cdc-{source_name}-{sink_name}".replace("_", "-")[:50]

    # Validate the (source, sink) combination.
    _validate_pair(source, sink)

    services: dict[str, dict] = {}
    volumes: dict[str, dict] = {}

    # ── Source service (only when managed) ──
    if source.managed and isinstance(source, PostgresSourceSpec):
        services["source-pg"] = _build_managed_pg_source_service(source)
        volumes["source_pg_data"] = {}

    # ── Sink service (only when managed) ──
    if sink.managed:
        if isinstance(sink, StarRocksSinkSpec):
            services["sink-starrocks"] = _build_managed_starrocks_service()
            services["sink-starrocks-init"] = _build_starrocks_init_service()
            volumes["sink_starrocks_data"] = {}
        elif isinstance(sink, PostgresSinkSpec):
            services["sink-pg"] = _build_managed_pg_sink_service()
            volumes["sink_pg_data"] = {}

    # ── dbmazz service (always present) ──
    services["dbmazz"] = _build_dbmazz_service(source, sink)

    # ── Network ──
    networks = {"dbmazz": {"driver": "bridge"}}

    compose_dict = {
        "name": project_name,
        "services": services,
        "networks": networks,
    }
    if volumes:
        compose_dict["volumes"] = volumes

    # Write env file referenced by the dbmazz service.
    env_path = cache / ".env"
    env_content = _build_env_file(source, sink)
    env_path.write_text(env_content)

    # Write compose.yml.
    compose_path = cache / "compose.yml"
    compose_yaml = (
        "# ez-cdc generated docker compose for source/sink pair\n"
        f"# source: {source_name}\n"
        f"# sink:   {sink_name}\n"
        "# Generated by compose_builder.py — do not edit by hand.\n"
        "# Re-run `ez-cdc up` to regenerate after changing the datasource spec.\n\n"
        + yaml.safe_dump(compose_dict, sort_keys=False, default_flow_style=False)
    )
    compose_path.write_text(compose_yaml)

    return compose_path, env_path


# ── Pair validation ──────────────────────────────────────────────────────────

def _validate_pair(source: SourceSpec, sink: SinkSpec) -> None:
    """Sanity-check the source/sink combination before generating anything.

    Catches mistakes like "managed Snowflake" (impossible per the schema,
    but defensive) and any future cases where two specific datasource
    types can't actually replicate to each other.
    """
    if isinstance(source, PostgresSourceSpec):
        return  # all sinks accept a PG source
    raise ValueError(f"unsupported source spec type: {type(source).__name__}")


# ── Service builders: source ─────────────────────────────────────────────────

_DBMAZZ_DOCKERFILE_PATH = "e2e/Dockerfile"  # relative to repo root (build context)


def _build_managed_pg_source_service(source: PostgresSourceSpec) -> dict:
    """Build the docker compose service for a managed PostgreSQL source.

    Mounts the seed SQL file as a /docker-entrypoint-initdb.d entry so the
    schema is created on first start. Uses logical replication settings
    so dbmazz can subscribe.
    """
    seed_path = FIXTURES_DIR / (source.seed or "postgres-seed.sql")
    if not seed_path.exists():
        raise FileNotFoundError(
            f"seed file not found: {seed_path}. "
            f"Either create it under e2e/fixtures/ or change the source's `seed:` field."
        )

    return {
        "image": "postgres:16",
        "environment": {
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "dbmazz",
        },
        "ports": ["15432:5432"],
        "volumes": [
            f"{seed_path.resolve()}:/docker-entrypoint-initdb.d/01-init.sql:ro",
            "source_pg_data:/var/lib/postgresql/data",
        ],
        "command": [
            "postgres",
            "-c", "wal_level=logical",
            "-c", "max_replication_slots=10",
            "-c", "max_wal_senders=10",
        ],
        "healthcheck": {
            "test": ["CMD-SHELL", "pg_isready -U postgres"],
            "interval": "2s",
            "timeout": "5s",
            "retries": 30,
        },
        "networks": ["dbmazz"],
    }


# ── Service builders: sink ───────────────────────────────────────────────────

def _build_managed_starrocks_service() -> dict:
    """StarRocks all-in-one container with healthcheck."""
    return {
        "image": "starrocks/allin1-ubuntu:latest",
        "ports": ["8030:8030", "9030:9030"],
        "volumes": ["sink_starrocks_data:/data"],
        "healthcheck": {
            "test": [
                "CMD-SHELL",
                "mysql -h 127.0.0.1 -P 9030 -u root -e 'SELECT 1' 2>/dev/null || exit 1",
            ],
            "interval": "5s",
            "timeout": "10s",
            "retries": 60,
            "start_period": "60s",
        },
        "networks": ["dbmazz"],
    }


def _build_starrocks_init_service() -> dict:
    """One-shot init container that creates the dbmazz database in StarRocks."""
    init_script = FIXTURES_DIR / "starrocks-init.sh"
    if not init_script.exists():
        raise FileNotFoundError(
            f"starrocks init script not found: {init_script}"
        )
    return {
        "image": "mysql:8.0",
        "volumes": [f"{init_script.resolve()}:/init.sh:ro"],
        "entrypoint": ["/bin/sh", "/init.sh"],
        "environment": {
            "SR_HOST": "sink-starrocks",
            "SR_PORT": "9030",
            "SR_DATABASE": "dbmazz",
        },
        "depends_on": {
            "sink-starrocks": {"condition": "service_healthy"},
        },
        "networks": ["dbmazz"],
    }


def _build_managed_pg_sink_service() -> dict:
    """Empty PostgreSQL target."""
    return {
        "image": "postgres:16",
        "environment": {
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "dbmazz_target",
        },
        "ports": ["25432:5432"],
        "volumes": ["sink_pg_data:/var/lib/postgresql/data"],
        "healthcheck": {
            "test": ["CMD-SHELL", "pg_isready -U postgres"],
            "interval": "2s",
            "timeout": "5s",
            "retries": 30,
        },
        "networks": ["dbmazz"],
    }


# ── Service builders: dbmazz ─────────────────────────────────────────────────

def _build_dbmazz_service(source: SourceSpec, sink: SinkSpec) -> dict:
    """Build the dbmazz container service.

    Uses build context = repo root (so the Dockerfile in e2e/Dockerfile
    can COPY the Rust source). The .env file in the cache dir provides
    SOURCE_URL / SINK_TYPE / etc.

    `depends_on` is computed dynamically based on which other services
    are present (managed source/sink). For pure BYOD (both user-managed),
    no depends_on entries are added — dbmazz is the only service.
    """
    repo_root = E2E_DIR.parent

    depends_on: dict = {}
    if source.managed:
        depends_on["source-pg"] = {"condition": "service_healthy"}
    if sink.managed:
        if isinstance(sink, StarRocksSinkSpec):
            depends_on["sink-starrocks-init"] = {"condition": "service_completed_successfully"}
        elif isinstance(sink, PostgresSinkSpec):
            depends_on["sink-pg"] = {"condition": "service_healthy"}

    service = {
        "build": {
            "context": str(repo_root.resolve()),
            "dockerfile": _DBMAZZ_DOCKERFILE_PATH,
            "args": {"CARGO_FEATURES": "http-api"},
        },
        "ports": ["8080:8080", "50051:50051"],
        "env_file": [".env"],  # relative to compose.yml in the cache dir
        "restart": "unless-stopped",
        "healthcheck": {
            "test": ["CMD", "curl", "-f", "http://localhost:8080/healthz"],
            "interval": "10s",
            "timeout": "5s",
            "retries": 3,
            "start_period": "30s",
        },
        "networks": ["dbmazz"],
    }
    if depends_on:
        service["depends_on"] = depends_on

    return service


# ── Env file generation ──────────────────────────────────────────────────────

def _build_env_file(source: SourceSpec, sink: SinkSpec) -> str:
    """Generate the .env file content for the dbmazz container.

    Translates the SourceSpec / SinkSpec into the SOURCE_URL / SINK_TYPE /
    SINK_URL etc. environment variables that the dbmazz daemon expects.
    """
    lines = [
        "# Generated by ez-cdc compose_builder.py — do not edit by hand",
        "",
    ]

    # ── Source vars ──
    if isinstance(source, PostgresSourceSpec):
        if source.managed:
            # When the source is managed, dbmazz connects to the source-pg
            # container by its docker network name (not localhost!).
            source_url = "postgres://postgres:postgres@source-pg:5432/dbmazz?replication=database"
        else:
            # User-provided URL — append ?replication=database if missing.
            source_url = source.url or ""
            if "replication=database" not in source_url:
                sep = "&" if "?" in source_url else "?"
                source_url = f"{source_url}{sep}replication=database"

        lines += [
            f"SOURCE_TYPE=postgres",
            f"SOURCE_URL={source_url}",
            f"SOURCE_SLOT_NAME={source.replication_slot}",
            f"SOURCE_PUBLICATION_NAME={source.publication}",
            f"TABLES={','.join(source.tables)}",
            "",
        ]

    # ── Sink vars ──
    if isinstance(sink, PostgresSinkSpec):
        if sink.managed:
            sink_url = "postgres://postgres:postgres@sink-pg:5432/dbmazz_target"
            sink_database = "dbmazz_target"
            sink_schema = "public"
        else:
            sink_url = sink.url or ""
            sink_database = sink.database or ""
            sink_schema = sink.schema_

        lines += [
            "SINK_TYPE=postgres",
            f"SINK_URL={sink_url}",
            f"SINK_DATABASE={sink_database}",
            f"SINK_SCHEMA={sink_schema}",
        ]

    elif isinstance(sink, StarRocksSinkSpec):
        if sink.managed:
            sink_url = "http://sink-starrocks:8030"
            sink_database = "dbmazz"
            sink_user = "root"
            sink_password = ""
        else:
            sink_url = sink.url or ""
            sink_database = sink.database or ""
            sink_user = sink.user
            sink_password = sink.password

        lines += [
            "SINK_TYPE=starrocks",
            f"SINK_URL={sink_url}",
            f"SINK_PORT={sink.mysql_port}",
            f"SINK_DATABASE={sink_database}",
            f"SINK_USER={sink_user}",
            f"SINK_PASSWORD={sink_password}",
        ]

    elif isinstance(sink, SnowflakeSinkSpec):
        # Snowflake is always user-managed (cloud).
        lines += [
            "SINK_TYPE=snowflake",
            f"SINK_SNOWFLAKE_ACCOUNT={sink.account}",
            f"SINK_USER={sink.user}",
            f"SINK_PASSWORD={sink.password}",
            f"SINK_DATABASE={sink.database}",
            f"SINK_SCHEMA={sink.schema_}",
            f"SINK_SNOWFLAKE_WAREHOUSE={sink.warehouse}",
            f"SINK_SNOWFLAKE_SOFT_DELETE={'true' if sink.soft_delete else 'false'}",
        ]
        if sink.role:
            lines.append(f"SINK_SNOWFLAKE_ROLE={sink.role}")
        if sink.private_key_path:
            lines.append(f"SINK_SNOWFLAKE_PRIVATE_KEY_PATH={sink.private_key_path}")

    lines += [
        "",
        "# Pipeline tuning",
        "FLUSH_SIZE=2000",
        "FLUSH_INTERVAL_MS=2000",
        "",
        "# Snapshot",
        "DO_SNAPSHOT=true",
        "SNAPSHOT_CHUNK_SIZE=10000",
        "SNAPSHOT_PARALLEL_WORKERS=2",
        "",
        "# Ports + logging",
        "HTTP_API_PORT=8080",
        "GRPC_PORT=50051",
        "RUST_LOG=info",
        "",
    ]

    return "\n".join(lines)
