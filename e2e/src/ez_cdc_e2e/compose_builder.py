"""Programmatic docker-compose YAML generation for a (source, sink) pair.

Replaces the static `e2e/compose.yml` with a dynamically generated file
per source/sink combination. The compose builder infers which Docker
containers to create based on well-known localhost ports in spec URLs:

  localhost:15432 source → source-pg container + dbmazz
  localhost:18030 sink   → sink-starrocks container + dbmazz
  localhost:25432 sink   → sink-pg container + dbmazz
  remote URLs            → only dbmazz container (pure BYOD)

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
    PipelineSettings,
    PostgresSinkSpec,
    PostgresSourceSpec,
    SinkSpec,
    SnowflakeSinkSpec,
    SourceSpec,
    StarRocksSinkSpec,
)
from .paths import E2E_DIR, FIXTURES_DIR


# ── Well-known localhost ports for Docker-managed datasources ────────────────
# The compose builder uses these to decide which containers to create
# and how to rewrite URLs for the docker-internal network.

_SOURCE_PG_EXT_PORT = 15432
_SOURCE_PG_INT_PORT = 5432
_SOURCE_PG_SERVICE = "source-pg"

_SINK_PG_EXT_PORT = 25432
_SINK_PG_INT_PORT = 5432
_SINK_PG_SERVICE = "sink-pg"

_SINK_SR_HTTP_EXT_PORT = 18030
_SINK_SR_HTTP_INT_PORT = 8030
_SINK_SR_MYSQL_EXT_PORT = 19030
_SINK_SR_MYSQL_INT_PORT = 9030
_SINK_SR_SERVICE = "sink-starrocks"


# ── URL helpers ──────────────────────────────────────────────────────────────

def _extract_port(url: str) -> int | None:
    """Extract port from a URL. Returns None if no port found."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        return parsed.port
    except (ValueError, AttributeError):
        return None


def _rewrite_url_for_docker(url: str, ext_port: int, int_port: int, service: str) -> str:
    """Rewrite a localhost URL to use the docker-internal service name and port."""
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(url)
    if parsed.hostname in ("localhost", "127.0.0.1") and parsed.port == ext_port:
        new_netloc = f"{parsed.username}:{parsed.password}@{service}:{int_port}" if parsed.username else f"{service}:{int_port}"
        return urlunparse((parsed.scheme, new_netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return url


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

_INFRA_PROJECT_NAME = "ez-cdc-infra"
_INFRA_NETWORK = f"{_INFRA_PROJECT_NAME}_dbmazz"


def build_compose_for_pair(
    source_name: str,
    source: SourceSpec,
    sink_name: str,
    sink: SinkSpec,
    *,
    settings: PipelineSettings | None = None,
    project_name: Optional[str] = None,
) -> tuple[Path, Path]:
    """Generate compose.yml + .env for a (source, sink) pair.

    The compose contains **only the dbmazz service**, connected to the
    infra network created by ``build_infra_compose()``.  Infra containers
    (source DB, sink DB) are NOT included — they are managed separately
    by ``ez-cdc up`` via the global infra compose.

    Returns:
        (compose_path, env_path): paths to the generated files.
    """
    cache = cache_dir_for(source_name, sink_name)
    cache.mkdir(parents=True, exist_ok=True)

    if project_name is None:
        project_name = f"ez-cdc-{source_name}-{sink_name}".replace("_", "-")[:50]

    _validate_pair(source, sink)

    # dbmazz service connected to the external infra network.
    dbmazz_service = _build_dbmazz_service(source, sink)
    # Override the network to use the infra compose's network.
    dbmazz_service["networks"] = [_INFRA_NETWORK]
    # Remove depends_on — infra services live in a different compose.
    dbmazz_service.pop("depends_on", None)

    compose_dict = {
        "name": project_name,
        "services": {"dbmazz": dbmazz_service},
        "networks": {
            _INFRA_NETWORK: {"external": True},
        },
    }

    # Write env file referenced by the dbmazz service.
    env_path = cache / ".env"
    if settings is None:
        settings = PipelineSettings()
    env_content = _build_env_file(source, sink, settings)
    env_path.write_text(env_content)

    # Write compose.yml.
    compose_path = cache / "compose.yml"
    compose_yaml = (
        "# ez-cdc generated docker compose — dbmazz only (infra is separate)\n"
        f"# source: {source_name}\n"
        f"# sink:   {sink_name}\n"
        "# Generated by compose_builder.py — do not edit by hand.\n\n"
        + yaml.safe_dump(compose_dict, sort_keys=False, default_flow_style=False)
    )
    compose_path.write_text(compose_yaml)

    return compose_path, env_path


# ── Global infra compose (all datasources at once) ─────────────────────────

INFRA_CACHE_DIR = CACHE_ROOT / "_infra"


def infra_compose_path() -> Path:
    """Return the path to the global infra compose.yml."""
    return INFRA_CACHE_DIR / "compose.yml"


def build_infra_compose(
    sources: dict[str, SourceSpec],
    sinks: dict[str, SinkSpec],
    *,
    project_name: str = "ez-cdc-infra",
) -> Path:
    """Generate a single compose.yml with ALL infra containers.

    Scans all sources and sinks. For each one that uses a well-known
    localhost port, adds the corresponding Docker service. Deduplicates
    automatically (e.g., two pairs sharing the same source PG at :15432
    only get one source-pg container).

    Does NOT include the dbmazz service — that's added per-pair by
    quickstart/verify via build_compose_for_pair().

    Returns the path to the generated compose.yml.
    """
    INFRA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    services: dict[str, dict] = {}
    volumes: dict[str, dict] = {}

    # ── Source services ──
    # Find the first PostgresSourceSpec with a seed field to use for the
    # source-pg container's init script.  If none has a seed, the container
    # is created without a seed mount.
    seed_source: PostgresSourceSpec | None = None
    for source in sources.values():
        if isinstance(source, PostgresSourceSpec) and _extract_port(source.url) == _SOURCE_PG_EXT_PORT:
            if seed_source is None and hasattr(source, "seed") and source.seed:
                seed_source = source
            # Deduplicate: only add the service once
            if "source-pg" not in services:
                src_to_use = seed_source if seed_source is not None else source
                services["source-pg"] = _build_managed_pg_source_service(src_to_use)
                volumes["source_pg_data"] = {}

    # If we found a seed source after already adding the service without one,
    # rebuild with the seed source.
    if seed_source is not None and "source-pg" in services:
        services["source-pg"] = _build_managed_pg_source_service(seed_source)

    # ── Sink services ──
    for sink in sinks.values():
        if isinstance(sink, StarRocksSinkSpec) and _extract_port(sink.url) == _SINK_SR_HTTP_EXT_PORT:
            if "sink-starrocks" not in services:
                services["sink-starrocks"] = _build_managed_starrocks_service()
                services["sink-starrocks-init"] = _build_starrocks_init_service()
                volumes["sink_starrocks_data"] = {}
        elif isinstance(sink, PostgresSinkSpec) and _extract_port(sink.url) == _SINK_PG_EXT_PORT:
            if "sink-pg" not in services:
                services["sink-pg"] = _build_managed_pg_sink_service()
                volumes["sink_pg_data"] = {}
        # SnowflakeSinkSpec → skip (no Docker container)

    # ── Network ──
    networks = {"dbmazz": {"driver": "bridge"}}

    compose_dict: dict = {
        "name": project_name,
        "services": services,
        "networks": networks,
    }
    if volumes:
        compose_dict["volumes"] = volumes

    # ── Write compose.yml ──
    compose_path = INFRA_CACHE_DIR / "compose.yml"
    compose_yaml = (
        "# ez-cdc generated infra compose — ALL datasource containers\n"
        "# Generated by compose_builder.py — do not edit by hand.\n"
        "# Re-run `ez-cdc up` to regenerate after changing datasource specs.\n\n"
        + yaml.safe_dump(compose_dict, sort_keys=False, default_flow_style=False)
    )
    compose_path.write_text(compose_yaml)

    return compose_path


def list_infra_services(
    sources: dict[str, SourceSpec],
    sinks: dict[str, SinkSpec],
) -> list[str]:
    """Return the service names that would be created for these datasources.

    Useful for the CLI to display what ``ez-cdc up`` is about to start.
    The list is sorted alphabetically for stable output.
    """
    names: set[str] = set()

    for source in sources.values():
        if isinstance(source, PostgresSourceSpec) and _extract_port(source.url) == _SOURCE_PG_EXT_PORT:
            names.add("source-pg")

    for sink in sinks.values():
        if isinstance(sink, StarRocksSinkSpec) and _extract_port(sink.url) == _SINK_SR_HTTP_EXT_PORT:
            names.add("sink-starrocks")
            names.add("sink-starrocks-init")
        elif isinstance(sink, PostgresSinkSpec) and _extract_port(sink.url) == _SINK_PG_EXT_PORT:
            names.add("sink-pg")

    return sorted(names)


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
    """Build the docker compose service for a Docker-managed PostgreSQL source.

    If the source spec has a ``seed`` field, mounts the SQL file as a
    /docker-entrypoint-initdb.d entry so the schema is created on first
    start. Uses logical replication settings so dbmazz can subscribe.
    """
    service: dict = {
        "image": "postgres:16",
        "environment": {
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "dbmazz",
        },
        "ports": [f"{_SOURCE_PG_EXT_PORT}:{_SOURCE_PG_INT_PORT}"],
        "volumes": [
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

    if hasattr(source, "seed") and source.seed:
        seed_path = FIXTURES_DIR / source.seed
        if not seed_path.exists():
            raise FileNotFoundError(
                f"seed file not found: {seed_path}. "
                f"Either create it under e2e/fixtures/ or change the source's `seed:` field."
            )
        service["volumes"].insert(
            0, f"{seed_path.resolve()}:/docker-entrypoint-initdb.d/01-init.sql:ro"
        )

    return service


# ── Service builders: sink ───────────────────────────────────────────────────

def _build_managed_starrocks_service() -> dict:
    """StarRocks all-in-one container with healthcheck."""
    return {
        "image": "starrocks/allin1-ubuntu:latest",
        "ports": [
            f"{_SINK_SR_HTTP_EXT_PORT}:{_SINK_SR_HTTP_INT_PORT}",
            f"{_SINK_SR_MYSQL_EXT_PORT}:{_SINK_SR_MYSQL_INT_PORT}",
        ],
        "volumes": ["sink_starrocks_data:/data"],
        "healthcheck": {
            "test": [
                "CMD-SHELL",
                f"mysql -h 127.0.0.1 -P {_SINK_SR_MYSQL_INT_PORT} -u root -e 'SELECT 1' 2>/dev/null || exit 1",
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
            "SR_HOST": _SINK_SR_SERVICE,
            "SR_PORT": str(_SINK_SR_MYSQL_INT_PORT),
            "SR_DATABASE": "dbmazz",
        },
        "depends_on": {
            _SINK_SR_SERVICE: {"condition": "service_healthy"},
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
        "ports": [f"{_SINK_PG_EXT_PORT}:{_SINK_PG_INT_PORT}"],
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
    are present (inferred from well-known localhost ports). For pure BYOD
    (all remote URLs), no depends_on entries are added — dbmazz is the
    only service.
    """
    repo_root = E2E_DIR.parent

    depends_on: dict = {}
    if isinstance(source, PostgresSourceSpec) and _extract_port(source.url) == _SOURCE_PG_EXT_PORT:
        depends_on["source-pg"] = {"condition": "service_healthy"}
    if isinstance(sink, StarRocksSinkSpec) and _extract_port(sink.url) == _SINK_SR_HTTP_EXT_PORT:
        depends_on["sink-starrocks-init"] = {"condition": "service_completed_successfully"}
    elif isinstance(sink, PostgresSinkSpec) and _extract_port(sink.url) == _SINK_PG_EXT_PORT:
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

def _build_env_file(source: SourceSpec, sink: SinkSpec, settings: PipelineSettings) -> str:
    """Generate the .env file content for the dbmazz container.

    Translates the SourceSpec / SinkSpec + PipelineSettings into the
    environment variables that the dbmazz daemon expects.
    """
    lines = [
        "# Generated by ez-cdc compose_builder.py — do not edit by hand",
        "",
    ]

    # ── Source vars ──
    if isinstance(source, PostgresSourceSpec):
        # Rewrite localhost URL to docker-internal service name if needed.
        source_url = _rewrite_url_for_docker(
            source.url, _SOURCE_PG_EXT_PORT, _SOURCE_PG_INT_PORT, _SOURCE_PG_SERVICE,
        )
        # Append ?replication=database if missing.
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
        sink_url = _rewrite_url_for_docker(
            sink.url, _SINK_PG_EXT_PORT, _SINK_PG_INT_PORT, _SINK_PG_SERVICE,
        )
        lines += [
            "SINK_TYPE=postgres",
            f"SINK_URL={sink_url}",
            f"SINK_DATABASE={sink.database}",
            f"SINK_SCHEMA={sink.schema_}",
        ]

    elif isinstance(sink, StarRocksSinkSpec):
        sink_url = _rewrite_url_for_docker(
            sink.url, _SINK_SR_HTTP_EXT_PORT, _SINK_SR_HTTP_INT_PORT, _SINK_SR_SERVICE,
        )
        # Rewrite MySQL port for docker-internal if the HTTP URL was rewritten.
        mysql_port = sink.mysql_port
        if sink_url != sink.url:
            # HTTP URL was rewritten → we're in docker; use internal MySQL port.
            mysql_port = _SINK_SR_MYSQL_INT_PORT
        lines += [
            "SINK_TYPE=starrocks",
            f"SINK_URL={sink_url}",
            f"SINK_PORT={mysql_port}",
            f"SINK_DATABASE={sink.database}",
            f"SINK_USER={sink.user}",
            f"SINK_PASSWORD={sink.password}",
        ]

    elif isinstance(sink, SnowflakeSinkSpec):
        # Snowflake: no URL rewriting (cloud service, no Docker container).
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
        "# Pipeline / snapshot / logging (from ez-cdc.yaml settings:)",
    ]
    lines += settings.to_env_lines()
    lines += [
        "",
        "# Ports (fixed by compose service ports mapping)",
        "HTTP_API_PORT=8080",
        "GRPC_PORT=50051",
        "",
    ]

    return "\n".join(lines)
