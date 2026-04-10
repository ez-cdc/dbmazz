"""Pydantic schema for `e2e/ez-cdc.yaml`.

A datasources file has two top-level sections:

    sources:
      <name>: <PostgresSourceSpec>
    sinks:
      <name>: <PostgresSinkSpec | StarRocksSinkSpec | SnowflakeSinkSpec>

Each spec discriminates on `type:`. The `managed` flag distinguishes between
"ez-cdc runs this database in Docker for you" (managed=true) and "you provide
the connection details for an existing instance" (managed=false).

Pydantic v2 is used for validation. Errors include the field path and a
human-readable message — see DatasourceValidationError in loader.py for the
formatting layer that turns pydantic errors into something a CLI user can act on.

Notes on naming consistency:

  - Source/sink **names** are restricted to lowercase letters, digits, hyphens,
    and underscores. Names are used as filesystem cache keys (compose builder)
    and as command-line arguments, so we want them shell-safe.
  - **Tables** are stored as a list of unqualified table names; schema is
    always 'public' for now (tracked as TODO for multi-schema support).
  - **Passwords** can use `${VAR}` interpolation; see loader.py.
"""

from __future__ import annotations

import re
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ── Identifier validation ────────────────────────────────────────────────────

# Datasource names: lowercase, alphanumeric + hyphen/underscore, 1-64 chars.
_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")

# Postgres logical replication slot/publication: pg internal naming rules.
_PG_IDENT_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _validate_datasource_name(name: str) -> str:
    if not _NAME_PATTERN.match(name):
        raise ValueError(
            f"datasource name {name!r} must be lowercase alphanumeric "
            f"(plus '-' and '_'), 1-64 chars, starting with a letter or digit"
        )
    return name


def _validate_pg_ident(value: str) -> str:
    if not _PG_IDENT_PATTERN.match(value):
        raise ValueError(
            f"PostgreSQL identifier {value!r} must start with a lowercase letter "
            f"or underscore and contain only lowercase letters, digits, and underscores"
        )
    return value


# ── Common base ──────────────────────────────────────────────────────────────

class _DatasourceBase(BaseModel):
    """Shared config for all datasource specs."""

    model_config = ConfigDict(
        extra="forbid",          # reject unknown fields with a clear error
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    managed: bool = Field(
        ...,
        description=(
            "If true, ez-cdc runs this database in a Docker container for you. "
            "If false, you provide the connection details for an existing instance."
        ),
    )


# ── Source specs ─────────────────────────────────────────────────────────────

class PostgresSourceSpec(_DatasourceBase):
    """A PostgreSQL source database for CDC.

    For managed=true (we run it):
      - `seed` (optional): path to a SQL file inside e2e/fixtures/ to apply
        on startup. Defaults to postgres-seed.sql which provides the
        orders + order_items demo schema.
      - `tables` (required): which tables to replicate.

    For managed=false (user-provided):
      - `url` (required): PostgreSQL connection URL. Should NOT include
        `?replication=database` — ez-cdc adds it when starting dbmazz.
      - `replication_slot` (optional): defaults to "dbmazz_slot".
      - `publication` (optional): defaults to "dbmazz_pub".
      - `tables` (required).
    """

    type: Literal["postgres"] = "postgres"

    # Connection (only used when managed=false)
    url: str | None = Field(
        default=None,
        description="postgres:// connection URL. Required when managed=false.",
    )

    # Fixture (only used when managed=true)
    seed: str | None = Field(
        default=None,
        description="Path to a SQL seed file inside e2e/fixtures/.",
    )

    # Replication topology (applies to both modes)
    replication_slot: str = Field(
        default="dbmazz_slot",
        description="Logical replication slot name.",
    )
    publication: str = Field(
        default="dbmazz_pub",
        description="Publication name.",
    )

    tables: list[str] = Field(
        ...,
        min_length=1,
        description="Tables to replicate. Schema is assumed to be 'public'.",
    )

    @field_validator("replication_slot", "publication")
    @classmethod
    def _check_pg_ident(cls, v: str) -> str:
        return _validate_pg_ident(v)

    @field_validator("tables")
    @classmethod
    def _check_tables(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("tables cannot be empty")
        seen: set[str] = set()
        for t in v:
            if not t or not t.strip():
                raise ValueError("table name cannot be empty")
            if t in seen:
                raise ValueError(f"duplicate table {t!r}")
            seen.add(t)
        return v


# ── Sink specs ───────────────────────────────────────────────────────────────

class PostgresSinkSpec(_DatasourceBase):
    """A PostgreSQL sink database.

    For managed=true: ez-cdc spins up a fresh empty Postgres in Docker.
    For managed=false: user provides url + database.
    """

    type: Literal["postgres"] = "postgres"

    url: str | None = Field(
        default=None,
        description="postgres:// connection URL. Required when managed=false.",
    )
    database: str | None = Field(
        default=None,
        description="Target database name. Required when managed=false.",
    )
    schema_: str = Field(
        default="public",
        alias="schema",
        description="Target schema. Defaults to 'public'.",
    )


class StarRocksSinkSpec(_DatasourceBase):
    """A StarRocks sink.

    For managed=true: ez-cdc spins up StarRocks in Docker (~60s on first run).
    For managed=false: user provides FE HTTP URL + credentials.
    """

    type: Literal["starrocks"] = "starrocks"

    url: str | None = Field(
        default=None,
        description="StarRocks FE HTTP URL like http://host:8030. Required when managed=false.",
    )
    mysql_port: int = Field(
        default=9030,
        ge=1,
        le=65535,
        description="StarRocks FE MySQL protocol port (used for DDL).",
    )
    database: str | None = Field(
        default=None,
        description="Target database. Required when managed=false.",
    )
    user: str = Field(default="root")
    password: str = Field(default="")


class SnowflakeSinkSpec(_DatasourceBase):
    """A Snowflake sink.

    Snowflake is cloud-only and can NEVER be managed=true. The schema enforces
    this in the validator below — passing managed=true will raise.
    """

    type: Literal["snowflake"] = "snowflake"

    account: str = Field(
        ...,
        description="Snowflake account identifier (e.g., xy12345.us-east-1).",
    )
    user: str = Field(...)
    password: str = Field(..., description="Or use a JWT key — see private_key_path.")
    database: str = Field(...)
    schema_: str = Field(default="PUBLIC", alias="schema")
    warehouse: str = Field(...)
    role: str | None = None
    private_key_path: str | None = Field(
        default=None,
        description="Optional RSA key path for JWT auth (preferred over password).",
    )
    soft_delete: bool = Field(
        default=True,
        description="If true, DELETEs become _DBMAZZ_IS_DELETED=true. If false, hard delete.",
    )

    @field_validator("managed", mode="before")
    @classmethod
    def _enforce_not_managed(cls, v: bool) -> bool:
        if v is True:
            raise ValueError(
                "Snowflake is cloud-only and cannot be managed by ez-cdc. "
                "Set managed: false and provide your account credentials."
            )
        return v


# ── Pipeline settings ───────────────────────────────────────────────────────

class PipelineSettings(BaseModel):
    """Tuning knobs for the dbmazz daemon.

    These map 1:1 to the environment variables that dbmazz reads from
    ``Config::from_env()`` in ``src/config.rs``.  Defaults match the
    values used by the old static compose.yml so existing workflows are
    unchanged when ``settings:`` is omitted from the YAML.
    """

    model_config = ConfigDict(extra="forbid")

    flush_size: int = Field(
        default=2000,
        ge=1,
        description="Max events per batch (FLUSH_SIZE).",
    )
    flush_interval_ms: int = Field(
        default=2000,
        ge=100,
        description="Max ms before flushing a partial batch (FLUSH_INTERVAL_MS).",
    )
    do_snapshot: bool = Field(
        default=True,
        description="Enable initial snapshot / backfill (DO_SNAPSHOT).",
    )
    snapshot_chunk_size: int = Field(
        default=10000,
        ge=1,
        description="Rows per snapshot chunk (SNAPSHOT_CHUNK_SIZE).",
    )
    snapshot_parallel_workers: int = Field(
        default=2,
        ge=1,
        le=32,
        description="Parallel snapshot workers (SNAPSHOT_PARALLEL_WORKERS).",
    )
    initial_snapshot_only: bool = Field(
        default=False,
        description="Exit after snapshot — no CDC (INITIAL_SNAPSHOT_ONLY).",
    )
    rust_log: str = Field(
        default="info",
        description="Rust log filter (RUST_LOG). e.g. info, debug, dbmazz=debug.",
    )
    snowflake_flush_files: int = Field(
        default=1,
        ge=1,
        description="Snowflake: COPY INTO after N staged files (SINK_SNOWFLAKE_FLUSH_FILES). "
        "Default 1 for e2e (immediate). Production uses 20.",
    )
    snowflake_flush_bytes: int = Field(
        default=104857600,
        ge=1,
        description="Snowflake: COPY INTO after N bytes staged (SINK_SNOWFLAKE_FLUSH_BYTES). "
        "Default 100MB. Whichever threshold (files or bytes) is reached first triggers the flush.",
    )

    def to_env_lines(self) -> list[str]:
        """Render as KEY=value lines for the .env file."""
        return [
            f"FLUSH_SIZE={self.flush_size}",
            f"FLUSH_INTERVAL_MS={self.flush_interval_ms}",
            f"DO_SNAPSHOT={'true' if self.do_snapshot else 'false'}",
            f"SNAPSHOT_CHUNK_SIZE={self.snapshot_chunk_size}",
            f"SNAPSHOT_PARALLEL_WORKERS={self.snapshot_parallel_workers}",
            f"INITIAL_SNAPSHOT_ONLY={'true' if self.initial_snapshot_only else 'false'}",
            f"RUST_LOG={self.rust_log}",
            f"SINK_SNOWFLAKE_FLUSH_FILES={self.snowflake_flush_files}",
            f"SINK_SNOWFLAKE_FLUSH_BYTES={self.snowflake_flush_bytes}",
        ]



# ── Discriminated unions ─────────────────────────────────────────────────────

# Pydantic v2 discriminator on `type` — tells the parser which subclass to
# instantiate based on the `type:` field in the YAML.
SourceSpec = Annotated[
    Union[PostgresSourceSpec],
    Field(discriminator="type"),
]

SinkSpec = Annotated[
    Union[PostgresSinkSpec, StarRocksSinkSpec, SnowflakeSinkSpec],
    Field(discriminator="type"),
]


# ── Top-level container ──────────────────────────────────────────────────────

class DatasourcesFile(BaseModel):
    """Top-level structure of `e2e/ez-cdc.yaml`.

    Loaded by `loader.load_datasources()`. Both `sources` and `sinks` are
    dicts keyed by datasource name. Validation ensures names are unique within
    each section (Python dicts already enforce this) and that names match
    the allowed pattern.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    settings: PipelineSettings = Field(default_factory=PipelineSettings)
    sources: dict[str, SourceSpec] = Field(default_factory=dict)
    sinks: dict[str, SinkSpec] = Field(default_factory=dict)

    @field_validator("sources", "sinks")
    @classmethod
    def _validate_names(cls, v: dict) -> dict:
        for name in v.keys():
            _validate_datasource_name(name)
        return v

    # ── helpers ──────────────────────────────────────────────────────────────

    def has_any(self) -> bool:
        """True if at least one source AND one sink are configured."""
        return bool(self.sources) and bool(self.sinks)

    def is_empty(self) -> bool:
        """True if neither sources nor sinks are configured."""
        return not self.sources and not self.sinks

    def list_source_names(self) -> list[str]:
        return sorted(self.sources.keys())

    def list_sink_names(self) -> list[str]:
        return sorted(self.sinks.keys())

    def get_source(self, name: str) -> SourceSpec:
        if name not in self.sources:
            from .loader import DatasourceNotFoundError  # avoid circular import
            available = ", ".join(self.list_source_names()) or "(none configured)"
            raise DatasourceNotFoundError(
                f"source datasource {name!r} not found. Available: {available}"
            )
        return self.sources[name]

    def get_sink(self, name: str) -> SinkSpec:
        if name not in self.sinks:
            from .loader import DatasourceNotFoundError
            available = ", ".join(self.list_sink_names()) or "(none configured)"
            raise DatasourceNotFoundError(
                f"sink datasource {name!r} not found. Available: {available}"
            )
        return self.sinks[name]
