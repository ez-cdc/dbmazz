"""TargetBackend ABC — contract for target sinks in the e2e runner.

Each sink (PostgreSQL, StarRocks, Snowflake, future MongoDB, etc.) implements
this interface. The verify runner and load runner only ever call methods on
this ABC — they never reach into sink-specific internals. Adding a new sink
is: new subclass + entry in profiles.py + compose service.

Sinks differ in non-trivial ways:

  - audit column naming: StarRocks uses `dbmazz_*`, Postgres uses `_dbmazz_*`,
    Snowflake uses `_DBMAZZ_*`. Each backend declares its own naming via
    `expected_audit_columns()`.
  - metadata table presence: Postgres and Snowflake have a `_dbmazz._metadata`
    table; StarRocks does not. `capabilities.has_metadata_table` gates A4.
  - delete semantics: Postgres hard-deletes (MERGE), StarRocks and Snowflake
    can soft-delete via `is_deleted=true`. `capabilities.supports_hard_delete`
    tells the verify runner which mode to test.
  - normalizer settle time: Snowflake has an async normalizer MERGE that runs
    after each COPY INTO, so verification has to wait. StarRocks is near-sync.
    Postgres has MERGE within the same TX as the COPY. `post_cdc_settle_seconds`
    exposes this per sink.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ColumnInfo:
    """Normalized column metadata."""
    name: str                  # column name as stored in the target
    sql_type: str              # backend-normalized type name (INTEGER, BIGINT, TEXT, etc.)
    nullable: bool


@dataclass(frozen=True)
class BackendCapabilities:
    """What a target sink can and cannot do. Used by the verify runner to
    skip validations that don't apply to this sink."""

    # delete semantics
    supports_hard_delete: bool           # True → DELETE removes the row; False → soft delete via is_deleted

    # schema
    supports_schema_evolution: bool      # True → ALTER TABLE ADD COLUMN is propagated automatically
    supports_arrays: bool
    supports_enum: bool

    # metadata
    has_metadata_table: bool             # True → the sink maintains a _dbmazz._metadata table (A4 applies)

    # validation helpers
    supports_hash_compare_sql: bool      # True → hash_table() uses native SQL aggregation

    # timing hints (in seconds) — how long to wait after operations before reading
    post_cdc_settle_seconds: float       # wait after CDC op before reading the target
    post_snapshot_settle_seconds: float  # wait after snapshot completes before reading


# ── ABC ──────────────────────────────────────────────────────────────────────

class TargetBackend(ABC):
    """Contract for target sinks used by the e2e verify and load runners."""

    # ── lifecycle ────────────────────────────────────────────────────────────

    @abstractmethod
    def connect(self) -> None:
        """Open the connection. Must be called before any other method."""

    @abstractmethod
    def close(self) -> None:
        """Close the connection. Safe to call multiple times."""

    def __enter__(self) -> "TargetBackend":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── metadata / identity ──────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name: 'postgres', 'starrocks', 'snowflake'."""

    @property
    @abstractmethod
    def capabilities(self) -> BackendCapabilities: ...

    @abstractmethod
    def expected_audit_columns(self) -> list[str]:
        """Return the audit column names this sink adds to replicated tables.

        Used by A3 (audit columns present). Each sink has its own naming:
            StarRocks → ['dbmazz_op_type', 'dbmazz_is_deleted', 'dbmazz_synced_at', 'dbmazz_cdc_version']
            Postgres  → ['_dbmazz_op_type', '_dbmazz_synced_at']
            Snowflake → ['_DBMAZZ_OP_TYPE', '_DBMAZZ_SYNCED_AT', '_DBMAZZ_CDC_VERSION', '_DBMAZZ_IS_DELETED']
        """

    # ── schema inspection (A1-A6) ────────────────────────────────────────────

    @abstractmethod
    def list_tables(self) -> list[str]:
        """Return user tables in the target (excluding metadata/system tables).

        Used by A1 (target tables present).
        """

    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """Return True if the named table exists."""

    @abstractmethod
    def get_columns(self, table: str) -> list[ColumnInfo]:
        """Return all columns of a table, including audit columns."""

    @abstractmethod
    def metadata_row_count(self) -> int:
        """Return the number of rows in the sink's metadata table.

        Raises NotImplementedError if `capabilities.has_metadata_table` is False.
        """

    # ── row counting (B1-B4) ─────────────────────────────────────────────────

    @abstractmethod
    def count_rows(self, table: str, exclude_deleted: bool = True) -> int:
        """Count rows in a table.

        If `exclude_deleted=True` (default) and the sink uses soft delete, the
        count excludes rows where the is_deleted audit column is true. For sinks
        that hard-delete, `exclude_deleted` has no effect.
        """

    @abstractmethod
    def count_duplicates_by_pk(self, table: str, pk_column: str) -> int:
        """Count rows that share a PK value with another row.

        Used by B3 (zero duplicates after snapshot↔CDC boundary). Should return 0.
        """

    @abstractmethod
    def list_primary_keys(self, table: str, pk_column: str) -> list[Any]:
        """Return all PK values in the target, sorted."""

    # ── row queries (D1-D5, E1) ──────────────────────────────────────────────

    @abstractmethod
    def row_exists(self, table: str, pk_column: str, pk_value: Any) -> bool:
        """Return True if a row with the given PK exists.

        For sinks that use soft-delete, this returns True even if the row has
        is_deleted=true. Use `row_is_live()` if you need the live semantic.
        """

    @abstractmethod
    def row_is_live(self, table: str, pk_column: str, pk_value: Any) -> bool:
        """Return True if a row exists and is not marked as deleted.

        For hard-delete sinks this is identical to row_exists(). For soft-delete
        sinks, returns False if is_deleted=true.
        """

    @abstractmethod
    def fetch_value(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
        column: str,
    ) -> Any:
        """Return a single column value for a row, or None if not found."""

    @abstractmethod
    def fetch_row(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
    ) -> dict[str, Any] | None:
        """Return an entire row as a dict, or None if not found."""

    # ── tier 2 helpers (C1, B4) — implemented now, used in PR 2 ──────────────

    @abstractmethod
    def hash_table(self, table: str, pk_column: str, columns: list[str]) -> str:
        """Aggregate hash over the table, ordered by PK.

        Used for C1 row-level hash compare. The hash is computed over the
        concatenation of all column values, cast to text. Returns a hex string.
        Both source and target compute the hash the same way (see SourceClient.hash_table).
        """

    @abstractmethod
    def fetch_all_rows(
        self,
        table: str,
        columns: list[str],
        order_by: str,
    ) -> list[tuple]:
        """Return all rows of the table as tuples of values.

        Used as a fallback for hash compare when the sink's SQL doesn't
        produce a hash identical to Postgres. Only suitable for fixtures,
        not for real data.
        """

    @abstractmethod
    def missing_rows_for_pks(
        self,
        table: str,
        pk_column: str,
        expected_pks: list[Any],
    ) -> list[Any]:
        """Return the PKs in `expected_pks` that don't exist in the target.

        Used by B4 (no orphan rows). The opposite direction (target PKs not
        in source) is checked by list_primary_keys + set difference.
        """
