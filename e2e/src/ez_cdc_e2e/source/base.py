"""SourceClient ABC — contract for source databases in the e2e runner.

For now only PostgresSource is implemented. This abstraction exists so that
adding a second source type (MySQL, Oracle) later only requires a new
subclass without touching the verify runner.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class SourceClient(ABC):
    """Contract for the source database driver used by e2e tests."""

    @abstractmethod
    def connect(self) -> None:
        """Open the connection. Must be called before any other method."""

    @abstractmethod
    def close(self) -> None:
        """Close the connection. Safe to call multiple times."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name for error messages (e.g., 'postgres')."""

    # ── lifecycle as context manager ─────────────────────────────────────────

    def __enter__(self) -> "SourceClient":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── reads ────────────────────────────────────────────────────────────────

    @abstractmethod
    def count_rows(self, table: str) -> int:
        """Return total row count of a table."""

    @abstractmethod
    def fetch_value(self, table: str, pk_col: str, pk_val: Any, column: str) -> Any:
        """Return a single column value for a row, or None if not found."""

    @abstractmethod
    def list_primary_keys(self, table: str, pk_col: str) -> list[Any]:
        """Return all PK values in the table, sorted."""

    # ── writes ───────────────────────────────────────────────────────────────

    @abstractmethod
    def insert_row(self, table: str, values: dict[str, Any], pk_col: str = "id") -> Any:
        """Insert a single row. Returns the generated PK value."""

    @abstractmethod
    def insert_many(self, table: str, rows: list[dict[str, Any]], pk_col: str = "id") -> list[Any]:
        """Insert multiple rows in a single transaction. Returns all generated PKs.

        Used for D5 (multi-row INSERT in single TX) in tier 1.
        """

    @abstractmethod
    def update_row(
        self,
        table: str,
        pk_col: str,
        pk_val: Any,
        set_values: dict[str, Any],
    ) -> None:
        """Update one or more columns of a row identified by PK."""

    @abstractmethod
    def delete_row(self, table: str, pk_col: str, pk_val: Any) -> None:
        """Delete a row by PK."""

    # ── schema operations ────────────────────────────────────────────────────

    @abstractmethod
    def ensure_column(self, table: str, column: str, sql_type: str) -> None:
        """Idempotent ALTER TABLE ADD COLUMN IF NOT EXISTS.

        Used for test setup (e.g., ensuring the TOAST test column exists
        before D4 runs). For schema evolution testing (A5, tier 2), use
        alter_add_column which always runs and is observable.
        """

    @abstractmethod
    def alter_add_column(self, table: str, column: str, sql_type: str) -> None:
        """ALTER TABLE ADD COLUMN (non-idempotent). Used for A5 tier 2."""

    @abstractmethod
    def drop_column_if_exists(self, table: str, column: str) -> None:
        """ALTER TABLE DROP COLUMN IF EXISTS. Used for test cleanup."""
