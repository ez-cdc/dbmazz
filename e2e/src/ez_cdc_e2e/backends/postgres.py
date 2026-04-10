"""PostgreSQL target backend.

The dbmazz Postgres sink uses a COPY → raw table → MERGE normalizer pattern.
Target tables live in the configured schema (default `public`) with audit
columns `_dbmazz_synced_at` and `_dbmazz_op_type`. Metadata is tracked in
`_dbmazz._metadata`. Delete semantics are HARD delete: the MERGE removes
rows that correspond to CDC DELETE events, so a deleted row simply
disappears from the target.

See src/connectors/sinks/postgres/setup.rs for the authoritative schema.
"""

from __future__ import annotations

from typing import Any, Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import RealDictCursor

from .base import BackendCapabilities, ColumnInfo, TargetBackend


_METADATA_SCHEMA = "_dbmazz"
_METADATA_TABLE = "_metadata"


class PostgresTarget(TargetBackend):
    def __init__(self, dsn: str, schema: str = "public") -> None:
        self.dsn = dsn
        self.schema = schema
        self._conn: Optional[PgConnection] = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        self._conn = psycopg2.connect(self.dsn)
        self._conn.autocommit = True

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _require_conn(self) -> PgConnection:
        if self._conn is None:
            raise RuntimeError("PostgresTarget.connect() was not called")
        return self._conn

    @staticmethod
    def _quote(name: str) -> str:
        if '"' in name:
            raise ValueError(f"identifier contains double-quote: {name!r}")
        return f'"{name}"'

    def _qualified(self, table: str) -> str:
        return f"{self._quote(self.schema)}.{self._quote(table)}"

    # ── identity ─────────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "postgres"

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_hard_delete=True,       # MERGE deletes rows physically
            supports_schema_evolution=True,
            supports_arrays=True,
            supports_enum=True,
            has_metadata_table=True,
            supports_hash_compare_sql=True,
            post_cdc_settle_seconds=0.5,     # MERGE is synchronous with the COPY
            post_snapshot_settle_seconds=1.0,
        )

    def expected_audit_columns(self) -> list[str]:
        # Postgres sink only adds 2 audit columns (no is_deleted, no cdc_version).
        # See src/connectors/sinks/postgres/setup.rs:183-184.
        return ["_dbmazz_synced_at", "_dbmazz_op_type"]

    # ── schema inspection ────────────────────────────────────────────────────

    def list_tables(self) -> list[str]:
        """List user tables in the target schema, excluding metadata/raw tables."""
        conn = self._require_conn()
        sql = """
            SELECT tablename FROM pg_tables
            WHERE schemaname = %s
              AND tablename NOT LIKE '\\_dbmazz\\_%%' ESCAPE '\\'
              AND tablename NOT LIKE '\\_raw\\_%%' ESCAPE '\\'
            ORDER BY tablename
        """
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            return [r[0] for r in cur.fetchall()]

    def table_exists(self, table: str) -> bool:
        conn = self._require_conn()
        sql = """
            SELECT 1 FROM pg_tables
            WHERE schemaname = %s AND tablename = %s
        """
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema, table))
            return cur.fetchone() is not None

    def get_columns(self, table: str) -> list[ColumnInfo]:
        conn = self._require_conn()
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema, table))
            return [
                ColumnInfo(
                    name=name,
                    sql_type=data_type.upper(),
                    nullable=(nullable == "YES"),
                )
                for (name, data_type, nullable) in cur.fetchall()
            ]

    def metadata_row_count(self) -> int:
        conn = self._require_conn()
        sql = f"SELECT count(*) FROM {self._quote(_METADATA_SCHEMA)}.{self._quote(_METADATA_TABLE)}"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0

    # ── row counting ─────────────────────────────────────────────────────────

    def count_rows(self, table: str, exclude_deleted: bool = True) -> int:
        # Postgres uses hard delete, so exclude_deleted is a no-op.
        conn = self._require_conn()
        sql = f"SELECT count(*) FROM {self._qualified(table)}"
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def count_duplicates_by_pk(self, table: str, pk_column: str) -> int:
        conn = self._require_conn()
        sql = (
            f"SELECT count(*) FROM ("
            f"  SELECT {self._quote(pk_column)} FROM {self._qualified(table)} "
            f"  GROUP BY {self._quote(pk_column)} HAVING count(*) > 1"
            f") dupes"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def list_primary_keys(self, table: str, pk_column: str) -> list[Any]:
        conn = self._require_conn()
        sql = (
            f"SELECT {self._quote(pk_column)} FROM {self._qualified(table)} "
            f"ORDER BY {self._quote(pk_column)}"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
            return [r[0] for r in cur.fetchall()]

    # ── row queries ──────────────────────────────────────────────────────────

    def row_exists(self, table: str, pk_column: str, pk_value: Any) -> bool:
        conn = self._require_conn()
        sql = (
            f"SELECT 1 FROM {self._qualified(table)} "
            f"WHERE {self._quote(pk_column)} = %s"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (pk_value,))
            return cur.fetchone() is not None

    def row_is_live(self, table: str, pk_column: str, pk_value: Any) -> bool:
        # Hard-delete backend: live == exists.
        return self.row_exists(table, pk_column, pk_value)

    def fetch_value(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
        column: str,
    ) -> Any:
        conn = self._require_conn()
        sql = (
            f"SELECT {self._quote(column)} FROM {self._qualified(table)} "
            f"WHERE {self._quote(pk_column)} = %s"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (pk_value,))
            row = cur.fetchone()
            return row[0] if row else None

    def fetch_row(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
    ) -> dict[str, Any] | None:
        conn = self._require_conn()
        sql = (
            f"SELECT * FROM {self._qualified(table)} "
            f"WHERE {self._quote(pk_column)} = %s"
        )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (pk_value,))
            row = cur.fetchone()
            return dict(row) if row else None

    # ── tier 2 helpers ───────────────────────────────────────────────────────

    def hash_table(self, table: str, pk_column: str, columns: list[str]) -> str:
        """Hash the entire table by concatenating all column values per row,
        then aggregating with md5 over rows ordered by PK."""
        conn = self._require_conn()
        # Build a CONCAT_WS expression that casts each column to text with a
        # NULL-safe coalesce. Use md5 per row, then md5 over the concatenation
        # of row hashes, in PK order.
        col_exprs = ", ".join(f"COALESCE({self._quote(c)}::text, '\\\\N')" for c in columns)
        sql = (
            f"SELECT md5(string_agg(row_hash, '' ORDER BY {self._quote(pk_column)})) "
            f"FROM ("
            f"  SELECT {self._quote(pk_column)}, md5(concat_ws('|', {col_exprs})) AS row_hash "
            f"  FROM {self._qualified(table)}"
            f") t"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else ""

    def fetch_all_rows(
        self,
        table: str,
        columns: list[str],
        order_by: str,
    ) -> list[tuple]:
        conn = self._require_conn()
        col_list = ", ".join(self._quote(c) for c in columns)
        sql = (
            f"SELECT {col_list} FROM {self._qualified(table)} "
            f"ORDER BY {self._quote(order_by)}"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def missing_rows_for_pks(
        self,
        table: str,
        pk_column: str,
        expected_pks: list[Any],
    ) -> list[Any]:
        if not expected_pks:
            return []
        conn = self._require_conn()
        sql = (
            f"SELECT unnest(%s::bigint[]) AS expected_pk "
            f"EXCEPT "
            f"SELECT {self._quote(pk_column)} FROM {self._qualified(table)}"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (list(expected_pks),))
            return [r[0] for r in cur.fetchall()]

    def clean(self, tables: list[str]) -> list[str]:
        conn = self._require_conn()
        actions: list[str] = []

        audit_cols = ["_dbmazz_synced_at", "_dbmazz_op_type"]
        for table in tables:
            if not self.table_exists(table):
                continue
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {self._qualified(table)}")
                actions.append(f"truncated {table}")
                for col in audit_cols:
                    try:
                        cur.execute(
                            f"ALTER TABLE {self._qualified(table)} "
                            f"DROP COLUMN IF EXISTS {self._quote(col)}"
                        )
                    except Exception:
                        pass
                actions.append(f"dropped audit columns from {table}")

        # Drop _dbmazz schema (metadata, raw tables)
        with conn.cursor() as cur:
            try:
                cur.execute(f"DROP SCHEMA IF EXISTS {self._quote(_METADATA_SCHEMA)} CASCADE")
                actions.append("dropped _dbmazz schema (metadata, raw tables)")
            except Exception as e:
                actions.append(f"failed to drop _dbmazz schema: {e}")

        return actions
