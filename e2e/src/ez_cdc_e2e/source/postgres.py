"""PostgreSQL source client using psycopg2.

Connects to the source PG instance exposed by the e2e compose stack
(defaults to localhost:15432 in all profiles). Used by the verify runner
to seed data, apply CDC operations (INSERT/UPDATE/DELETE), and read back
for comparison with the target.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection

from .base import SourceClient


class PostgresSource(SourceClient):
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn: Optional[PgConnection] = None

    @property
    def name(self) -> str:
        return "postgres"

    def connect(self) -> None:
        self._conn = psycopg2.connect(self.dsn)
        self._conn.autocommit = True

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ── helpers ──────────────────────────────────────────────────────────────

    def _require_conn(self) -> PgConnection:
        if self._conn is None:
            raise RuntimeError("PostgresSource.connect() was not called")
        return self._conn

    @staticmethod
    def _quote_ident(name: str) -> str:
        """Quote a PG identifier, rejecting anything containing a double quote.

        Test inputs come from the runner's own constants, not user input, so
        this is defense-in-depth, not input sanitization.
        """
        if '"' in name:
            raise ValueError(f"identifier contains double-quote: {name!r}")
        return f'"{name}"'

    # ── reads ────────────────────────────────────────────────────────────────

    def count_rows(self, table: str) -> int:
        conn = self._require_conn()
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self._quote_ident(table)}")
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def fetch_value(self, table: str, pk_col: str, pk_val: Any, column: str) -> Any:
        conn = self._require_conn()
        sql = (
            f"SELECT {self._quote_ident(column)} "
            f"FROM {self._quote_ident(table)} "
            f"WHERE {self._quote_ident(pk_col)} = %s"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (pk_val,))
            row = cur.fetchone()
            return row[0] if row else None

    def list_primary_keys(self, table: str, pk_col: str) -> list[Any]:
        conn = self._require_conn()
        sql = (
            f"SELECT {self._quote_ident(pk_col)} "
            f"FROM {self._quote_ident(table)} "
            f"ORDER BY {self._quote_ident(pk_col)}"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
            return [r[0] for r in cur.fetchall()]

    # ── writes ───────────────────────────────────────────────────────────────

    def insert_row(self, table: str, values: dict[str, Any], pk_col: str = "id") -> Any:
        conn = self._require_conn()
        cols = list(values.keys())
        col_list = ", ".join(self._quote_ident(c) for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = (
            f"INSERT INTO {self._quote_ident(table)} ({col_list}) "
            f"VALUES ({placeholders}) "
            f"RETURNING {self._quote_ident(pk_col)}"
        )
        with conn.cursor() as cur:
            cur.execute(sql, [values[c] for c in cols])
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"INSERT into {table} returned no row")
            return row[0]

    def insert_many(
        self,
        table: str,
        rows: list[dict[str, Any]],
        pk_col: str = "id",
    ) -> list[Any]:
        if not rows:
            return []

        conn = self._require_conn()

        # All rows must have the same column set (caller's responsibility).
        cols = list(rows[0].keys())
        col_list = ", ".join(self._quote_ident(c) for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = (
            f"INSERT INTO {self._quote_ident(table)} ({col_list}) "
            f"VALUES ({placeholders}) "
            f"RETURNING {self._quote_ident(pk_col)}"
        )

        # Atomic: turn off autocommit, commit once at the end.
        conn.autocommit = False
        try:
            pks: list[Any] = []
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(sql, [row[c] for c in cols])
                    result = cur.fetchone()
                    if result is None:
                        raise RuntimeError(f"INSERT into {table} returned no row")
                    pks.append(result[0])
            conn.commit()
            return pks
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = True

    def update_row(
        self,
        table: str,
        pk_col: str,
        pk_val: Any,
        set_values: dict[str, Any],
    ) -> None:
        conn = self._require_conn()
        cols = list(set_values.keys())
        assignments = ", ".join(f"{self._quote_ident(c)} = %s" for c in cols)
        sql = (
            f"UPDATE {self._quote_ident(table)} "
            f"SET {assignments} "
            f"WHERE {self._quote_ident(pk_col)} = %s"
        )
        with conn.cursor() as cur:
            cur.execute(sql, [set_values[c] for c in cols] + [pk_val])

    def delete_row(self, table: str, pk_col: str, pk_val: Any) -> None:
        conn = self._require_conn()
        sql = (
            f"DELETE FROM {self._quote_ident(table)} "
            f"WHERE {self._quote_ident(pk_col)} = %s"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (pk_val,))

    # ── schema operations ────────────────────────────────────────────────────

    def ensure_column(self, table: str, column: str, sql_type: str) -> None:
        conn = self._require_conn()
        sql = (
            f"ALTER TABLE {self._quote_ident(table)} "
            f"ADD COLUMN IF NOT EXISTS {self._quote_ident(column)} {sql_type}"
        )
        with conn.cursor() as cur:
            cur.execute(sql)

    def alter_add_column(self, table: str, column: str, sql_type: str) -> None:
        conn = self._require_conn()
        sql = (
            f"ALTER TABLE {self._quote_ident(table)} "
            f"ADD COLUMN {self._quote_ident(column)} {sql_type}"
        )
        with conn.cursor() as cur:
            cur.execute(sql)

    def drop_column_if_exists(self, table: str, column: str) -> None:
        conn = self._require_conn()
        sql = (
            f"ALTER TABLE {self._quote_ident(table)} "
            f"DROP COLUMN IF EXISTS {self._quote_ident(column)}"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
