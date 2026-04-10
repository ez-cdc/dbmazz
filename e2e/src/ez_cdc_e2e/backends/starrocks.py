"""StarRocks target backend.

StarRocks speaks wire MySQL on port 9030 for DDL/DML, so we use the
mysql-connector-python client. The dbmazz StarRocks sink writes via the
Stream Load HTTP API (not via this connection) but we only need read
access for verification.

Audit columns (from src/connectors/sinks/starrocks/setup.rs):
    dbmazz_op_type        TINYINT
    dbmazz_is_deleted     BOOLEAN
    dbmazz_synced_at      DATETIME
    dbmazz_cdc_version    BIGINT

StarRocks does NOT have a metadata table in the target schema — the
daemon tracks LSN in its own state store (PG), not in the target.
Delete semantics: soft delete via `dbmazz_is_deleted=true`.
"""

from __future__ import annotations

from typing import Any, Optional

import mysql.connector
from mysql.connector import MySQLConnection

from .base import BackendCapabilities, ColumnInfo, TargetBackend


class StarRocksTarget(TargetBackend):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 9030,
        user: str = "root",
        password: str = "",
        database: str = "dbmazz",
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._conn: Optional[MySQLConnection] = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        self._conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            autocommit=True,
        )

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _require_conn(self) -> MySQLConnection:
        if self._conn is None:
            raise RuntimeError("StarRocksTarget.connect() was not called")
        return self._conn

    @staticmethod
    def _quote(name: str) -> str:
        # StarRocks uses backticks for identifiers (MySQL convention).
        if "`" in name:
            raise ValueError(f"identifier contains backtick: {name!r}")
        return f"`{name}`"

    def _qualified(self, table: str) -> str:
        return f"{self._quote(self.database)}.{self._quote(table)}"

    # ── identity ─────────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "starrocks"

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_hard_delete=False,          # soft delete via dbmazz_is_deleted
            supports_schema_evolution=True,
            supports_arrays=False,               # StarRocks ARRAY support is limited
            supports_enum=False,
            has_metadata_table=False,            # no metadata table in target schema
            supports_hash_compare_sql=True,
            post_cdc_settle_seconds=2.0,
            post_snapshot_settle_seconds=2.0,
        )

    def expected_audit_columns(self) -> list[str]:
        return [
            "dbmazz_op_type",
            "dbmazz_is_deleted",
            "dbmazz_synced_at",
            "dbmazz_cdc_version",
        ]

    # ── schema inspection ────────────────────────────────────────────────────

    def list_tables(self) -> list[str]:
        conn = self._require_conn()
        # StarRocks does NOT support `LIKE ... ESCAPE 'X'` (raises a syntax
        # error). Use LEFT() to filter the dbmazz_ prefix instead — portable
        # and avoids the underscore-as-wildcard problem entirely.
        sql = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s
              AND LEFT(table_name, 7) != 'dbmazz_'
            ORDER BY table_name
        """
        cur = conn.cursor()
        try:
            cur.execute(sql, (self.database,))
            return [r[0] for r in cur.fetchall()]
        finally:
            cur.close()

    def table_exists(self, table: str) -> bool:
        conn = self._require_conn()
        sql = """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """
        cur = conn.cursor()
        try:
            cur.execute(sql, (self.database, table))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def get_columns(self, table: str) -> list[ColumnInfo]:
        conn = self._require_conn()
        sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        cur = conn.cursor()
        try:
            cur.execute(sql, (self.database, table))
            return [
                ColumnInfo(
                    name=name,
                    sql_type=data_type.upper(),
                    nullable=(nullable == "YES"),
                )
                for (name, data_type, nullable) in cur.fetchall()
            ]
        finally:
            cur.close()

    def metadata_row_count(self) -> int:
        raise NotImplementedError(
            "StarRocks sink does not maintain a metadata table in the target schema"
        )

    # ── row counting ─────────────────────────────────────────────────────────

    def count_rows(self, table: str, exclude_deleted: bool = True) -> int:
        conn = self._require_conn()
        sql = f"SELECT count(*) FROM {self._qualified(table)}"
        if exclude_deleted:
            sql += " WHERE `dbmazz_is_deleted` = false"
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()

    def count_duplicates_by_pk(self, table: str, pk_column: str) -> int:
        conn = self._require_conn()
        sql = (
            f"SELECT count(*) FROM ("
            f"  SELECT {self._quote(pk_column)} FROM {self._qualified(table)} "
            f"  WHERE `dbmazz_is_deleted` = false "
            f"  GROUP BY {self._quote(pk_column)} HAVING count(*) > 1"
            f") dupes"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()

    def list_primary_keys(self, table: str, pk_column: str) -> list[Any]:
        conn = self._require_conn()
        sql = (
            f"SELECT {self._quote(pk_column)} FROM {self._qualified(table)} "
            f"WHERE `dbmazz_is_deleted` = false "
            f"ORDER BY {self._quote(pk_column)}"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return [r[0] for r in cur.fetchall()]
        finally:
            cur.close()

    # ── row queries ──────────────────────────────────────────────────────────

    def row_exists(self, table: str, pk_column: str, pk_value: Any) -> bool:
        conn = self._require_conn()
        sql = (
            f"SELECT 1 FROM {self._qualified(table)} "
            f"WHERE {self._quote(pk_column)} = %s"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql, (pk_value,))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def row_is_live(self, table: str, pk_column: str, pk_value: Any) -> bool:
        conn = self._require_conn()
        sql = (
            f"SELECT `dbmazz_is_deleted` FROM {self._qualified(table)} "
            f"WHERE {self._quote(pk_column)} = %s"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql, (pk_value,))
            row = cur.fetchone()
            if row is None:
                return False
            # `dbmazz_is_deleted` is BOOLEAN — mysql-connector returns int 0/1
            return not bool(row[0])
        finally:
            cur.close()

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
        cur = conn.cursor()
        try:
            cur.execute(sql, (pk_value,))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close()

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
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(sql, (pk_value,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            cur.close()

    # ── tier 2 helpers ───────────────────────────────────────────────────────

    def hash_table(self, table: str, pk_column: str, columns: list[str]) -> str:
        """Hash the table using StarRocks MD5 functions."""
        conn = self._require_conn()
        col_exprs = ", ".join(
            f"ifnull(cast({self._quote(c)} as string), '\\\\N')" for c in columns
        )
        sql = (
            f"SELECT md5(group_concat(row_hash ORDER BY {self._quote(pk_column)} SEPARATOR '')) "
            f"FROM ("
            f"  SELECT {self._quote(pk_column)}, md5(concat_ws('|', {col_exprs})) AS row_hash "
            f"  FROM {self._qualified(table)} "
            f"  WHERE `dbmazz_is_deleted` = false"
            f") t"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else ""
        finally:
            cur.close()

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
            f"WHERE `dbmazz_is_deleted` = false "
            f"ORDER BY {self._quote(order_by)}"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    def missing_rows_for_pks(
        self,
        table: str,
        pk_column: str,
        expected_pks: list[Any],
    ) -> list[Any]:
        if not expected_pks:
            return []
        conn = self._require_conn()
        # StarRocks doesn't support unnest; fetch target PKs and diff in Python.
        sql = (
            f"SELECT {self._quote(pk_column)} FROM {self._qualified(table)} "
            f"WHERE `dbmazz_is_deleted` = false"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            target_pks = {r[0] for r in cur.fetchall()}
        finally:
            cur.close()
        return [pk for pk in expected_pks if pk not in target_pks]

    def clean(self, tables: list[str]) -> list[str]:
        conn = self._require_conn()
        actions: list[str] = []

        audit_cols = [
            "dbmazz_op_type",
            "dbmazz_is_deleted",
            "dbmazz_synced_at",
            "dbmazz_cdc_version",
        ]
        for table in tables:
            if not self.table_exists(table):
                continue
            cur = conn.cursor()
            try:
                cur.execute(f"TRUNCATE TABLE {self._qualified(table)}")
                actions.append(f"truncated {table}")
            finally:
                cur.close()

            # StarRocks ALTER TABLE DROP COLUMN
            for col in audit_cols:
                cur = conn.cursor()
                try:
                    # Check if column exists first (StarRocks has no DROP IF EXISTS)
                    cur.execute(
                        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                        f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s",
                        (self.database, table, col),
                    )
                    if cur.fetchone():
                        cur.execute(
                            f"ALTER TABLE {self._qualified(table)} DROP COLUMN {self._quote(col)}"
                        )
                except Exception:
                    pass
                finally:
                    cur.close()
            actions.append(f"dropped audit columns from {table}")

        return actions
