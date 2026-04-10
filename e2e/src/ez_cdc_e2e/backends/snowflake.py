"""Snowflake target backend.

The dbmazz Snowflake sink uses a two-phase ELT pipeline: Parquet files
uploaded to an internal stage, COPY INTO a raw table with PARSE_JSON →
VARIANT, and an async normalizer MERGE that populates the final target
tables. Because the normalizer is async, verification has to wait
~10 seconds after the last CDC event before reading.

Audit columns (from src/connectors/sinks/snowflake/setup.rs):
    _DBMAZZ_OP_TYPE         NUMBER(3,0)
    _DBMAZZ_SYNCED_AT       TIMESTAMP_NTZ
    _DBMAZZ_CDC_VERSION     NUMBER(20,0)
    _DBMAZZ_IS_DELETED      BOOLEAN  (only when soft-delete enabled)

Metadata: `_DBMAZZ._METADATA` (in the configured database).

Credentials come from e2e/.env.snowflake (copy .env.snowflake.example).
Authentication: username + password (key-pair JWT is also supported by
the driver but we stick to user/pass for simplicity here since the
dev compose profile uses the same).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from .base import BackendCapabilities, ColumnInfo, TargetBackend


def _load_env_file(path: Path) -> None:
    """Parse a .env file and inject its variables into os.environ.

    Existing env vars take precedence (setdefault), so the user can override
    the file from their shell. Minimal parser: no variable substitution, no
    multi-line values. Good enough for .env.snowflake.
    """
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip surrounding quotes if balanced
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        os.environ.setdefault(key, value)


_INTERNAL_SCHEMA = "_DBMAZZ"
_METADATA_TABLE = "_METADATA"


class SnowflakeTarget(TargetBackend):
    def __init__(self, env_file: Optional[Path] = None) -> None:
        """Initialize from environment variables (optionally loaded from a file).

        Reads: SINK_SNOWFLAKE_ACCOUNT, SINK_USER, SINK_PASSWORD, SINK_DATABASE,
        SINK_SCHEMA (default PUBLIC), SINK_SNOWFLAKE_WAREHOUSE, SINK_SNOWFLAKE_ROLE,
        SINK_SNOWFLAKE_SOFT_DELETE (default "true").
        """
        if env_file is not None:
            _load_env_file(env_file)

        self.account = _require_env("SINK_SNOWFLAKE_ACCOUNT")
        self.user = _require_env("SINK_USER")
        self.password = _require_env("SINK_PASSWORD")
        self.database = _require_env("SINK_DATABASE")
        self.schema = os.environ.get("SINK_SCHEMA", "PUBLIC")
        self.warehouse = _require_env("SINK_SNOWFLAKE_WAREHOUSE")
        self.role = os.environ.get("SINK_SNOWFLAKE_ROLE") or None
        self.soft_delete = os.environ.get("SINK_SNOWFLAKE_SOFT_DELETE", "true").lower() == "true"
        self._conn: Optional[SnowflakeConnection] = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        kwargs = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        }
        if self.role:
            kwargs["role"] = self.role
        self._conn = snowflake.connector.connect(**kwargs)

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _require_conn(self) -> SnowflakeConnection:
        if self._conn is None:
            raise RuntimeError("SnowflakeTarget.connect() was not called")
        return self._conn

    @staticmethod
    def _quote(name: str) -> str:
        # Snowflake uses double-quoted identifiers; unquoted identifiers are
        # case-folded to uppercase. dbmazz creates tables in uppercase (see sanitize_identifier).
        if '"' in name:
            raise ValueError(f"identifier contains double-quote: {name!r}")
        return f'"{name.upper()}"'

    def _qualified(self, table: str) -> str:
        return f"{self._quote(self.database)}.{self._quote(self.schema)}.{self._quote(table)}"

    # ── identity ─────────────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "snowflake"

    @property
    def capabilities(self) -> BackendCapabilities:
        return BackendCapabilities(
            supports_hard_delete=not self.soft_delete,
            supports_schema_evolution=True,
            supports_arrays=False,
            supports_enum=False,
            has_metadata_table=True,
            supports_hash_compare_sql=True,
            post_cdc_settle_seconds=10.0,        # async normalizer MERGE
            post_snapshot_settle_seconds=15.0,   # snapshot flushes multiple COPY INTOs
        )

    def expected_audit_columns(self) -> list[str]:
        cols = [
            "_DBMAZZ_OP_TYPE",
            "_DBMAZZ_SYNCED_AT",
            "_DBMAZZ_CDC_VERSION",
        ]
        if self.soft_delete:
            cols.insert(1, "_DBMAZZ_IS_DELETED")
        return cols

    # ── schema inspection ────────────────────────────────────────────────────

    def list_tables(self) -> list[str]:
        conn = self._require_conn()
        # Snowflake supports `LIKE ... ESCAPE` but the syntax with backslash
        # interactions in f-strings is fragile. Use LEFT() to filter prefixes —
        # portable, no escape headaches, and matches the StarRocksTarget approach.
        sql = f"""
            SELECT TABLE_NAME FROM {self._quote(self.database)}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE = 'BASE TABLE'
              AND LEFT(TABLE_NAME, 8) != '_DBMAZZ_'
              AND LEFT(TABLE_NAME, 5) != '_RAW_'
            ORDER BY TABLE_NAME
        """
        cur = conn.cursor()
        try:
            cur.execute(sql, (self.schema.upper(),))
            return [r[0] for r in cur.fetchall()]
        finally:
            cur.close()

    def table_exists(self, table: str) -> bool:
        conn = self._require_conn()
        sql = f"""
            SELECT 1 FROM {self._quote(self.database)}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        cur = conn.cursor()
        try:
            cur.execute(sql, (self.schema.upper(), table.upper()))
            return cur.fetchone() is not None
        finally:
            cur.close()

    def get_columns(self, table: str) -> list[ColumnInfo]:
        conn = self._require_conn()
        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM {self._quote(self.database)}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        cur = conn.cursor()
        try:
            cur.execute(sql, (self.schema.upper(), table.upper()))
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
        conn = self._require_conn()
        sql = (
            f"SELECT count(*) FROM {self._quote(self.database)}."
            f"{self._quote(_INTERNAL_SCHEMA)}.{self._quote(_METADATA_TABLE)}"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()

    # ── row counting ─────────────────────────────────────────────────────────

    def count_rows(self, table: str, exclude_deleted: bool = True) -> int:
        conn = self._require_conn()
        sql = f"SELECT count(*) FROM {self._qualified(table)}"
        if exclude_deleted and self.soft_delete:
            sql += ' WHERE "_DBMAZZ_IS_DELETED" = FALSE'
        cur = conn.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()

    def count_duplicates_by_pk(self, table: str, pk_column: str) -> int:
        conn = self._require_conn()
        where = ' WHERE "_DBMAZZ_IS_DELETED" = FALSE' if self.soft_delete else ""
        sql = (
            f"SELECT count(*) FROM ("
            f"  SELECT {self._quote(pk_column)} FROM {self._qualified(table)}{where} "
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
        where = ' WHERE "_DBMAZZ_IS_DELETED" = FALSE' if self.soft_delete else ""
        sql = (
            f"SELECT {self._quote(pk_column)} FROM {self._qualified(table)}{where} "
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
        if not self.soft_delete:
            return self.row_exists(table, pk_column, pk_value)
        conn = self._require_conn()
        sql = (
            f'SELECT "_DBMAZZ_IS_DELETED" FROM {self._qualified(table)} '
            f"WHERE {self._quote(pk_column)} = %s"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql, (pk_value,))
            row = cur.fetchone()
            if row is None:
                return False
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
        cur = conn.cursor()
        try:
            cur.execute(sql, (pk_value,))
            row = cur.fetchone()
            if row is None:
                return None
            col_names = [d[0] for d in cur.description]
            return dict(zip(col_names, row))
        finally:
            cur.close()

    # ── tier 2 helpers ───────────────────────────────────────────────────────

    def hash_table(self, table: str, pk_column: str, columns: list[str]) -> str:
        conn = self._require_conn()
        col_exprs = ", ".join(
            f"COALESCE(CAST({self._quote(c)} AS VARCHAR), '\\\\N')" for c in columns
        )
        where = ' WHERE "_DBMAZZ_IS_DELETED" = FALSE' if self.soft_delete else ""
        sql = (
            f"SELECT MD5(LISTAGG(row_hash, '') WITHIN GROUP (ORDER BY {self._quote(pk_column)})) "
            f"FROM ("
            f"  SELECT {self._quote(pk_column)}, MD5(CONCAT_WS('|', {col_exprs})) AS row_hash "
            f"  FROM {self._qualified(table)}{where}"
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
        where = ' WHERE "_DBMAZZ_IS_DELETED" = FALSE' if self.soft_delete else ""
        sql = (
            f"SELECT {col_list} FROM {self._qualified(table)}{where} "
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
        where = ' WHERE "_DBMAZZ_IS_DELETED" = FALSE' if self.soft_delete else ""
        sql = (
            f"SELECT {self._quote(pk_column)} FROM {self._qualified(table)}{where}"
        )
        cur = conn.cursor()
        try:
            cur.execute(sql)
            target_pks = {r[0] for r in cur.fetchall()}
        finally:
            cur.close()
        return [pk for pk in expected_pks if pk not in target_pks]


def _require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(
            f"{key} is required for the Snowflake backend. "
            f"Set it in e2e/.env.snowflake (copy .env.snowflake.example)."
        )
    return val
