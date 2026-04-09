"""Tier 1 validation checks.

Baseline correctness suite — the minimum every sink must pass. Runs in
~30s per sink. Includes D4 (TOAST UPDATE), which is the bug #1 historical
failure mode in CDCs of PostgreSQL and therefore mandatory in tier 1.

Every function in this module returns a CheckResult. The runner
(verify/runner.py) sequences them, manages mutation ordering, and
collects results.

Checks are grouped into phases:

  Phase 1 — Schema (read-only): A1, A2, A3, A4
  Phase 2 — Snapshot baseline: B1, B3
  Phase 3 — Single-row CDC flow: D1 → D2 → E1 → D3 (one order.id threaded)
  Phase 4 — Multi-row CDC (D5)
  Phase 5 — TOAST (D4) — CRITICAL
  Phase 6 — NULL roundtrip (C10)
  Phase 7 — Delta verification (B2)
  Phase 8 — Idempotency (H1)

Each phase leaves the source in a state that subsequent phases don't
depend on, so individual checks can be skipped via --skip.
"""

from __future__ import annotations

import time
from typing import Any

from ..backends.base import TargetBackend
from ..source.base import SourceClient
from ..tui.report import CheckResult, CheckStatus
from .common import TestContext, wait_until, wait_until_with_value


# ── Constants ────────────────────────────────────────────────────────────────

# Magic marker in customer_id to identify test-generated rows. Large enough
# to not collide with the seed (random 1-1000).
_TEST_CUSTOMER_ID = 99_999

# Status values used by CDC checks. MUST be <= 20 chars to fit in
# orders.status VARCHAR(20). Keep them short and unique per check.
_STATUS_D1      = "e2e_d1_ins"
_STATUS_D2      = "e2e_d2_upd"
_STATUS_E1_MID  = "e2e_e1_mid"
_STATUS_E1_END  = "e2e_e1_end"
_STATUS_D4_INI  = "e2e_d4_ini"
_STATUS_D4_UPD  = "e2e_d4_upd"
_STATUS_C10     = "e2e_c10"

# TEXT column we ensure exists on orders for the D4 TOAST test. NULL by
# default in the seed (doesn't interfere with demos).
_TOAST_COLUMN = "description"
_TOAST_COLUMN_TYPE = "TEXT"

# 9 KB of text — comfortably above the default TOAST_TUPLE_THRESHOLD (2 KB)
# and the default TOAST_TUPLE_TARGET, so PG will definitely TOAST the value.
_TOAST_BIG_TEXT = "X" * 9216
_TOAST_BIG_TEXT_MARKER = "DBMAZZ_TOAST_TEST_MARKER"


def _wait_until_in_target(
    target: TargetBackend,
    table: str,
    pk_col: str,
    pk_value: Any,
    timeout: float,
) -> None:
    """Wait until a row with the given PK appears (is_live) in the target."""
    wait_until(
        lambda: target.row_is_live(table, pk_col, pk_value),
        timeout=timeout,
        description=f"row {pk_value} in {target.name}.{table}",
    )


def _wait_until_absent_in_target(
    target: TargetBackend,
    table: str,
    pk_col: str,
    pk_value: Any,
    timeout: float,
) -> None:
    """Wait until a row with the given PK disappears (is not live) in the target."""
    wait_until(
        lambda: not target.row_is_live(table, pk_col, pk_value),
        timeout=timeout,
        description=f"row {pk_value} removed from {target.name}.{table}",
    )


# ── Phase 1: Schema ──────────────────────────────────────────────────────────

def check_a1_target_tables_present(ctx: TestContext) -> CheckResult:
    expected = list(ctx.profile.tables)
    actual = set(ctx.target.list_tables())
    missing = [t for t in expected if t not in actual]
    if missing:
        return CheckResult(
            id="A1",
            description="Target tables present",
            status=CheckStatus.FAIL,
            error=f"missing: {', '.join(missing)}",
        )
    return CheckResult(
        id="A1",
        description="Target tables present",
        status=CheckStatus.PASS,
        detail=f"{len(expected)}/{len(expected)}",
    )


def check_a2_source_columns_in_target(ctx: TestContext) -> CheckResult:
    """For each replicated table, every source column should exist in the target.

    Column names are matched case-insensitively because Snowflake upper-cases
    unquoted identifiers and StarRocks/Postgres preserve the source case.
    """
    missing_by_table: dict[str, list[str]] = {}
    total_cols = 0
    matched_cols = 0

    for table in ctx.profile.tables:
        # Read source column names via information_schema.
        # We use the source client's low-level connection directly for this
        # because adding a list_columns method to SourceClient just for one
        # check feels like ABI bloat.
        source_cols = _list_source_columns(ctx.source, table)
        target_cols = {c.name.lower() for c in ctx.target.get_columns(table)}

        total_cols += len(source_cols)
        for col in source_cols:
            if col.lower() in target_cols:
                matched_cols += 1
            else:
                missing_by_table.setdefault(table, []).append(col)

    if missing_by_table:
        details = "; ".join(f"{t}: {', '.join(cols)}" for t, cols in missing_by_table.items())
        return CheckResult(
            id="A2",
            description="Source columns in target",
            status=CheckStatus.FAIL,
            error=f"missing columns — {details}",
        )

    return CheckResult(
        id="A2",
        description="Source columns in target",
        status=CheckStatus.PASS,
        detail=f"{matched_cols}/{total_cols}",
    )


def check_a3_audit_columns_present(ctx: TestContext) -> CheckResult:
    """Verify that each target table has the audit columns this sink expects."""
    expected_cols = ctx.target.expected_audit_columns()
    if not expected_cols:
        return CheckResult(
            id="A3",
            description="Audit columns present",
            status=CheckStatus.SKIP,
            detail="no audit columns declared by this sink",
        )

    missing_by_table: dict[str, list[str]] = {}
    for table in ctx.profile.tables:
        target_cols = {c.name.lower() for c in ctx.target.get_columns(table)}
        for col in expected_cols:
            if col.lower() not in target_cols:
                missing_by_table.setdefault(table, []).append(col)

    if missing_by_table:
        details = "; ".join(f"{t}: {', '.join(cols)}" for t, cols in missing_by_table.items())
        return CheckResult(
            id="A3",
            description="Audit columns present",
            status=CheckStatus.FAIL,
            error=f"missing — {details}",
        )

    return CheckResult(
        id="A3",
        description="Audit columns present",
        status=CheckStatus.PASS,
        detail=f"{len(expected_cols)} cols × {len(ctx.profile.tables)} tables",
    )


def check_a4_metadata_table(ctx: TestContext) -> CheckResult:
    """Verify the sink's metadata table has at least one row.

    Skipped for sinks that don't maintain a metadata table (e.g., StarRocks).
    """
    if not ctx.target.capabilities.has_metadata_table:
        return CheckResult(
            id="A4",
            description="Metadata table has rows",
            status=CheckStatus.SKIP,
            detail=f"{ctx.target.name} has no metadata table",
        )

    try:
        count = ctx.target.metadata_row_count()
    except Exception as e:
        return CheckResult(
            id="A4",
            description="Metadata table has rows",
            status=CheckStatus.FAIL,
            error=str(e),
        )

    if count < 1:
        return CheckResult(
            id="A4",
            description="Metadata table has rows",
            status=CheckStatus.FAIL,
            error=f"metadata row count is {count}",
        )

    return CheckResult(
        id="A4",
        description="Metadata table has rows",
        status=CheckStatus.PASS,
        detail=f"{count} row{'s' if count != 1 else ''}",
    )


# ── Phase 2: Snapshot baseline ───────────────────────────────────────────────

def check_b1_snapshot_counts(ctx: TestContext) -> CheckResult:
    """Verify that post-snapshot, source and target counts match per table.

    Stores baselines in ctx.scratch['baseline_counts'] for B2 to use later.
    """
    # Settle first — snapshot may just have finished; normalizer async flush.
    time.sleep(ctx.target.capabilities.post_snapshot_settle_seconds)

    baselines: dict[str, int] = {}
    mismatches: list[str] = []

    for table in ctx.profile.tables:
        src_count = ctx.source.count_rows(table)
        tgt_count = ctx.target.count_rows(table)
        baselines[table] = src_count
        if tgt_count < src_count:
            mismatches.append(f"{table}: source={src_count} target={tgt_count}")

    ctx.scratch["baseline_counts"] = baselines

    if mismatches:
        return CheckResult(
            id="B1",
            description="Snapshot counts match",
            status=CheckStatus.FAIL,
            error="; ".join(mismatches),
        )

    detail_parts = [f"{t}={baselines[t]}" for t in baselines]
    return CheckResult(
        id="B1",
        description="Snapshot counts match",
        status=CheckStatus.PASS,
        detail=", ".join(detail_parts),
    )


def check_b3_no_duplicates(ctx: TestContext) -> CheckResult:
    """Verify that no PK appears more than once in the target.

    The snapshot↔CDC boundary is the historical source of duplicates —
    if dedup (should_emit) fails, a row can be snapshot-ed and then also
    emitted as a CDC event. This check should return 0 duplicates.
    """
    dupes_by_table: dict[str, int] = {}
    for table in ctx.profile.tables:
        dupes = ctx.target.count_duplicates_by_pk(table, pk_column="id")
        if dupes > 0:
            dupes_by_table[table] = dupes

    if dupes_by_table:
        details = ", ".join(f"{t}={n}" for t, n in dupes_by_table.items())
        return CheckResult(
            id="B3",
            description="Zero duplicates by PK",
            status=CheckStatus.FAIL,
            error=details,
        )
    return CheckResult(
        id="B3",
        description="Zero duplicates by PK",
        status=CheckStatus.PASS,
        detail="0",
    )


# ── Phase 3: Single-row CDC flow ─────────────────────────────────────────────

def run_single_row_cdc_flow(ctx: TestContext) -> list[CheckResult]:
    """Apply INSERT → UPDATE → UPDATE → DELETE to one row and verify each step.

    Runs as a single multi-check phase because the checks share a row id.
    Yields four CheckResults: D1, D2, E1, D3.
    """
    results: list[CheckResult] = []
    settle = ctx.target.capabilities.post_cdc_settle_seconds

    # D1: INSERT
    t0 = time.time()
    pk = ctx.source.insert_row(
        "orders",
        {
            "customer_id": _TEST_CUSTOMER_ID,
            "total": 123.45,
            "status": _STATUS_D1,
        },
    )
    try:
        _wait_until_in_target(ctx.target, "orders", "id", pk, timeout=settle + 15)
        dt_ms = int((time.time() - t0) * 1000)
        results.append(CheckResult(
            id="D1",
            description="INSERT replicated",
            status=CheckStatus.PASS,
            detail=f"{dt_ms} ms",
            duration_ms=dt_ms,
        ))
    except TimeoutError as e:
        results.append(CheckResult(
            id="D1",
            description="INSERT replicated",
            status=CheckStatus.FAIL,
            error=str(e),
        ))
        # If INSERT didn't replicate, the rest of this phase can't run meaningfully.
        results.extend([
            CheckResult(id=cid, description=desc, status=CheckStatus.SKIP,
                        detail="skipped: D1 failed")
            for cid, desc in [
                ("D2", "UPDATE replicated"),
                ("E1", "Sequential UPDATEs, last wins"),
                ("D3", "DELETE replicated"),
            ]
        ])
        return results

    # D2: UPDATE
    t0 = time.time()
    ctx.source.update_row("orders", "id", pk, {"status": _STATUS_D2})
    try:
        wait_until(
            lambda: ctx.target.fetch_value("orders", "id", pk, "status") == _STATUS_D2,
            timeout=settle + 15,
            description=f"status={_STATUS_D2} in {ctx.target.name}.orders",
        )
        dt_ms = int((time.time() - t0) * 1000)
        results.append(CheckResult(
            id="D2",
            description="UPDATE replicated",
            status=CheckStatus.PASS,
            detail=f"{dt_ms} ms",
            duration_ms=dt_ms,
        ))
    except TimeoutError as e:
        results.append(CheckResult(
            id="D2",
            description="UPDATE replicated",
            status=CheckStatus.FAIL,
            error=str(e),
        ))

    # E1: Sequential UPDATEs, last wins
    # Two quick updates — the last one should be what ends up in the target.
    ctx.source.update_row("orders", "id", pk, {"status": _STATUS_E1_MID})
    time.sleep(0.05)  # tiny gap to distinguish the updates in the WAL
    ctx.source.update_row("orders", "id", pk, {"status": _STATUS_E1_END})
    try:
        wait_until(
            lambda: ctx.target.fetch_value("orders", "id", pk, "status") == _STATUS_E1_END,
            timeout=settle + 15,
            description="final status after sequential updates",
        )
        results.append(CheckResult(
            id="E1",
            description="Sequential UPDATEs, last wins",
            status=CheckStatus.PASS,
            detail="last wins",
        ))
    except TimeoutError as e:
        results.append(CheckResult(
            id="E1",
            description="Sequential UPDATEs, last wins",
            status=CheckStatus.FAIL,
            error=str(e),
        ))

    # D3: DELETE
    t0 = time.time()
    ctx.source.delete_row("orders", "id", pk)
    try:
        _wait_until_absent_in_target(ctx.target, "orders", "id", pk, timeout=settle + 15)
        dt_ms = int((time.time() - t0) * 1000)
        results.append(CheckResult(
            id="D3",
            description="DELETE replicated",
            status=CheckStatus.PASS,
            detail=f"{dt_ms} ms",
            duration_ms=dt_ms,
        ))
    except TimeoutError as e:
        results.append(CheckResult(
            id="D3",
            description="DELETE replicated",
            status=CheckStatus.FAIL,
            error=str(e),
        ))

    return results


# ── Phase 4: Multi-row CDC ───────────────────────────────────────────────────

def check_d5_multi_row_insert(ctx: TestContext) -> CheckResult:
    """Insert 10 rows in a single transaction and verify all appear."""
    rows = [
        {"customer_id": _TEST_CUSTOMER_ID, "total": 10.0 + i, "status": f"e2e_d5_{i}"}
        for i in range(10)
    ]
    pks = ctx.source.insert_many("orders", rows)

    settle = ctx.target.capabilities.post_cdc_settle_seconds
    missing: list[Any] = []

    try:
        wait_until(
            lambda: all(ctx.target.row_is_live("orders", "id", pk) for pk in pks),
            timeout=settle + 20,
            description="all 10 rows replicated",
        )
    except TimeoutError:
        missing = [pk for pk in pks if not ctx.target.row_is_live("orders", "id", pk)]

    # Cleanup: delete the test rows so we don't pollute B2 baseline math.
    for pk in pks:
        try:
            ctx.source.delete_row("orders", "id", pk)
        except Exception:
            pass
    # Wait for the deletes to propagate before returning.
    time.sleep(settle + 1)

    if missing:
        return CheckResult(
            id="D5",
            description="Multi-row INSERT in single TX",
            status=CheckStatus.FAIL,
            error=f"{len(missing)}/{len(pks)} rows not replicated",
        )
    return CheckResult(
        id="D5",
        description="Multi-row INSERT in single TX",
        status=CheckStatus.PASS,
        detail=f"{len(pks)}/{len(pks)}",
    )


# ── Phase 5: TOAST (D4) — CRITICAL ───────────────────────────────────────────

def check_d4_toast_update(ctx: TestContext) -> CheckResult:
    """Verify that updating an unrelated column doesn't drop TOAST'd values.

    This is the historical bug #1 in PG CDCs: when PG stores a TOAST'd value
    and you UPDATE a different column, the TOAST column is marked 'unchanged'
    in the pgoutput stream. If the CDC tool doesn't resolve the unchanged
    marker correctly, the target ends up with NULL or garbage for that column.

    Test procedure:
      1. Ensure the orders.description column exists (ALTER ADD COLUMN IF NOT EXISTS).
      2. Insert a row with a 9 KB description (above TOAST threshold).
      3. Wait for replication — verify the big description is in the target intact.
      4. UPDATE the row's status (NOT the description).
      5. Wait for replication — verify the description in the target is STILL intact.
      6. Cleanup.
    """
    # Step 1: ensure the column exists
    try:
        ctx.source.ensure_column("orders", _TOAST_COLUMN, _TOAST_COLUMN_TYPE)
    except Exception as e:
        return CheckResult(
            id="D4",
            description="TOAST UPDATE preserves unchanged value",
            status=CheckStatus.FAIL,
            error=f"failed to add {_TOAST_COLUMN} column: {e}",
        )

    # Give dbmazz a moment to observe the schema change (A5 is tier 2, but
    # we need the column to propagate for D4 to even work here).
    time.sleep(2.0)

    settle = ctx.target.capabilities.post_cdc_settle_seconds
    big_text = _TOAST_BIG_TEXT_MARKER + _TOAST_BIG_TEXT

    # Step 2: insert row with big description
    pk = ctx.source.insert_row(
        "orders",
        {
            "customer_id": _TEST_CUSTOMER_ID,
            "total": 777.77,
            "status": _STATUS_D4_INI,
            _TOAST_COLUMN: big_text,
        },
    )

    # Step 3: wait and verify the big text arrived in target
    try:
        wait_until(
            lambda: ctx.target.row_is_live("orders", "id", pk),
            timeout=settle + 20,
            description="TOAST insert row in target",
        )
    except TimeoutError as e:
        _cleanup_pk(ctx, pk)
        return CheckResult(
            id="D4",
            description="TOAST UPDATE preserves unchanged value",
            status=CheckStatus.FAIL,
            error=f"initial insert not replicated: {e}",
        )

    # Verify the large text is intact BEFORE the update
    initial_desc = ctx.target.fetch_value("orders", "id", pk, _TOAST_COLUMN)
    initial_desc_str = str(initial_desc) if initial_desc is not None else ""
    if _TOAST_BIG_TEXT_MARKER not in initial_desc_str or len(initial_desc_str) < len(big_text):
        _cleanup_pk(ctx, pk)
        return CheckResult(
            id="D4",
            description="TOAST UPDATE preserves unchanged value",
            status=CheckStatus.FAIL,
            error=(
                f"initial insert lost content: "
                f"target has {len(initial_desc_str)} bytes, expected {len(big_text)}"
            ),
        )

    # Step 4: UPDATE a different column — NOT the description
    ctx.source.update_row("orders", "id", pk, {"status": _STATUS_D4_UPD})

    # Step 5: wait for the status change to propagate, then check description
    try:
        wait_until(
            lambda: ctx.target.fetch_value("orders", "id", pk, "status") == _STATUS_D4_UPD,
            timeout=settle + 15,
            description="status update replicated",
        )
    except TimeoutError as e:
        _cleanup_pk(ctx, pk)
        return CheckResult(
            id="D4",
            description="TOAST UPDATE preserves unchanged value",
            status=CheckStatus.FAIL,
            error=f"status update not replicated: {e}",
        )

    # The critical assertion: description must STILL equal the original big text
    # after updating a different column. If dbmazz mishandles the TOAST 'unchanged'
    # marker, this is where it shows up — the description becomes NULL or empty.
    post_update_desc = ctx.target.fetch_value("orders", "id", pk, _TOAST_COLUMN)
    post_update_desc_str = str(post_update_desc) if post_update_desc is not None else ""

    _cleanup_pk(ctx, pk)

    if post_update_desc_str != big_text:
        return CheckResult(
            id="D4",
            description="TOAST UPDATE preserves unchanged value",
            status=CheckStatus.FAIL,
            error=(
                f"TOAST column was lost during unrelated UPDATE: "
                f"target has {len(post_update_desc_str)} bytes, expected {len(big_text)}"
            ),
        )

    return CheckResult(
        id="D4",
        description="TOAST UPDATE preserves unchanged value",
        status=CheckStatus.PASS,
        detail=f"{len(big_text)/1024:.1f} KB text intact",
    )


def _cleanup_pk(ctx: TestContext, pk: Any) -> None:
    """Best-effort delete of a test row. Ignores errors."""
    try:
        ctx.source.delete_row("orders", "id", pk)
    except Exception:
        pass
    # Give the DELETE a moment to propagate.
    time.sleep(ctx.target.capabilities.post_cdc_settle_seconds + 1)


# ── Phase 6: NULL roundtrip (C10) ────────────────────────────────────────────

def check_c10_null_roundtrip(ctx: TestContext) -> CheckResult:
    """Insert a row with NULLs in nullable columns and verify they arrive as NULL."""
    # orders.description is the one column we know is nullable (added by D4 phase,
    # if that phase ran). If it hasn't been added yet, add it now.
    try:
        ctx.source.ensure_column("orders", _TOAST_COLUMN, _TOAST_COLUMN_TYPE)
    except Exception as e:
        return CheckResult(
            id="C10",
            description="NULL roundtrip",
            status=CheckStatus.FAIL,
            error=f"failed to ensure {_TOAST_COLUMN}: {e}",
        )

    pk = ctx.source.insert_row(
        "orders",
        {
            "customer_id": _TEST_CUSTOMER_ID,
            "total": 0.01,
            "status": _STATUS_C10,
            _TOAST_COLUMN: None,  # explicit NULL
        },
    )

    settle = ctx.target.capabilities.post_cdc_settle_seconds
    try:
        wait_until(
            lambda: ctx.target.row_is_live("orders", "id", pk),
            timeout=settle + 15,
            description="C10 row in target",
        )
    except TimeoutError as e:
        _cleanup_pk(ctx, pk)
        return CheckResult(
            id="C10",
            description="NULL roundtrip",
            status=CheckStatus.FAIL,
            error=f"row not replicated: {e}",
        )

    # Fetch the description value — should be None (NULL).
    desc = ctx.target.fetch_value("orders", "id", pk, _TOAST_COLUMN)
    _cleanup_pk(ctx, pk)

    if desc is not None:
        return CheckResult(
            id="C10",
            description="NULL roundtrip",
            status=CheckStatus.FAIL,
            error=f"NULL column arrived as {desc!r}",
        )

    return CheckResult(
        id="C10",
        description="NULL roundtrip",
        status=CheckStatus.PASS,
        detail="NULL preserved",
    )


# ── Phase 7: Delta verification (B2) ─────────────────────────────────────────

def check_b2_post_cdc_delta(ctx: TestContext) -> CheckResult:
    """Verify that after all the CDC ops, source and target still agree.

    All the previous phases cleaned up after themselves (deleted their test
    rows), so the final counts should equal the baselines captured in B1.
    If they don't, it means we either lost events or leaked test rows.
    """
    baselines: dict[str, int] | None = ctx.scratch.get("baseline_counts")
    if not baselines:
        return CheckResult(
            id="B2",
            description="Post-CDC delta matches",
            status=CheckStatus.SKIP,
            detail="B1 baseline not captured",
        )

    # Wait for any in-flight CDC ops to settle before counting.
    time.sleep(ctx.target.capabilities.post_cdc_settle_seconds + 1)

    mismatches: list[str] = []
    for table, baseline in baselines.items():
        src_count = ctx.source.count_rows(table)
        tgt_count = ctx.target.count_rows(table)
        if src_count != tgt_count:
            mismatches.append(f"{table}: source={src_count} target={tgt_count}")

    if mismatches:
        return CheckResult(
            id="B2",
            description="Post-CDC delta matches",
            status=CheckStatus.FAIL,
            error="; ".join(mismatches),
        )
    return CheckResult(
        id="B2",
        description="Post-CDC delta matches",
        status=CheckStatus.PASS,
        detail="source = target",
    )


# ── Phase 8: Idempotency (H1) ────────────────────────────────────────────────

def check_h1_no_op_idempotent(ctx: TestContext) -> CheckResult:
    """With no traffic, counts should not change over a short window.

    PR 1 implements the weaker variant of H1 (no actual dbmazz restart).
    The full restart-based idempotency test lives in the roadmap (plan §11 H2).
    """
    counts_before = {t: ctx.target.count_rows(t) for t in ctx.profile.tables}
    time.sleep(ctx.target.capabilities.post_cdc_settle_seconds + 1)
    counts_after = {t: ctx.target.count_rows(t) for t in ctx.profile.tables}

    drift: list[str] = []
    for table, before in counts_before.items():
        after = counts_after[table]
        if after != before:
            drift.append(f"{table}: {before} → {after}")

    if drift:
        return CheckResult(
            id="H1",
            description="Restart without traffic is no-op",
            status=CheckStatus.FAIL,
            error="counts drifted: " + "; ".join(drift),
        )
    return CheckResult(
        id="H1",
        description="Restart without traffic is no-op",
        status=CheckStatus.PASS,
        detail="no drift",
    )


# ── Source column listing helper ─────────────────────────────────────────────

def _list_source_columns(source: SourceClient, table: str) -> list[str]:
    """List column names of a table in the source.

    Adding a `list_columns` method to the SourceClient ABC just for one
    check would be bloat, so we reach into the psycopg2 connection directly.
    If future source types are added, this helper should move to the ABC.
    """
    # All current sources are PostgresSource. Access the underlying conn.
    pg_source = source  # type: ignore
    conn = pg_source._require_conn()  # type: ignore[attr-defined]
    sql = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table,))
        return [r[0] for r in cur.fetchall()]
