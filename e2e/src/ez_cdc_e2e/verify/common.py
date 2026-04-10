"""Common helpers for verify checks: polling, context dataclass, prechecks."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from rich.console import Console

from ..backends.base import TargetBackend
from ..dbmazz import DbmazzClient
from ..source.base import SourceClient


class PrecheckError(Exception):
    """Raised when a precondition for running verify checks isn't met."""


@dataclass
class TestContext:
    """Shared state for a verify run.

    Passed to every check function. The `scratch` dict is a free-form
    place for checks to share state across each other (e.g., B1 stores
    the baseline counts, B2 reads them).

    PR 4 reshape: instead of holding a `profile: ProfileSpec`, this dataclass
    now carries the bare minimum the checks need: tables, source name (for
    error messages), and a flag for whether the source is managed by us or
    user-provided. The latter drives the read-only/full mode decision in
    PR 5; today the field is set but unused by the tier 1 checks.
    """
    source: SourceClient
    target: TargetBackend
    dbmazz: DbmazzClient
    console: Console
    tables: tuple[str, ...]
    source_name: str = "source"
    sink_name: str = "sink"
    source_managed: bool = True
    scratch: dict[str, Any] = field(default_factory=dict)


def wait_until(
    predicate: Callable[[], bool],
    timeout: float,
    poll_interval: float = 0.5,
    description: str = "condition",
) -> None:
    """Poll `predicate` until it returns True or timeout expires.

    Raises TimeoutError if the predicate never returns True. Any exception
    from the predicate is swallowed (treated as "not yet") so transient
    connection errors don't fail the whole check.
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            if predicate():
                return
        except Exception:
            pass  # transient — retry
        time.sleep(poll_interval)
    elapsed = time.time() - start
    raise TimeoutError(f"timed out waiting for {description} after {elapsed:.1f}s")


def wait_until_with_value(
    fetch: Callable[[], Any],
    expected: Any,
    timeout: float,
    poll_interval: float = 0.5,
    description: str = "value",
) -> Any:
    """Poll `fetch` until it returns `expected` (equality check).

    Returns the value when it matches. Raises TimeoutError otherwise.
    """
    def _predicate() -> bool:
        nonlocal last
        last = fetch()
        return last == expected

    last: Any = None
    wait_until(_predicate, timeout, poll_interval, description)
    return last


def precheck(ctx: TestContext) -> None:
    """Verify preconditions before running any checks.

    Raises PrecheckError with a human-readable message if anything is wrong.
    """
    # dbmazz healthy
    if not ctx.dbmazz.health():
        raise PrecheckError(
            "dbmazz daemon is not healthy. Is the compose stack up?"
        )

    # Source reachable
    if not ctx.tables:
        raise PrecheckError("source has no tables declared in the datasource spec")
    try:
        _ = ctx.source.count_rows(ctx.tables[0])
    except Exception as e:
        raise PrecheckError(f"Source database unreachable: {e}") from e

    # Target reachable
    try:
        target_tables = ctx.target.list_tables()
    except Exception as e:
        raise PrecheckError(f"Target {ctx.target.name} unreachable: {e}") from e

    # Target must be clean — stale data from a previous run will pollute results.
    # We only check tables that are in ctx.tables (the ones being replicated).
    dirty_tables: list[str] = []
    for table in ctx.tables:
        if table in target_tables:
            try:
                count = ctx.target.count_rows(table, exclude_deleted=False)
                if count > 0:
                    dirty_tables.append(f"{table} ({count} rows)")
            except Exception:
                pass  # table might exist but be inaccessible; A1 will catch that
    if dirty_tables:
        raise PrecheckError(
            f"Target has stale data from a previous run — clean it before verify.\n"
            f"  Dirty tables: {', '.join(dirty_tables)}\n"
            f"  For managed sinks, run: ez-cdc down --source <SRC> --sink <SINK>\n"
            f"  Then start fresh:       ez-cdc verify --source <SRC> --sink <SINK>"
        )
