"""Bundled demo datasources factory.

Provides ready-to-use datasources that work out of the box without any
credentials from the user. Used by:

  - ``ez-cdc datasource init-demos`` (CLI subcommand)
  - The "Init config" option in the first-run menu

The bundled demos are intentionally minimal:

  - demo-pg          — PostgreSQL source on localhost:15432 with
                       the orders+order_items seed
  - demo-starrocks   — StarRocks sink on localhost:18030 / 19030
  - demo-pg-target   — PostgreSQL sink on localhost:25432

All demos carry explicit connection details that match the Docker
containers created by ``ez-cdc up``.  The compose builder detects
these well-known localhost ports and creates the appropriate containers.

**Snowflake is NOT included in the demos.** Snowflake requires real
credentials and cloud connectivity, so a "no-config quickstart" can't
ship a working snowflake datasource. Users should run
``ez-cdc datasource add`` or edit ez-cdc.yaml manually.
"""

from __future__ import annotations

from .schema import (
    DatasourcesFile,
    PostgresSinkSpec,
    PostgresSourceSpec,
    StarRocksSinkSpec,
)


def build_demo_datasources() -> DatasourcesFile:
    """Return a DatasourcesFile populated with the 3 bundled demos.

    The returned file is in-memory only — the caller is responsible for
    persisting it via DatasourceStore.save() or merging it into an
    existing store.

    All demos use localhost URLs with well-known ports that match the
    Docker containers created by ``ez-cdc up``.
    """
    return DatasourcesFile(
        sources={
            "demo-pg": PostgresSourceSpec(
                url="postgres://postgres:postgres@localhost:15432/dbmazz",
                seed="postgres-seed.sql",
                tables=["orders", "order_items"],
            ),
        },
        sinks={
            "demo-starrocks": StarRocksSinkSpec(
                url="http://localhost:18030",
                mysql_port=19030,
                database="dbmazz",
                user="root",
                password="",
            ),
            "demo-pg-target": PostgresSinkSpec(
                url="postgres://postgres:postgres@localhost:25432/dbmazz_target",
                database="dbmazz_target",
                schema="public",
            ),
        },
    )


def merge_demos_into(store) -> tuple[list[str], list[str]]:
    """Merge bundled demos into an existing DatasourceStore.

    Existing datasources with the same name are skipped (not overwritten),
    so re-running `init-demos` is idempotent and won't clobber any user-edited
    demo entries.

    Returns:
        (added_sources, added_sinks): lists of names that were actually
        added (i.e., not already present in the store).
    """
    demos = build_demo_datasources()

    added_sources: list[str] = []
    for name, spec in demos.sources.items():
        if not store.exists(name):
            store.add_source(name, spec)
            added_sources.append(name)

    added_sinks: list[str] = []
    for name, spec in demos.sinks.items():
        if not store.exists(name):
            store.add_sink(name, spec)
            added_sinks.append(name)

    return (added_sources, added_sinks)
