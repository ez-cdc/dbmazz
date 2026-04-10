"""Bundled demo datasources factory.

Provides ready-to-use datasources that work out of the box without any
credentials from the user. Used by:

  - `ez-cdc datasource init-demos` (CLI subcommand)
  - The "Use bundled demo datasources" option in the first-run menu

The bundled demos are intentionally minimal:

  - demo-pg          — managed PostgreSQL source with the orders+order_items
                       seed (the same schema PR 1 uses for fixtures)
  - demo-starrocks   — managed StarRocks sink
  - demo-pg-target   — managed PostgreSQL sink

These three datasources cover the same surface as the old `starrocks` and
`pg-target` profiles. They form the matrix:

  demo-pg → demo-starrocks    (= old starrocks profile)
  demo-pg → demo-pg-target    (= old pg-target profile)

**Snowflake is NOT included in the demos.** Snowflake requires real
credentials and cloud connectivity, so a "no-config quickstart" can't
ship a working snowflake datasource. Users who want Snowflake should run
`ez-cdc datasource add` and pick "snowflake" in the wizard, which will
prompt for their account credentials interactively. Or they can copy
the example block from datasources.example.yaml and edit by hand.
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

    All demos are `managed: true`, meaning ez-cdc spins them up in Docker
    containers when a flow needs them. None require user credentials.
    """
    return DatasourcesFile(
        sources={
            "demo-pg": PostgresSourceSpec(
                managed=True,
                seed="postgres-seed.sql",
                tables=["orders", "order_items"],
                # slot/publication names use the defaults: dbmazz_slot, dbmazz_pub
            ),
        },
        sinks={
            "demo-starrocks": StarRocksSinkSpec(
                managed=True,
                # connection details are filled in by the compose builder
                # at runtime when this sink is selected — managed sinks
                # don't need them in the spec
            ),
            "demo-pg-target": PostgresSinkSpec(
                managed=True,
                # database name and schema also filled by compose builder
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
