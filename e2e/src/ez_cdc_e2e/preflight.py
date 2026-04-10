"""Pre-flight validation: connectivity and schema checks.

Run before quickstart/verify to catch configuration issues early, before
the compose stack or dbmazz daemon starts. Validates that:

  1. Source and sink are reachable (check_connectivity).
  2. Target schema is compatible with the source (check_schema).

If the target has no tables at all, schema check passes — dbmazz will
create them during the initial snapshot.
"""

from __future__ import annotations

from rich.console import Console
from rich.text import Text

from .datasources.schema import PostgresSourceSpec, SinkSpec, SourceSpec
from .instantiate import instantiate_backend_from_spec, instantiate_source_from_spec


# ---------------------------------------------------------------------------
# Connectivity
# ---------------------------------------------------------------------------

def check_connectivity(
    src_spec: SourceSpec,
    sk_spec: SinkSpec,
    console: Console,
    *,
    src_name: str = "source",
    sk_name: str = "sink",
) -> bool:
    """Try to connect to both source and sink. Returns True if both succeed."""
    source = None
    target = None
    src_ok = False
    sk_ok = False

    try:
        # -- source ----------------------------------------------------------
        try:
            source = instantiate_source_from_spec(src_spec)
            source.connect()
            src_ok = True
            console.print(Text(f"  Checking source ({src_name})... ✓ Connected", style="success"))
        except Exception as exc:
            console.print(Text(f"  Checking source ({src_name})... ✗ {exc}", style="error"))

        # -- sink ------------------------------------------------------------
        try:
            target = instantiate_backend_from_spec(sk_spec)
            target.connect()
            sk_ok = True
            console.print(Text(f"  Checking sink ({sk_name})... ✓ Connected", style="success"))
        except Exception as exc:
            console.print(Text(f"  Checking sink ({sk_name})... ✗ {exc}", style="error"))

    finally:
        if source is not None:
            try:
                source.close()
            except Exception:
                pass
        if target is not None:
            try:
                target.close()
            except Exception:
                pass

    return src_ok and sk_ok


# ---------------------------------------------------------------------------
# Schema compatibility
# ---------------------------------------------------------------------------

def check_schema(src_spec: SourceSpec, sk_spec: SinkSpec, console: Console) -> bool:
    """Compare source tables/columns against the target.

    If the target has no tables at all we treat it as a fresh target and
    return True — dbmazz will create the tables during snapshot.

    If connection fails (e.g. connectivity already reported by
    check_connectivity), returns False without crashing.
    """
    source = None
    target = None

    try:
        source = instantiate_source_from_spec(src_spec)
        source.connect()

        target = instantiate_backend_from_spec(sk_spec)
        target.connect()
    except Exception as exc:
        console.print(Text(f"  Schema check skipped — cannot connect: {exc}", style="warning"))
        return False
    finally:
        # Only close on early-exit failure; success path closes below.
        pass

    try:
        target_tables = [t.lower() for t in target.list_tables()]

        # Fresh target — nothing to compare.
        if not target_tables:
            console.print(Text("  Target has no tables (fresh) — schema check skipped", style="muted"))
            return True

        all_ok = True
        tables: list[str] = getattr(src_spec, "tables", [])

        for table in tables:
            if table.lower() not in target_tables:
                console.print(Text(f'  ✗ Table "{table}" exists in source but not in target', style="error"))
                console.print(Text(
                    "\n  For replication to work, the target must have the same tables and columns\n"
                    "  as the source. If using demo datasources, run `ez-cdc up` first — the\n"
                    "  demo DBs come pre-seeded with matching schemas.\n",
                    style="muted",
                ))
                all_ok = False
                continue

            # Source columns via information_schema.
            if not isinstance(src_spec, PostgresSourceSpec):
                continue

            conn = source._require_conn()  # type: ignore[union-attr]
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (table,),
                )
                source_cols = [row[0].lower() for row in cur.fetchall()]

            target_cols = [c.name.lower() for c in target.get_columns(table)]

            missing = [c for c in source_cols if c not in target_cols]
            if missing:
                console.print(Text(
                    f'  ✗ Table "{table}": missing columns in target: {", ".join(missing)}',
                    style="error",
                ))
                console.print(Text(
                    "\n  For replication to work, the target must have the same tables and columns\n"
                    "  as the source. If using demo datasources, run `ez-cdc up` first — the\n"
                    "  demo DBs come pre-seeded with matching schemas.\n",
                    style="muted",
                ))
                all_ok = False
            else:
                console.print(Text(f'  ✓ Table "{table}" — columns match', style="success"))

        return all_ok

    except Exception as exc:
        console.print(Text(f"  Schema check failed: {exc}", style="error"))
        return False

    finally:
        if source is not None:
            try:
                source.close()
            except Exception:
                pass
        if target is not None:
            try:
                target.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Combined preflight
# ---------------------------------------------------------------------------

def run_preflight(
    src_spec: SourceSpec,
    sk_spec: SinkSpec,
    console: Console,
    *,
    src_name: str = "source",
    sk_name: str = "sink",
) -> bool:
    """Run connectivity then schema checks. Returns True only if both pass."""
    if not check_connectivity(src_spec, sk_spec, console, src_name=src_name, sk_name=sk_name):
        return False
    return check_schema(src_spec, sk_spec, console)
