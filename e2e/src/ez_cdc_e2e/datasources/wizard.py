"""Interactive `datasource add` wizard.

Drives the user through creating a new datasource entry. Used by:

  - `ez-cdc datasource add` (CLI subcommand)
  - The first-run flow in the main menu when the user picks "Configure
    datasources now" instead of "Use bundled demos"

Flow:

  1. Ask for a name (validated against the schema's name pattern, checked
     for collisions in the store).
  2. Ask for the role (source or sink).
  3. Ask for the type (postgres / starrocks / snowflake — sinks only).
  4. Type-specific connection prompts. All specs require explicit
     connection details; a "Use demo defaults?" shortcut pre-fills
     localhost URLs for quick local testing. For Postgres sources, this
     includes a connection test and an interactive table picker via
     pg_tables.
  5. Save to the store and persist to disk.

Cancellation handling:
  Any prompt can be cancelled with Ctrl+C / Esc. We catch _Cancelled
  and return to the caller without saving. The store is mutated only
  in the very last step (success), so a mid-wizard cancel never leaves
  partial state.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

from rich.console import Console
from rich.text import Text

from ..tui import prompts
from .loader import DatasourceError
from .schema import (
    PostgresSinkSpec,
    PostgresSourceSpec,
    SinkSpec,
    SnowflakeSinkSpec,
    SourceSpec,
    StarRocksSinkSpec,
    _NAME_PATTERN,
)
from .store import DatasourceStore


# Sentinel raised by helper prompts when the user cancels. Mirrors the
# _BackToMenu pattern from cli.py without importing it (avoids cycles).
class _Cancelled(Exception):
    pass


# ── Public entry point ──────────────────────────────────────────────────────

def run_add_wizard(store: DatasourceStore, console: Console) -> Optional[str]:
    """Run the interactive add wizard against `store`.

    Returns the name of the created datasource on success, or None if the
    user cancelled at any point. The store is saved to disk on success.
    """
    console.print()
    console.print(Text(
        "  Add a new datasource. Press Ctrl+C at any prompt to cancel.",
        style="info",
    ))
    console.print()

    try:
        name = _prompt_name(store, console)
        role = _prompt_role()
        type_ = _prompt_type(role)

        if role == "source":
            spec = _build_source_spec(console, type_)
            store.add_source(name, spec)
        else:
            spec = _build_sink_spec(console, type_)
            store.add_sink(name, spec)

        store.save()

    except _Cancelled:
        console.print()
        console.print(Text("  Cancelled. No datasource was saved.", style="muted"))
        console.print()
        return None
    except DatasourceError as e:
        console.print()
        console.print(Text(f"  Error: {e}", style="error"))
        console.print()
        return None

    console.print()
    console.print(Text(
        f"  ✓ Datasource {name!r} saved to {store.path}",
        style="success",
    ))
    console.print()
    return name


# ── Step 1: name ─────────────────────────────────────────────────────────────

def _prompt_name(store: DatasourceStore, console: Console) -> str:
    """Prompt for a unique, valid datasource name."""
    while True:
        raw = prompts.text(
            "Name for this datasource (lowercase, hyphens/underscores ok):",
            default="",
        )
        if raw is None:
            raise _Cancelled()

        name = raw.strip().lower()
        if not name:
            continue

        if not _NAME_PATTERN.match(name):
            console.print(Text(
                f"  ✗ Invalid name {name!r}. Use lowercase letters, digits, "
                f"hyphens, or underscores (1-64 chars).",
                style="error",
            ))
            continue

        if store.exists(name):
            console.print(Text(
                f"  ✗ Name {name!r} is already used. Pick a different name "
                f"or remove the existing one first.",
                style="error",
            ))
            continue

        return name


# ── Step 2: role ─────────────────────────────────────────────────────────────

def _prompt_role() -> str:
    role = prompts.select(
        "What is this datasource for?",
        choices=[
            {"name": "Source",   "value": "source",
             "description": "A database to replicate FROM (CDC source)"},
            {"name": "Sink",     "value": "sink",
             "description": "A database to replicate TO (CDC target)"},
            {"name": "← Cancel", "value": "cancel", "description": ""},
        ],
        default="source",
    )
    if role in (None, "cancel"):
        raise _Cancelled()
    return role


# ── Step 3: type ─────────────────────────────────────────────────────────────

def _prompt_type(role: str) -> str:
    if role == "source":
        return "postgres"  # only PostgreSQL sources today

    type_ = prompts.select(
        "What kind of sink?",
        choices=[
            {"name": "PostgreSQL", "value": "postgres",  "description": ""},
            {"name": "StarRocks",  "value": "starrocks", "description": ""},
            {"name": "Snowflake",  "value": "snowflake",
             "description": "(cloud-only, requires credentials)"},
            {"name": "← Cancel",   "value": "cancel",    "description": ""},
        ],
        default="postgres",
    )
    if type_ in (None, "cancel"):
        raise _Cancelled()
    return type_


# ── Step 4: source spec ─────────────────────────────────────────────────────

def _build_source_spec(console: Console, type_: str) -> SourceSpec:
    if type_ != "postgres":
        raise _Cancelled()
    return _build_pg_source(console)


_DEMO_PG_SOURCE_URL = "postgres://postgres:postgres@localhost:15432/postgres"


def _build_pg_source(console: Console) -> PostgresSourceSpec:
    console.print()
    console.print(Text(
        "  PostgreSQL source — you'll need:",
        style="info",
    ))
    console.print(Text("    • A connection URL with replication permission", style="muted"))
    console.print(Text("    • The user must have REPLICATION privilege",      style="muted"))
    console.print(Text("    • The database must have wal_level=logical",      style="muted"))
    console.print()

    use_demo = prompts.confirm(
        "Use demo defaults? (localhost:15432 Docker source)",
        default=False,
    )
    if use_demo is None:
        raise _Cancelled()

    default_url = _DEMO_PG_SOURCE_URL if use_demo else ""

    url = prompts.text(
        "Connection URL (postgres://user:pass@host:port/dbname):",
        default=default_url,
    )
    if url is None or not url.strip():
        raise _Cancelled()
    url = url.strip()

    # Validate URL format
    parsed = urlparse(url)
    if parsed.scheme not in ("postgres", "postgresql"):
        raise DatasourceError(
            f"URL scheme must be postgres:// or postgresql://, got {parsed.scheme!r}"
        )
    if not parsed.hostname:
        raise DatasourceError("URL must include a hostname")

    seed: str | None = None
    if use_demo:
        seed_raw = prompts.text(
            "Seed SQL file in e2e/fixtures/ (default: postgres-seed.sql)",
            default="postgres-seed.sql",
        )
        if seed_raw is None:
            raise _Cancelled()
        seed = seed_raw.strip() or "postgres-seed.sql"

    slot = prompts.text("Replication slot name", default="dbmazz_slot")
    if slot is None:
        raise _Cancelled()
    slot = slot.strip() or "dbmazz_slot"

    pub = prompts.text("Publication name", default="dbmazz_pub")
    if pub is None:
        raise _Cancelled()
    pub = pub.strip() or "dbmazz_pub"

    # Test connection + discover tables
    console.print()
    console.print(Text("  → Testing connection...", style="info"))
    try:
        discovered_tables = _test_pg_and_discover(url)
    except Exception as e:
        console.print(Text(f"  ✗ Connection failed: {e}", style="error"))
        proceed = prompts.confirm(
            "Continue anyway and enter table names manually?",
            default=False,
        )
        if not proceed:
            raise _Cancelled()
        discovered_tables = []
    else:
        console.print(Text(
            f"  ✓ Connection OK ({len(discovered_tables)} tables visible in public schema)",
            style="success",
        ))
        console.print()

    default_tables = "orders,order_items" if use_demo else ""

    if discovered_tables:
        tables = _pick_tables_interactive(discovered_tables)
    else:
        tables_raw = prompts.text(
            "Tables to replicate (comma-separated):",
            default=default_tables,
        )
        if tables_raw is None:
            raise _Cancelled()
        tables = [t.strip() for t in tables_raw.split(",") if t.strip()]

    if not tables:
        raise DatasourceError("at least one table is required")

    return PostgresSourceSpec(
        url=url,
        seed=seed,
        replication_slot=slot,
        publication=pub,
        tables=tables,
    )


def _test_pg_and_discover(url: str) -> list[str]:
    """Connect to a PG and return the list of public-schema tables."""
    import psycopg2

    conn = psycopg2.connect(url, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' "
                "  AND tablename NOT LIKE 'dbmazz_%' "
                "  AND tablename NOT LIKE '_dbmazz_%' "
                "ORDER BY tablename"
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _pick_tables_interactive(available: list[str]) -> list[str]:
    """Interactive multiselect over a list of table names.

    questionary.checkbox supports multi-select with space-to-toggle.
    """
    import questionary
    from ..tui.prompts import EZ_CDC_PROMPT_STYLE

    try:
        result = questionary.checkbox(
            "Which tables do you want to replicate?",
            choices=available,
            style=EZ_CDC_PROMPT_STYLE,
            qmark="?",
            instruction="(use ↑↓ to navigate, space to toggle, ↵ to confirm)",
        ).ask()
    except KeyboardInterrupt:
        raise _Cancelled()

    if result is None:
        raise _Cancelled()
    return result


# ── Step 5: sink spec ────────────────────────────────────────────────────────

def _build_sink_spec(console: Console, type_: str) -> SinkSpec:
    if type_ == "postgres":
        return _build_pg_sink(console)
    if type_ == "starrocks":
        return _build_starrocks_sink(console)
    if type_ == "snowflake":
        return _build_snowflake_sink(console)
    raise _Cancelled()


_DEMO_PG_SINK_URL = "postgres://postgres:postgres@localhost:15433/postgres"
_DEMO_SR_URL = "http://localhost:18030"


def _build_pg_sink(console: Console) -> PostgresSinkSpec:
    console.print()
    console.print(Text("  PostgreSQL sink.", style="info"))
    console.print()

    use_demo = prompts.confirm(
        "Use demo defaults? (localhost:15433 Docker sink)",
        default=False,
    )
    if use_demo is None:
        raise _Cancelled()

    default_url = _DEMO_PG_SINK_URL if use_demo else ""

    url = prompts.text("Connection URL (postgres://user:pass@host:port/dbname):", default=default_url)
    if url is None or not url.strip():
        raise _Cancelled()
    url = url.strip()

    database = prompts.text("Target database name:", default="postgres" if use_demo else "")
    if database is None or not database.strip():
        raise _Cancelled()
    database = database.strip()

    schema = prompts.text("Target schema (default: public):", default="public")
    if schema is None:
        raise _Cancelled()
    schema = schema.strip() or "public"

    return PostgresSinkSpec(
        url=url,
        database=database,
        **{"schema": schema},  # alias to schema_
    )


def _build_starrocks_sink(console: Console) -> StarRocksSinkSpec:
    console.print()
    console.print(Text("  StarRocks sink.", style="info"))
    console.print()

    use_demo = prompts.confirm(
        "Use demo defaults? (localhost:18030 Docker StarRocks)",
        default=False,
    )
    if use_demo is None:
        raise _Cancelled()

    default_url = _DEMO_SR_URL if use_demo else ""

    url = prompts.text("FE HTTP URL (e.g., http://starrocks.local:8030):", default=default_url)
    if url is None or not url.strip():
        raise _Cancelled()
    url = url.strip()

    database = prompts.text("Target database name:", default="test" if use_demo else "")
    if database is None or not database.strip():
        raise _Cancelled()
    database = database.strip()

    user = prompts.text("User:", default="root")
    if user is None:
        raise _Cancelled()
    user = user.strip() or "root"

    password = prompts.password("Password (leave empty if none):") or ""

    return StarRocksSinkSpec(
        url=url,
        database=database,
        user=user,
        password=password,
    )


def _build_snowflake_sink(console: Console) -> SnowflakeSinkSpec:
    """Interactive prompts for a Snowflake sink datasource.

    Snowflake's UI shows several account-related fields separately
    (Account locator, Region, Cloud, Account/Server URL, Account name)
    and the user typically has all of them. Rather than asking for a
    single "account identifier" string and forcing them to know the
    canonical format, this function offers three input formats and
    composes the final `account` string internally.
    """
    console.print()
    console.print(Text(
        "  Snowflake sink (cloud-only — credentials required).",
        style="info",
    ))
    console.print(Text(
        "  Find your account details in the Snowflake UI:",
        style="muted",
    ))
    console.print(Text(
        "    Profile menu (top-right) → Account → View account details",
        style="muted",
    ))
    console.print()

    account = _prompt_snowflake_account(console)

    user = prompts.text("Username:", default="")
    if user is None or not user.strip():
        raise _Cancelled()

    password = prompts.password("Password (input hidden):")
    if password is None or not password:
        raise _Cancelled()

    database = prompts.text("Database name:", default="")
    if database is None or not database.strip():
        raise _Cancelled()

    schema = prompts.text("Schema (default: PUBLIC):", default="PUBLIC")
    if schema is None:
        raise _Cancelled()
    schema = schema.strip() or "PUBLIC"

    warehouse = prompts.text("Warehouse name:", default="")
    if warehouse is None or not warehouse.strip():
        raise _Cancelled()

    role_raw = prompts.text(
        "Role (optional, e.g. ACCOUNTADMIN — press Enter to skip):",
        default="",
    )
    role = role_raw.strip() if role_raw else None

    soft_delete = prompts.confirm(
        "Use soft delete? (rows marked with _DBMAZZ_IS_DELETED instead of being removed)",
        default=True,
    )
    if soft_delete is None:
        raise _Cancelled()

    return SnowflakeSinkSpec(
        account=account,
        user=user.strip(),
        password=password,
        database=database.strip(),
        warehouse=warehouse.strip(),
        role=role,
        soft_delete=soft_delete,
        **{"schema": schema},
    )


def _prompt_snowflake_account(console: Console) -> str:
    """Ask the user for the Snowflake account, offering three input formats.

    Returns the canonical `account` string that snowflake-connector-python
    expects. Examples it can produce:
        xy12345.us-east-1
        xy12345.us-east-1.aws
        myorg-myaccount
        xy12345
    """
    fmt = prompts.select(
        "How do you want to provide your Snowflake account?",
        choices=[
            {"name": "Account URL",
             "value": "url",
             "description": "Full URL from Snowflake UI (most common — copy/paste)"},
            {"name": "Locator + region",
             "value": "parts",
             "description": "Account locator and region as separate fields"},
            {"name": "Identifier",
             "value": "id",
             "description": "Combined identifier if you already know it"},
            {"name": "← Cancel",
             "value": "cancel",
             "description": ""},
        ],
        default="url",
    )
    if fmt in (None, "cancel"):
        raise _Cancelled()

    if fmt == "url":
        raw = prompts.text(
            "Account URL (e.g. xy12345.us-east-1.snowflakecomputing.com):",
            default="",
        )
        if raw is None or not raw.strip():
            raise _Cancelled()
        account = _parse_snowflake_url(raw.strip())
        if not account:
            print_err(
                console,
                "Could not extract an account identifier from that URL. "
                "Try the 'Locator + region' option instead."
            )
            raise _Cancelled()
        console.print(Text(f"  → parsed as account = {account}", style="muted"))
        return account

    if fmt == "parts":
        locator = prompts.text(
            "Account locator (e.g. xy12345 or myorg-myaccount):",
            default="",
        )
        if locator is None or not locator.strip():
            raise _Cancelled()
        locator = locator.strip()

        region = prompts.text(
            "Region (e.g. us-east-1 — press Enter if not applicable):",
            default="",
        )
        if region is None:
            raise _Cancelled()
        region = region.strip()

        cloud = prompts.text(
            "Cloud provider (aws / azure / gcp — optional, press Enter to skip):",
            default="",
        )
        if cloud is None:
            raise _Cancelled()
        cloud = cloud.strip().lower()

        # Compose the account identifier:
        #   <locator>                               (no region)
        #   <locator>.<region>                      (region only)
        #   <locator>.<region>.<cloud>              (region + cloud)
        parts = [locator]
        if region:
            parts.append(region)
            if cloud and cloud in ("aws", "azure", "gcp"):
                parts.append(cloud)
        account = ".".join(parts)
        console.print(Text(f"  → composed as account = {account}", style="muted"))
        return account

    # fmt == "id"
    raw = prompts.text(
        "Account identifier (e.g. xy12345.us-east-1 or myorg-myaccount):",
        default="",
    )
    if raw is None or not raw.strip():
        raise _Cancelled()
    return raw.strip()


def _parse_snowflake_url(url: str) -> str:
    """Extract the account identifier portion of a Snowflake URL.

    Handles common shapes:
        xy12345.us-east-1.snowflakecomputing.com         → xy12345.us-east-1
        https://xy12345.us-east-1.snowflakecomputing.com → xy12345.us-east-1
        xy12345.us-east-1.aws.snowflakecomputing.com     → xy12345.us-east-1.aws
        myorg-myaccount.snowflakecomputing.com           → myorg-myaccount
        xy12345.snowflakecomputing.com                   → xy12345

    If the input doesn't look like a Snowflake URL (no
    .snowflakecomputing.com suffix), returns it unchanged on the assumption
    that the user already provided a bare identifier.
    """
    # Strip protocol if present
    if url.startswith("http://"):
        url = url[7:]
    elif url.startswith("https://"):
        url = url[8:]

    # Strip path/query/port if present
    for sep in ("/", "?", "#", ":"):
        if sep in url:
            url = url.split(sep, 1)[0]

    # Strip the snowflakecomputing.com suffix
    suffix = ".snowflakecomputing.com"
    if url.endswith(suffix):
        url = url[: -len(suffix)]

    return url


def print_err(console: Console, msg: str) -> None:
    """Print an inline error inside the wizard."""
    console.print(Text(f"  ✗ {msg}", style="error"))
