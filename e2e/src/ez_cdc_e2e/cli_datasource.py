"""`ez-cdc datasource ...` subcommand group.

Provides CRUD over the datasources YAML file:

  ez-cdc datasource list                       # rich table, passwords redacted
  ez-cdc datasource show NAME [--reveal]       # detail view
  ez-cdc datasource add                        # interactive wizard (chunk 5)
  ez-cdc datasource remove NAME [--yes]        # remove with confirm
  ez-cdc datasource test NAME                  # connection test
  ez-cdc datasource init-demos                 # write bundled demo datasources

Sensitive fields (passwords, private_key_path, password embedded in URL)
are redacted by default in `list` and `show`. Pass `--reveal` to `show`
to display them.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import typer
from rich.console import Console
from rich.padding import Padding
from rich.table import Table
from rich.text import Text

from .datasources.loader import (
    DatasourceError,
    DatasourceNotFoundError,
)
from .datasources.presets import merge_demos_into
from .datasources.schema import (
    PostgresSinkSpec,
    PostgresSourceSpec,
    SinkSpec,
    SnowflakeSinkSpec,
    SourceSpec,
    StarRocksSinkSpec,
)
from .datasources.store import DatasourceStore
from .paths import E2E_DIR, ENV_SNOWFLAKE_FILE
from .tui import prompts
from .tui.report import final_padding
from .tui.theme import EZ_CDC_THEME


# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_DATASOURCES_PATH = E2E_DIR / "datasources.yaml"

# Field names that contain credentials and should be redacted by default.
_SENSITIVE_KEYS = {"password", "private_key_path"}


# ── Console ──────────────────────────────────────────────────────────────────

console = Console(theme=EZ_CDC_THEME)


# ── Typer app ────────────────────────────────────────────────────────────────

datasource_app = typer.Typer(
    name="datasource",
    help="Manage source/sink datasources stored in datasources.yaml.",
    no_args_is_help=True,
)


# ── Redaction helpers ────────────────────────────────────────────────────────

def _redact_url_password(url: str) -> str:
    """Replace the password in a connection URL with ***.

    Handles `scheme://user:password@host:port/path` URLs. Returns the URL
    unchanged if there's no password to redact.
    """
    if not url or "://" not in url:
        return url
    try:
        parsed = urlparse(url)
    except ValueError:
        return url

    if parsed.password is None:
        return url

    # Rebuild netloc with the password redacted
    user = parsed.username or ""
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    new_netloc = f"{user}:***@{host}{port}"

    return urlunparse((
        parsed.scheme, new_netloc, parsed.path,
        parsed.params, parsed.query, parsed.fragment,
    ))


def _spec_to_dict(spec: SourceSpec | SinkSpec, *, reveal: bool = False) -> dict[str, Any]:
    """Convert a spec to a dict suitable for display, redacting credentials
    unless reveal=True."""
    d = spec.model_dump(by_alias=True, exclude_none=True)

    if reveal:
        return d

    # Redact known sensitive top-level fields
    for key in list(d.keys()):
        lower = key.lower()
        if any(s in lower for s in _SENSITIVE_KEYS):
            d[key] = "***"

    # Redact passwords embedded in URLs
    if "url" in d and isinstance(d["url"], str):
        d["url"] = _redact_url_password(d["url"])

    return d


# ── Store loader (with consistent error handling) ────────────────────────────

def _load_store(
    path: Path = DEFAULT_DATASOURCES_PATH,
    *,
    must_exist: bool = False,
) -> DatasourceStore:
    """Load the datasources store, exiting cleanly on error.

    With must_exist=True, exits with code 2 if the file doesn't exist
    (used by commands that need data, like `list` or `show`).
    With must_exist=False, returns an empty store if the file is missing
    (used by commands that create data, like `init-demos` and `add`).
    """
    store = DatasourceStore(path)
    try:
        store.load(allow_missing=not must_exist)
    except DatasourceError as e:
        console.print(Text(f"Error: {e}", style="error"))
        raise typer.Exit(2)

    if must_exist and store.is_empty():
        console.print(Text(
            f"No datasources configured yet. Run `ez-cdc datasource init-demos` "
            f"to create the bundled demo datasources, or `ez-cdc datasource add` "
            f"to configure your own.",
            style="warning",
        ))
        raise typer.Exit(2)

    return store


# ── Commands ─────────────────────────────────────────────────────────────────

@datasource_app.command("list", help="Show all configured datasources in a table.")
def list_cmd(
    datasources: Path = typer.Option(
        DEFAULT_DATASOURCES_PATH, "--datasources",
        help="Path to the datasources YAML file.",
    ),
) -> None:
    store = _load_store(datasources, must_exist=False)

    if store.is_empty():
        console.print()
        console.print(Text(
            "  No datasources configured yet.",
            style="warning",
        ))
        console.print(Text(
            "  Run `ez-cdc datasource init-demos` to create the bundled demos,",
            style="muted",
        ))
        console.print(Text(
            "  or `ez-cdc datasource add` to configure your own.",
            style="muted",
        ))
        console.print()
        return

    # Build a rich table
    table = Table(
        title=Text(f"Datasources in {datasources}", style="brand"),
        title_justify="left",
        show_header=True,
        header_style="panel.header",
        border_style="panel.border",
        padding=(0, 1),
    )
    table.add_column("Role",    style="check.id", no_wrap=True)
    table.add_column("Name",    style="metric.number", no_wrap=True)
    table.add_column("Type",    style="metric.label", no_wrap=True)
    table.add_column("Managed", style="metric.label", justify="center", no_wrap=True)
    table.add_column("Connection / details", style="check.detail")

    for name in store.list_sources():
        spec = store.get_source(name)
        table.add_row(
            "source",
            name,
            spec.type,
            "✓ yes" if spec.managed else "✗ no",
            _summarize_source(spec),
        )

    for name in store.list_sinks():
        spec = store.get_sink(name)
        table.add_row(
            "sink",
            name,
            spec.type,
            "✓ yes" if spec.managed else "✗ no",
            _summarize_sink(spec),
        )

    # Wrap the table in a Padding so it sits 2 cols away from the terminal
    # left edge, matching the rest of the CLI's "  " indent convention.
    # Extra blank lines above/below give the table room to breathe.
    console.print()
    console.print()
    console.print(Padding(table, (0, 0, 0, 2)))
    console.print()
    console.print(Text(
        f"  {len(store.list_sources())} source(s), {len(store.list_sinks())} sink(s)",
        style="muted",
    ))
    final_padding(console)


def _summarize_source(spec: SourceSpec) -> str:
    """One-line summary of a source spec for the list view."""
    if isinstance(spec, PostgresSourceSpec):
        if spec.managed:
            seed = f"seed={spec.seed}" if spec.seed else "no seed"
            return f"managed PG · {seed} · tables={','.join(spec.tables)}"
        else:
            redacted_url = _redact_url_password(spec.url or "")
            return f"{redacted_url} · tables={','.join(spec.tables)}"
    return spec.type


def _summarize_sink(spec: SinkSpec) -> str:
    """One-line summary of a sink spec for the list view."""
    if isinstance(spec, PostgresSinkSpec):
        if spec.managed:
            return "managed PG (will be started by ez-cdc)"
        else:
            redacted_url = _redact_url_password(spec.url or "")
            return f"{redacted_url}/{spec.database}"
    if isinstance(spec, StarRocksSinkSpec):
        if spec.managed:
            return "managed StarRocks (will be started by ez-cdc)"
        else:
            return f"{spec.url}/{spec.database} (user={spec.user})"
    if isinstance(spec, SnowflakeSinkSpec):
        return (
            f"{spec.account} · db={spec.database} · wh={spec.warehouse} · "
            f"{'soft' if spec.soft_delete else 'hard'} delete"
        )
    return spec.type


@datasource_app.command("show", help="Show full details of one datasource.")
def show_cmd(
    name: str = typer.Argument(..., help="Datasource name."),
    reveal: bool = typer.Option(
        False, "--reveal",
        help="Show passwords and other sensitive fields in plaintext (default: redacted).",
    ),
    datasources: Path = typer.Option(
        DEFAULT_DATASOURCES_PATH, "--datasources",
    ),
) -> None:
    store = _load_store(datasources, must_exist=True)

    # Find the datasource (could be source or sink)
    spec: SourceSpec | SinkSpec
    role: str
    if name in store.data.sources:
        spec = store.get_source(name)
        role = "source"
    elif name in store.data.sinks:
        spec = store.get_sink(name)
        role = "sink"
    else:
        console.print(Text(
            f"Error: datasource {name!r} not found in {datasources}",
            style="error",
        ))
        sources = store.list_sources()
        sinks = store.list_sinks()
        if sources:
            console.print(Text(f"  Available sources: {', '.join(sources)}", style="muted"))
        if sinks:
            console.print(Text(f"  Available sinks: {', '.join(sinks)}", style="muted"))
        raise typer.Exit(2)

    # Render
    fields = _spec_to_dict(spec, reveal=reveal)

    console.print()
    console.print(Text(f"  {role.upper()}: ", style="metric.label") +
                  Text(name, style="brand") +
                  Text(f"   ({spec.type})", style="muted"))
    console.print()

    for key, value in fields.items():
        if key == "type":
            continue  # already in the header
        console.print(
            Text(f"    {key:<20}", style="metric.label") +
            Text(str(value), style="metric.number")
        )

    if not reveal and any("***" in str(v) for v in fields.values()):
        console.print()
        console.print(Text(
            "  (passwords are redacted; use --reveal to show them)",
            style="muted",
        ))
    final_padding(console)


@datasource_app.command("remove", help="Remove a datasource by name.")
def remove_cmd(
    name: str = typer.Argument(..., help="Datasource name."),
    yes: bool = typer.Option(
        False, "--yes", "-y",
        help="Skip the confirmation prompt.",
    ),
    datasources: Path = typer.Option(
        DEFAULT_DATASOURCES_PATH, "--datasources",
    ),
) -> None:
    store = _load_store(datasources, must_exist=True)

    if not store.exists(name):
        console.print(Text(
            f"Error: datasource {name!r} not found in {datasources}",
            style="error",
        ))
        raise typer.Exit(2)

    # Confirm
    if not yes:
        confirmed = prompts.confirm(
            f"Remove datasource {name!r}? This cannot be undone.",
            default=False,
        )
        if not confirmed:
            console.print(Text("  Cancelled.", style="muted"))
            return

    try:
        store.remove(name)
        store.save()
    except (DatasourceError, OSError) as e:
        console.print(Text(f"Error: {e}", style="error"))
        raise typer.Exit(2)

    console.print(Text(f"  ✓ Removed {name!r}", style="success"))
    final_padding(console)


@datasource_app.command("test", help="Test connectivity to a datasource (user-managed only).")
def test_cmd(
    name: str = typer.Argument(..., help="Datasource name."),
    datasources: Path = typer.Option(
        DEFAULT_DATASOURCES_PATH, "--datasources",
    ),
) -> None:
    store = _load_store(datasources, must_exist=True)

    if not store.exists(name):
        console.print(Text(
            f"Error: datasource {name!r} not found",
            style="error",
        ))
        raise typer.Exit(2)

    # Resolve the spec (source or sink)
    spec: SourceSpec | SinkSpec
    if name in store.data.sources:
        spec = store.get_source(name)
    else:
        spec = store.get_sink(name)

    # Managed datasources don't have a meaningful "test" — they're started
    # by ez-cdc itself, not pre-existing.
    if spec.managed:
        console.print(Text(
            f"  Datasource {name!r} is managed by ez-cdc — there's nothing to test.",
            style="warning",
        ))
        console.print(Text(
            f"  It will be created in a Docker container when a flow needs it.",
            style="muted",
        ))
        return

    console.print()
    console.print(Text(f"  Testing connection to {name!r}...", style="info"))

    try:
        if isinstance(spec, PostgresSourceSpec):
            _test_postgres_source(spec)
        elif isinstance(spec, PostgresSinkSpec):
            _test_postgres_sink(spec)
        elif isinstance(spec, StarRocksSinkSpec):
            _test_starrocks_sink(spec)
        elif isinstance(spec, SnowflakeSinkSpec):
            _test_snowflake_sink(spec)
        else:
            console.print(Text(
                f"  Connection test not implemented for type {spec.type!r}",
                style="warning",
            ))
            return
    except Exception as e:
        console.print(Text(f"  ✗ Connection failed: {e}", style="error"))
        raise typer.Exit(1)

    console.print(Text("  ✓ Connection successful", style="success"))
    final_padding(console)


# ── Per-type connection tests ───────────────────────────────────────────────

def _test_postgres_source(spec: PostgresSourceSpec) -> None:
    import psycopg2
    if not spec.url:
        raise RuntimeError("user-managed Postgres source has no url")
    conn = psycopg2.connect(spec.url, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            console.print(Text(f"    server: {version[:60]}...", style="muted"))
    finally:
        conn.close()


def _test_postgres_sink(spec: PostgresSinkSpec) -> None:
    import psycopg2
    if not spec.url:
        raise RuntimeError("user-managed Postgres sink has no url")
    conn = psycopg2.connect(spec.url, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            console.print(Text(f"    server: {version[:60]}...", style="muted"))
    finally:
        conn.close()


def _test_starrocks_sink(spec: StarRocksSinkSpec) -> None:
    import mysql.connector
    if not spec.url:
        raise RuntimeError("user-managed StarRocks sink has no url")
    # Parse the FE HTTP URL to extract the host
    from urllib.parse import urlparse
    parsed = urlparse(spec.url)
    host = parsed.hostname or "localhost"
    conn = mysql.connector.connect(
        host=host,
        port=spec.mysql_port,
        user=spec.user,
        password=spec.password,
        database=spec.database,
        connection_timeout=5,
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_version()")
        version = cur.fetchone()[0]
        console.print(Text(f"    StarRocks: {version}", style="muted"))
        cur.close()
    finally:
        conn.close()


def _test_snowflake_sink(spec: SnowflakeSinkSpec) -> None:
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=spec.account,
        user=spec.user,
        password=spec.password,
        database=spec.database,
        schema=spec.schema_,
        warehouse=spec.warehouse,
        role=spec.role,
        login_timeout=10,
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_version()")
        version = cur.fetchone()[0]
        console.print(Text(f"    Snowflake: {version}", style="muted"))
        cur.close()
    finally:
        conn.close()


# ── init-demos ───────────────────────────────────────────────────────────────

@datasource_app.command(
    "init-demos",
    help="Create the bundled demo datasources (demo-pg, demo-starrocks, demo-pg-target).",
)
def init_demos_cmd(
    datasources: Path = typer.Option(
        DEFAULT_DATASOURCES_PATH, "--datasources",
    ),
) -> None:
    store = _load_store(datasources, must_exist=False)

    added_sources, added_sinks = merge_demos_into(store)

    if not added_sources and not added_sinks:
        console.print()
        console.print(Text(
            "  All demo datasources already exist. Nothing to do.",
            style="muted",
        ))
        console.print(Text(
            "  Run `ez-cdc datasource list` to see them.",
            style="muted",
        ))
        console.print()
        return

    try:
        store.save()
    except OSError as e:
        console.print(Text(f"Error: failed to save: {e}", style="error"))
        raise typer.Exit(2)

    console.print()
    console.print(Text(
        f"  ✓ Created {len(added_sources) + len(added_sinks)} demo datasource(s) in {datasources}",
        style="success",
    ))
    if added_sources:
        for name in added_sources:
            console.print(Text(f"      + source: {name}", style="muted"))
    if added_sinks:
        for name in added_sinks:
            console.print(Text(f"      + sink:   {name}", style="muted"))
    console.print()
    console.print(Text(
        "  Run `ez-cdc quickstart` to try them.",
        style="info",
    ))
    final_padding(console)


# ── add (delegates to wizard, implemented in PR4-5) ─────────────────────────

@datasource_app.command("add", help="Add a new datasource interactively.")
def add_cmd(
    datasources: Path = typer.Option(
        DEFAULT_DATASOURCES_PATH, "--datasources",
    ),
) -> None:
    # The wizard implementation lives in datasources/wizard.py — added in
    # the next chunk (PR4-5). For now, this is a stub that points users
    # at init-demos and manual edit until the wizard lands.
    try:
        from .datasources.wizard import run_add_wizard
    except ImportError:
        console.print()
        console.print(Text(
            "  Interactive `add` wizard is not yet implemented (lands in PR4-5).",
            style="warning",
        ))
        console.print(Text(
            "  Workarounds:",
            style="muted",
        ))
        console.print(Text(
            f"    1. Run `ez-cdc datasource init-demos` to create the bundled demos",
            style="muted",
        ))
        console.print(Text(
            f"    2. Edit {datasources} manually using the schema in datasources.example.yaml",
            style="muted",
        ))
        console.print()
        return

    store = _load_store(datasources, must_exist=False)
    run_add_wizard(store, console)


# ── import-env ──────────────────────────────────────────────────────────────

@datasource_app.command(
    "import-env",
    help="Import credentials from a legacy .env file into a new datasource.",
)
def import_env_cmd(
    env_file: Path = typer.Option(
        ENV_SNOWFLAKE_FILE, "--file", "-f",
        help="Path to the env file to import (default: e2e/.env.snowflake).",
    ),
    name: str = typer.Option(
        "snowflake-imported", "--name", "-n",
        help="Name to assign to the new datasource.",
    ),
    datasource_type: str = typer.Option(
        "snowflake", "--type", "-t",
        help="Type of datasource to create. Currently only 'snowflake' is supported.",
    ),
    delete_after: bool = typer.Option(
        False, "--delete-after",
        help="Delete the env file after a successful import.",
    ),
    datasources: Path = typer.Option(
        DEFAULT_DATASOURCES_PATH, "--datasources",
    ),
) -> None:
    """Import legacy .env.snowflake credentials into datasources.yaml.

    Convenience for users coming from PR 1, where Snowflake credentials
    lived in a separate .env.snowflake file. This command parses that
    file and writes the equivalent SnowflakeSinkSpec into the datasources
    YAML, so subsequent runs use the YAML as the source of truth like
    every other datasource. Optionally deletes the legacy file after.

    The wizard (`ez-cdc datasource add` → sink → snowflake) is the
    recommended path for *new* Snowflake configurations. import-env is
    for migrating existing ones without retyping.
    """
    if not env_file.exists():
        console.print()
        console.print(Text(
            f"Error: env file not found: {env_file}",
            style="error",
        ))
        console.print(Text(
            "  If your credentials live elsewhere, pass --file PATH.",
            style="muted",
        ))
        final_padding(console)
        raise typer.Exit(2)

    if datasource_type != "snowflake":
        console.print()
        console.print(Text(
            f"Error: --type={datasource_type!r} not supported. "
            "Only 'snowflake' can be imported from .env files today.",
            style="error",
        ))
        final_padding(console)
        raise typer.Exit(3)

    # Parse the env file
    env_vars = _parse_env_file(env_file)

    # Validate the required Snowflake vars
    required = [
        "SINK_SNOWFLAKE_ACCOUNT",
        "SINK_USER",
        "SINK_PASSWORD",
        "SINK_DATABASE",
        "SINK_SNOWFLAKE_WAREHOUSE",
    ]
    missing = [v for v in required if not env_vars.get(v)]
    if missing:
        console.print()
        console.print(Text(
            f"Error: env file is missing required variables: {', '.join(missing)}",
            style="error",
        ))
        console.print(Text(
            f"  See e2e/.env.snowflake.example for the expected format.",
            style="muted",
        ))
        final_padding(console)
        raise typer.Exit(2)

    # Build the spec
    soft_delete = (env_vars.get("SINK_SNOWFLAKE_SOFT_DELETE", "true").lower() == "true")
    spec = SnowflakeSinkSpec(
        managed=False,
        account=env_vars["SINK_SNOWFLAKE_ACCOUNT"],
        user=env_vars["SINK_USER"],
        password=env_vars["SINK_PASSWORD"],
        database=env_vars["SINK_DATABASE"],
        warehouse=env_vars["SINK_SNOWFLAKE_WAREHOUSE"],
        role=env_vars.get("SINK_SNOWFLAKE_ROLE") or None,
        soft_delete=soft_delete,
        **{"schema": env_vars.get("SINK_SCHEMA", "PUBLIC")},
    )

    # Add to store
    store = _load_store(datasources, must_exist=False)
    if store.exists(name):
        console.print()
        console.print(Text(
            f"Error: a datasource named {name!r} already exists in {datasources}",
            style="error",
        ))
        console.print(Text(
            "  Use a different --name or remove the existing one first.",
            style="muted",
        ))
        final_padding(console)
        raise typer.Exit(2)

    try:
        store.add_sink(name, spec)
        store.save()
    except (DatasourceError, OSError) as e:
        console.print(Text(f"Error: failed to save: {e}", style="error"))
        final_padding(console)
        raise typer.Exit(2)

    console.print()
    console.print(Text(
        f"  ✓ Imported credentials from {env_file}",
        style="success",
    ))
    console.print(Text(
        f"      → saved as sink {name!r} in {datasources}",
        style="muted",
    ))

    if delete_after:
        try:
            env_file.unlink()
            console.print(Text(f"  ✓ Deleted {env_file}", style="success"))
        except OSError as e:
            console.print(Text(
                f"  ⚠ Could not delete {env_file}: {e}",
                style="warning",
            ))
    else:
        console.print()
        console.print(Text(
            f"  ℹ The env file at {env_file} is no longer used by ez-cdc.",
            style="muted",
        ))
        console.print(Text(
            f"    Re-run with --delete-after to remove it, or keep it as a backup.",
            style="muted",
        ))

    console.print()
    console.print(Text(
        f"  Try it: `ez-cdc verify --source demo-pg --sink {name}`",
        style="info",
    ))
    final_padding(console)


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse a simple KEY=VALUE .env file (same parser as backends/snowflake.py)."""
    env: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        env[key] = value
    return env
