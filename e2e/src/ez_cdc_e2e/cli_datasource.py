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
    PipelineSettings,
    PostgresSinkSpec,
    PostgresSourceSpec,
    SinkSpec,
    SnowflakeSinkSpec,
    SourceSpec,
    StarRocksSinkSpec,
)
from .datasources.store import DatasourceStore
from .paths import E2E_DIR
from .tui import prompts
from .tui.report import final_padding
from .tui.theme import EZ_CDC_THEME


# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_CONFIG_PATH = E2E_DIR / "ez-cdc.yaml"

# Field names that contain credentials and should be redacted by default.
_SENSITIVE_KEYS = {"password", "private_key_path"}


# ── Console ──────────────────────────────────────────────────────────────────

console = Console(theme=EZ_CDC_THEME)


# ── Typer app ────────────────────────────────────────────────────────────────

datasource_app = typer.Typer(
    name="datasource",
    help="Manage source/sink datasources and pipeline settings in ez-cdc.yaml.",
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
    path: Path = DEFAULT_CONFIG_PATH,
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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config",
        help="Path to the ez-cdc config YAML file.",
    ),
) -> None:
    store = _load_store(config, must_exist=False)

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
        title=Text(f"Datasources in {config}", style="brand"),
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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config",
    ),
) -> None:
    store = _load_store(config, must_exist=True)

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
            f"Error: datasource {name!r} not found in {config}",
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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config",
    ),
) -> None:
    store = _load_store(config, must_exist=True)

    if not store.exists(name):
        console.print(Text(
            f"Error: datasource {name!r} not found in {config}",
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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config",
    ),
) -> None:
    store = _load_store(config, must_exist=True)

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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config",
    ),
) -> None:
    store = _load_store(config, must_exist=False)

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
        f"  ✓ Created {len(added_sources) + len(added_sinks)} demo datasource(s) in {config}",
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
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config",
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
            f"    2. Edit {config} manually using the schema in ez-cdc.example.yaml",
            style="muted",
        ))
        console.print()
        return

    store = _load_store(config, must_exist=False)
    run_add_wizard(store, console)


# ── settings ────────────────────────────────────────────────────────────────

_SETTINGS_FIELDS = [
    ("flush_size",               "Batch size (events per flush)",          "int"),
    ("flush_interval_ms",        "Flush interval (ms)",                    "int"),
    ("do_snapshot",              "Enable initial snapshot",                "bool"),
    ("snapshot_chunk_size",      "Rows per snapshot chunk",                "int"),
    ("snapshot_parallel_workers","Parallel snapshot workers",              "int"),
    ("initial_snapshot_only",    "Exit after snapshot (no CDC)",           "bool"),
    ("rust_log",                 "Log level (RUST_LOG)",                   "str"),
    ("snowflake_flush_files",    "Snowflake: COPY INTO after N files",    "int"),
]


@datasource_app.command("settings", help="View or edit dbmazz pipeline settings.")
def settings_cmd(
    edit: bool = typer.Option(
        False, "--edit", "-e",
        help="Interactively edit settings (without this flag, just shows current values).",
    ),
    config: Path = typer.Option(
        DEFAULT_CONFIG_PATH, "--config",
    ),
) -> None:
    store = _load_store(config, must_exist=False)
    settings = store.data.settings

    if not edit:
        _show_settings(settings)
        return

    _edit_settings_interactive(store, settings)


def _show_settings(settings: PipelineSettings) -> None:
    """Display current pipeline settings in a table."""
    table = Table(
        title=Text("Pipeline settings", style="brand"),
        title_justify="left",
        show_header=True,
        header_style="panel.header",
        border_style="panel.border",
        padding=(0, 1),
    )
    table.add_column("Setting",     style="metric.label", no_wrap=True)
    table.add_column("Value",       style="metric.number", no_wrap=True)
    table.add_column("Env var",     style="muted", no_wrap=True)
    table.add_column("Description", style="check.detail")

    for field_name, description, _ in _SETTINGS_FIELDS:
        value = getattr(settings, field_name)
        env_var = field_name.upper()
        table.add_row(field_name, str(value), env_var, description)

    console.print()
    console.print(Padding(table, (0, 0, 0, 2)))
    console.print()
    console.print(Text(
        "  Use `ez-cdc datasource settings --edit` to change values.",
        style="muted",
    ))
    final_padding(console)


def _edit_settings_interactive(store: DatasourceStore, settings: PipelineSettings) -> None:
    """Walk through each setting and let the user change it."""
    console.print()
    console.print(Text(
        "  Edit pipeline settings (press Enter to keep current value):",
        style="info",
    ))
    console.print()

    changed = False
    for field_name, description, field_type in _SETTINGS_FIELDS:
        current = getattr(settings, field_name)

        if field_type == "bool":
            new_val = prompts.confirm(
                f"  {description}",
                default=current,
            )
            if new_val != current:
                setattr(settings, field_name, new_val)
                changed = True
        elif field_type == "int":
            raw = prompts.text(
                f"  {description}",
                default=str(current),
            )
            if raw is None:
                continue
            raw = raw.strip()
            if raw and raw != str(current):
                try:
                    setattr(settings, field_name, int(raw))
                    changed = True
                except (ValueError, Exception) as e:
                    console.print(Text(f"    Invalid value: {e}", style="error"))
        else:
            raw = prompts.text(
                f"  {description}",
                default=str(current),
            )
            if raw is None:
                continue
            raw = raw.strip()
            if raw and raw != str(current):
                setattr(settings, field_name, raw)
                changed = True

    if not changed:
        console.print()
        console.print(Text("  No changes made.", style="muted"))
        final_padding(console)
        return

    store.data.settings = settings
    try:
        store.save()
    except OSError as e:
        console.print(Text(f"Error: failed to save: {e}", style="error"))
        raise typer.Exit(2)

    console.print()
    console.print(Text("  ✓ Settings saved", style="success"))
    final_padding(console)
