"""ez-cdc CLI entry point.

Typer app with subcommands:

    ez-cdc                    → interactive main menu (banner A)
    ez-cdc quickstart [SINK]  → launch dashboard (banner A)
    ez-cdc verify   [SINK]    → run validation tests (banner D)
    ez-cdc load     [SINK]    → load test (banner D) — PR 3
    ez-cdc up       [SINK]    → compose up (banner D)
    ez-cdc down     [SINK]    → compose down (banner D)
    ez-cdc logs     [SINK]    → tail compose logs (banner D)
    ez-cdc status   [SINK]    → one-shot /status fetch (banner D)

Detects interactive TTY and prompts for missing args when possible. Falls
back to hard errors in non-interactive mode.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.text import Text

from . import __version__, compose
from .backends.base import TargetBackend
from .backends.postgres import PostgresTarget
from .backends.snowflake import SnowflakeTarget
from .backends.starrocks import StarRocksTarget
from .compose import ComposeError
from .compose_builder import build_compose_for_pair, cache_dir_for
from .datasources.loader import DatasourceError, DatasourceNotFoundError
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
from .dbmazz import DbmazzClient, DbmazzError
from .quickstart.dashboard import QuickstartDashboard
from .source.postgres import PostgresSource
from .tui import prompts
from .tui.banner import render_banner_a, render_banner_d
from .tui.report import (
    format_step,
    format_step_ok,
    format_totals,
    report_to_json,
)
from .tui.theme import EZ_CDC_THEME
from .verify.runner import VerifyRunner


# ── Global console + app ─────────────────────────────────────────────────────

console = Console(theme=EZ_CDC_THEME)

app = typer.Typer(
    name="ez-cdc",
    help="EZ-CDC test harness — verify, load, and quickstart for dbmazz sinks.",
    invoke_without_command=True,
    no_args_is_help=False,
    add_completion=True,  # enables `ez-cdc --install-completion`
    rich_markup_mode="rich",
)

# Register the `datasource` subcommand group from PR4-4. Adds:
#   ez-cdc datasource list / show / add / remove / test / init-demos
from .cli_datasource import datasource_app  # noqa: E402  (circular-import-safe)
app.add_typer(datasource_app, name="datasource")


# ── TTY detection ────────────────────────────────────────────────────────────

def is_interactive() -> bool:
    """Return True if we should show prompts/banners/live UI.

    False when: piped stdout, piped stdin, --non-interactive flag set,
    or when typer is completing shell arguments.
    """
    if "--non-interactive" in sys.argv:
        return False
    return sys.stdin.isatty() and sys.stdout.isatty()


# ── Banner helpers ───────────────────────────────────────────────────────────

def _show_banner_a() -> None:
    if "--no-banner" in sys.argv or not is_interactive():
        return
    console.print(render_banner_a())


def _show_banner_d() -> None:
    if "--no-banner" in sys.argv or not is_interactive():
        return
    console.print(render_banner_d())


# ── Main callback (no subcommand → interactive menu) ────────────────────────

@app.callback()
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False, "--version", help="Show version and exit."
    ),
    no_banner: bool = typer.Option(
        False, "--no-banner", help="Suppress banner output."
    ),
    non_interactive: bool = typer.Option(
        False, "--non-interactive", help="Disable all prompts and interactive UI."
    ),
) -> None:
    if version:
        console.print(f"ez-cdc {__version__}")
        raise typer.Exit(0)

    # If the user invoked `ez-cdc` with no subcommand, launch the interactive menu.
    if ctx.invoked_subcommand is None:
        _main_menu()
        raise typer.Exit(0)


# Sentinel raised by `_resolve_sink` and similar helpers when the user cancels
# an interactive prompt (Esc / Ctrl+C / chose "Back"). The main menu catches
# this and returns to the top-level menu instead of exiting the program.
class _BackToMenu(Exception):
    """Signal: user wants to go back to the previous menu, not exit."""


def _main_menu() -> None:
    """Interactive main menu: banner A + top-level choice, loops until Exit.

    The menu loops so users can run multiple subcommands in one session.
    Any subcommand that raises _BackToMenu (because the user cancelled a
    sub-prompt with Esc / Ctrl+C) returns to this loop instead of exiting
    the program.
    """
    _show_banner_a()

    if not is_interactive():
        console.print(Text(
            "No subcommand given. Run `ez-cdc --help` to see available commands.",
            style="error",
        ))
        raise typer.Exit(3)

    while True:
        choice = prompts.select(
            "What would you like to do?",
            choices=[
                {"name": "Quickstart", "value": "quickstart", "description": "Try dbmazz with a sink"},
                {"name": "Verify",     "value": "verify",     "description": "Run e2e validation tests"},
                {"name": "Load test",  "value": "load",       "description": "Generate traffic + monitor"},
                {"name": "Compose",    "value": "compose",    "description": "Manage docker stack"},
                {"name": "Exit",       "value": "exit",       "description": ""},
            ],
            default="quickstart",
        )

        # Cancel (Esc/Ctrl+C) or explicit Exit → leave the program.
        if choice is None or choice == "exit":
            return

        try:
            if choice == "quickstart":
                quickstart(sink=None)
            elif choice == "verify":
                verify(sink=None)
            elif choice == "load":
                console.print(Text(
                    "Load test is coming in PR 3 — not yet implemented.",
                    style="warning",
                ))
            elif choice == "compose":
                _compose_menu()
        except _BackToMenu:
            # User cancelled a sub-prompt — fall through to the next loop iteration.
            pass
        except typer.Exit as e:
            # Some subcommand decided to exit. Exit code 130 means the user
            # cancelled (Ctrl+C); we treat that as "back to menu" in interactive
            # mode. Any other exit code is a real termination.
            if e.exit_code == 130:
                pass
            else:
                raise

        console.print()  # spacing before showing the menu again


def _compose_menu() -> None:
    """Submenu for compose operations (up, down, logs, status).

    Reads the user's datasources, lets them pick a (source, sink) pair, and
    dispatches to the appropriate top-level command. For actions that
    require an already-running stack (down/logs/status), only pairs whose
    cache directory shows a running compose are listed.
    """
    action = prompts.select(
        "Compose action:",
        choices=[
            {"name": "up",     "value": "up",     "description": "Start a stack"},
            {"name": "down",   "value": "down",   "description": "Stop and destroy a stack"},
            {"name": "logs",   "value": "logs",   "description": "Tail logs"},
            {"name": "status", "value": "status", "description": "Fetch current status"},
            {"name": "← Back", "value": "back",   "description": ""},
        ],
    )
    if action in (None, "back"):
        raise _BackToMenu()

    needs_running_stack = action in ("down", "logs", "status")

    if needs_running_stack:
        pair = _pick_running_pair_or_back(action)
        if pair is None:
            return
        source_name, sink_name = pair
    else:
        # `up` — let the user pick any pair from their datasources
        try:
            store = _load_store_for_flow()
            _ensure_datasources_or_setup(store)
            store.reload()
            source_name = _pick_source_interactive(store)
            sink_name = _pick_sink_interactive(store)
        except _BackToMenu:
            return

    if action == "up":
        up(source=source_name, sink=sink_name)
    elif action == "down":
        down(source=source_name, sink=sink_name)
    elif action == "logs":
        logs(source=source_name, sink=sink_name)
    elif action == "status":
        status(source=source_name, sink=sink_name)


def _pick_running_pair_or_back(action_name: str) -> Optional[tuple[str, str]]:
    """List currently-running source/sink pairs and let the user pick one.

    Iterates over all (source, sink) combinations from the user's datasources,
    checks each cache_dir/compose.yml with compose.is_running(), and shows
    only the running ones. If nothing is running, prints a helpful message
    and returns None (caller falls back to the compose menu).
    """
    try:
        store = _load_store_for_flow()
    except typer.Exit:
        raise _BackToMenu()

    if store.is_empty():
        console.print()
        console.print(Text(
            f"  No datasources configured. Nothing can be {action_name}'d.",
            style="warning",
        ))
        console.print()
        raise _BackToMenu()

    running_pairs: list[tuple[str, str]] = []
    try:
        for src_name in store.list_sources():
            for sk_name in store.list_sinks():
                cache = cache_dir_for(src_name, sk_name)
                cf = cache / "compose.yml"
                if compose.is_running(cf):
                    running_pairs.append((src_name, sk_name))
    except ComposeError as e:
        console.print(Text(f"  Error querying docker: {e}", style="error"))
        raise _BackToMenu()

    if not running_pairs:
        console.print()
        console.print(Text(
            f"  No stacks are currently running — nothing to {action_name}.",
            style="warning",
        ))
        console.print(Text(
            "  Start one with `ez-cdc up --source NAME --sink NAME` first.",
            style="muted",
        ))
        console.print()
        raise _BackToMenu()

    choices = [
        {"name": f"{s} → {k}", "value": f"{s}|{k}", "description": ""}
        for (s, k) in running_pairs
    ]
    choices.append({"name": "← Back", "value": "__back__", "description": ""})

    result = prompts.select(
        f"Which running stack would you like to {action_name}?",
        choices=choices,
        default=choices[0]["value"],
    )
    if result is None or result == "__back__":
        raise _BackToMenu()

    src_name, sk_name = result.split("|", 1)
    return (src_name, sk_name)


# ── Datasource pair resolution ──────────────────────────────────────────────

# Default datasources file location (configurable per-flow with --datasources).
from .paths import E2E_DIR  # noqa: E402
DEFAULT_DATASOURCES_PATH = E2E_DIR / "datasources.yaml"

# Legacy profile names from PR 1 mapped to (source_name, sink_name) pairs of
# the bundled demo datasources. Lets old CI scripts that did
# `ez-cdc verify pg-target` keep working — they get auto-translated to
# `--source demo-pg --sink demo-pg-target`.
LEGACY_PROFILE_PAIRS: dict[str, tuple[str, str]] = {
    "starrocks":  ("demo-pg", "demo-starrocks"),
    "pg-target":  ("demo-pg", "demo-pg-target"),
    "snowflake":  ("demo-pg", "demo-snowflake"),
}


def _load_store_for_flow(
    datasources_path: Path = DEFAULT_DATASOURCES_PATH,
) -> DatasourceStore:
    """Load the datasources store for a flow command.

    On parse errors, prints a friendly message and exits with code 2.
    On a missing file, returns an empty store (the gate decides what to do).
    """
    store = DatasourceStore(datasources_path)
    try:
        store.load(allow_missing=True)
    except DatasourceError as e:
        console.print(Text(f"Error: {e}", style="error"))
        raise typer.Exit(2)
    return store


def _ensure_datasources_or_setup(store: DatasourceStore) -> None:
    """Validate that the store has at least one source AND one sink.

    If not, drives the user through one of three remediation paths in
    interactive mode (configure now, use bundled demos, edit YAML manually),
    or exits with a clear error in non-interactive mode.

    Raises _BackToMenu if the user cancels — caught by the main menu loop.
    """
    if store.has_any():
        return

    if not is_interactive():
        console.print(Text(
            f"Error: no datasources configured in {store.path}\n"
            f"  Run `ez-cdc datasource init-demos` to create the bundled demos,\n"
            f"  or `ez-cdc datasource add` to configure your own.",
            style="error",
        ))
        raise typer.Exit(2)

    console.print()
    console.print(Text(
        "  ⚠ No datasources configured yet. You need at least one source "
        "and one sink before you can run any flow.",
        style="warning",
    ))
    console.print()

    choice = prompts.select(
        "How do you want to set them up?",
        choices=[
            {"name": "Use the bundled demo datasources", "value": "demos",
             "description": "Adds demo-pg + demo-starrocks + demo-pg-target (no config needed)"},
            {"name": "Configure them now (interactive wizard)", "value": "wizard",
             "description": "Runs `ez-cdc datasource add` for source then sink"},
            {"name": "Edit datasources.yaml manually", "value": "manual",
             "description": "Show me the schema and let me edit"},
            {"name": "← Back", "value": "back", "description": ""},
        ],
        default="demos",
    )

    if choice in (None, "back"):
        raise _BackToMenu()

    if choice == "demos":
        _init_demos_inline(store)
        return

    if choice == "wizard":
        from .datasources.wizard import run_add_wizard
        # Run the wizard at least twice (need both a source and a sink).
        while not store.has_any():
            console.print()
            if not store.list_sources():
                console.print(Text("  Step: configure a source", style="info"))
            elif not store.list_sinks():
                console.print(Text("  Step: configure a sink", style="info"))
            result = run_add_wizard(store, console)
            if result is None:
                # User cancelled mid-wizard. Loop back to the top-level
                # remediation menu so they can pick again.
                raise _BackToMenu()
        return

    if choice == "manual":
        console.print()
        console.print(Text(
            f"  Edit {store.path} to add sources/sinks.",
            style="info",
        ))
        console.print(Text(
            "  See e2e/datasources.example.yaml for the schema "
            "(or `ez-cdc datasource init-demos` to bootstrap).",
            style="muted",
        ))
        console.print()
        raise typer.Exit(0)


def _init_demos_inline(store: DatasourceStore) -> None:
    """Add bundled demos to the store and save. Used by the gate."""
    added_src, added_sk = merge_demos_into(store)
    try:
        store.save()
    except OSError as e:
        console.print(Text(f"Error: failed to save: {e}", style="error"))
        raise typer.Exit(2)
    console.print()
    console.print(Text(
        f"  ✓ Added {len(added_src) + len(added_sk)} bundled demo datasource(s)",
        style="success",
    ))
    for name in added_src:
        console.print(Text(f"      + source: {name}", style="muted"))
    for name in added_sk:
        console.print(Text(f"      + sink:   {name}", style="muted"))
    console.print()


def _resolve_pair(
    positional: Optional[str],
    source_name: Optional[str],
    sink_name: Optional[str],
    *,
    datasources_path: Path = DEFAULT_DATASOURCES_PATH,
) -> tuple[str, SourceSpec, str, SinkSpec, Path, Path]:
    """Resolve a (source, sink) pair from positional arg / flags / interactive picker.

    Drives the user through the gate (_ensure_datasources_or_setup) if no
    datasources are configured. Returns:

        (source_name, source_spec, sink_name, sink_spec, compose_path, env_path)

    Resolution order:
      1. If positional matches a legacy profile name (starrocks/pg-target/snowflake),
         map it to the demo pair (auto-importing demos if missing) and ignore
         --source/--sink flags.
      2. Else use the explicit --source/--sink flags if both given.
      3. Else, in interactive mode, prompt with selectors. In non-interactive
         mode, error out clearly.
    """
    store = _load_store_for_flow(datasources_path)

    # Legacy profile name handling — auto-import demos if needed.
    if positional and positional in LEGACY_PROFILE_PAIRS:
        if not store.has_any():
            _init_demos_inline(store)
        legacy_src, legacy_sk = LEGACY_PROFILE_PAIRS[positional]
        if not store.exists(legacy_src) or not store.exists(legacy_sk):
            _init_demos_inline(store)
        source_name = source_name or legacy_src
        sink_name = sink_name or legacy_sk

    # Gate: must have at least one source + one sink.
    _ensure_datasources_or_setup(store)
    # Reload in case the gate added datasources
    store.reload()

    # Resolve source name
    if source_name is None:
        if is_interactive():
            source_name = _pick_source_interactive(store)
        else:
            console.print(Text(
                "Error: --source is required in non-interactive mode.",
                style="error",
            ))
            raise typer.Exit(3)

    # Resolve sink name
    if sink_name is None:
        if is_interactive():
            sink_name = _pick_sink_interactive(store)
        else:
            console.print(Text(
                "Error: --sink is required in non-interactive mode.",
                style="error",
            ))
            raise typer.Exit(3)

    try:
        source_spec = store.get_source(source_name)
        sink_spec = store.get_sink(sink_name)
    except DatasourceNotFoundError as e:
        console.print(Text(f"Error: {e}", style="error"))
        raise typer.Exit(2)

    # Generate the per-pair compose
    try:
        compose_path, env_path = build_compose_for_pair(
            source_name, source_spec, sink_name, sink_spec,
        )
    except (FileNotFoundError, ValueError) as e:
        console.print(Text(f"Error: {e}", style="error"))
        raise typer.Exit(2)

    return source_name, source_spec, sink_name, sink_spec, compose_path, env_path


def _pick_source_interactive(store: DatasourceStore) -> str:
    """Prompt the user to pick a source from the store. Auto-pick if only 1."""
    sources = store.list_sources()
    if len(sources) == 1:
        return sources[0]

    choices = [
        {"name": name, "value": name, "description": _short_summary(store.get_source(name))}
        for name in sources
    ]
    choices.append({"name": "← Back", "value": "__back__", "description": ""})

    result = prompts.select(
        "Which source?",
        choices=choices,
        default=sources[0],
    )
    if result is None or result == "__back__":
        raise _BackToMenu()
    return result


def _pick_sink_interactive(store: DatasourceStore) -> str:
    """Prompt the user to pick a sink from the store. Auto-pick if only 1."""
    sinks = store.list_sinks()
    if len(sinks) == 1:
        return sinks[0]

    choices = [
        {"name": name, "value": name, "description": _short_summary(store.get_sink(name))}
        for name in sinks
    ]
    choices.append({"name": "← Back", "value": "__back__", "description": ""})

    result = prompts.select(
        "Which sink?",
        choices=choices,
        default=sinks[0],
    )
    if result is None or result == "__back__":
        raise _BackToMenu()
    return result


def _short_summary(spec) -> str:
    """One-line summary of a datasource spec for selector prompts."""
    if isinstance(spec, PostgresSourceSpec):
        if spec.managed:
            return f"managed PG · tables={','.join(spec.tables)}"
        return f"user PG · {len(spec.tables)} tables"
    if isinstance(spec, PostgresSinkSpec):
        return "managed PG" if spec.managed else "user PG"
    if isinstance(spec, StarRocksSinkSpec):
        return "managed StarRocks" if spec.managed else f"user StarRocks ({spec.url})"
    if isinstance(spec, SnowflakeSinkSpec):
        return f"Snowflake ({spec.account})"
    return spec.type


# (The old `_ensure_env_file_if_needed` family of helpers from PR 1 was
# removed in PR4-7. Credentials now live in datasource specs (loaded from
# datasources.yaml with ${VAR} interpolation), and the `add` wizard
# collects them via interactive prompts that write directly to the spec.
# There is no longer a separate .env.snowflake file managed by the CLI.)


# (Old _prompt_sink_or_back / _prompt_sink helpers removed in PR4-7.
# Replaced by _pick_source_interactive / _pick_sink_interactive above,
# which operate on a DatasourceStore instead of a hardcoded profile list.)


# ── Subcommand: quickstart ───────────────────────────────────────────────────

@app.command(help="Launch a source/sink pair and watch replication live in a terminal dashboard.")
def quickstart(
    profile: Optional[str] = typer.Argument(
        None,
        help="Legacy profile name (starrocks, pg-target, snowflake). For explicit pairs use --source/--sink.",
    ),
    source: Optional[str] = typer.Option(None, "--source", help="Source datasource name."),
    sink: Optional[str] = typer.Option(None, "--sink", help="Sink datasource name."),
    keep_up: bool = typer.Option(
        False, "--keep-up",
        help="Don't tear down the stack on exit (leave it running).",
    ),
    rebuild: bool = typer.Option(
        False, "--rebuild",
        help="Force `docker compose up --build`. Default reuses the cached dbmazz image.",
    ),
) -> None:
    _show_banner_a()

    src_name, src_spec, sk_name, sk_spec, compose_path, _env_path = _resolve_pair(
        profile, source, sink,
    )

    # If a stack is already running for this pair, offer reuse/recreate.
    already_running = False
    try:
        already_running = compose.is_running(compose_path)
    except ComposeError:
        pass

    if already_running:
        console.print()
        console.print(Text(
            f"  A stack for '{src_name} → {sk_name}' is already running.",
            style="warning",
        ))
        console.print()
        action = prompts.select(
            "What do you want to do?",
            choices=[
                {"name": "Reuse it",             "value": "reuse",
                 "description": "Open the dashboard on the existing stack"},
                {"name": "Destroy and recreate", "value": "recreate",
                 "description": "docker compose down -v, then up (clean slate)"},
                {"name": "← Back",                "value": "back", "description": ""},
            ],
            default="reuse",
        )
        if action in (None, "back"):
            raise _BackToMenu()

        if action == "recreate":
            console.print()
            console.print(format_step(f"Destroying existing stack..."))
            try:
                compose.down(compose_path, remove_volumes=True)
            except ComposeError as e:
                console.print(Text(f"Failed to tear down stack: {e}", style="error"))
                raise typer.Exit(2)
            console.print(format_step_ok(f"Destroying existing stack"))
            already_running = False

    console.print()
    if already_running:
        console.print(format_step_ok(f"Reusing existing stack: {src_name} → {sk_name}"))
    else:
        console.print(format_step(f"Starting compose: {src_name} → {sk_name}"))
        try:
            compose.up(compose_path, wait=True, build=rebuild)
        except ComposeError as e:
            console.print(Text(f"Failed to start compose: {e}", style="error"))
            raise typer.Exit(2)
        console.print(format_step_ok(f"Starting compose: {src_name} → {sk_name}"))

    # Connect clients to the actual containers/endpoints
    dbmazz_client = DbmazzClient("http://localhost:8080")
    source_client = _instantiate_source_from_spec(src_spec)
    source_client.connect()
    target = _instantiate_backend_from_spec(sk_spec)
    target.connect()

    # Wait for CDC stage
    console.print(format_step("Waiting for dbmazz to reach CDC stage (this may take a minute on first run)..."))
    try:
        dbmazz_client.wait_for_stage("cdc", timeout=180.0)
    except DbmazzError as e:
        console.print(Text(f"dbmazz did not reach CDC stage: {e}", style="error"))
        source_client.close()
        target.close()
        dbmazz_client.close()
        raise typer.Exit(2)
    console.print(format_step_ok("Waiting for dbmazz to reach CDC stage"))
    console.print()
    console.print(Text("Stack is live. Opening dashboard...", style="success"))
    console.print()

    # Decide whether to enable the traffic generator: only for managed sources
    # (we don't generate fake traffic against the user's real DB).
    enable_traffic = bool(getattr(src_spec, "managed", False))

    dashboard = QuickstartDashboard(
        profile=_make_profile_shim(src_name, sk_name, src_spec),
        dbmazz=dbmazz_client,
        target=target,
        console=console,
        source_counts_fn=lambda: {t: source_client.count_rows(t) for t in src_spec.tables},
        traffic_rate_eps=15.0 if enable_traffic else 0.0,
    )
    try:
        dashboard.run()
    except KeyboardInterrupt:
        pass
    finally:
        source_client.close()
        target.close()
        dbmazz_client.close()

    console.print()
    if keep_up:
        console.print(Text(
            f"Stack left running. Run `ez-cdc down --source {src_name} --sink {sk_name}` to stop it.",
            style="muted",
        ))
        _print_thanks()
        return

    confirmed = True
    if is_interactive():
        ans = prompts.confirm("Stop and destroy the stack?", default=True)
        confirmed = bool(ans)

    if confirmed:
        console.print()
        try:
            compose.down(compose_path, remove_volumes=True)
        except ComposeError as e:
            console.print(Text(f"Failed to stop compose: {e}", style="error"))
            raise typer.Exit(2)
        console.print(Text("Stack destroyed.", style="muted"))
    else:
        console.print(Text(
            f"Stack left running. Run `ez-cdc down --source {src_name} --sink {sk_name}` to stop it.",
            style="muted",
        ))

    _print_thanks()


# ── Subcommand: verify ───────────────────────────────────────────────────────

@app.command(help="Run e2e validation tests for a source/sink pair (or all pairs with --all).")
def verify(
    profile: Optional[str] = typer.Argument(
        None,
        help="Legacy profile name (starrocks, pg-target, snowflake). For explicit pairs use --source/--sink.",
    ),
    source: Optional[str] = typer.Option(None, "--source", help="Source datasource name."),
    sink: Optional[str] = typer.Option(None, "--sink", help="Sink datasource name."),
    quick: bool = typer.Option(False, "--quick", help="Tier 1 only (~30s per pair)."),
    all_pairs: bool = typer.Option(
        False, "--all",
        help="Run verify for all (managed source × managed sink) pairs in your datasources.",
    ),
    skip: Optional[str] = typer.Option(
        None, "--skip", help="Comma-separated check IDs to skip, e.g. --skip C3,D7"
    ),
    json_report: Optional[Path] = typer.Option(
        None, "--json-report", help="Write a JSON report to this path."
    ),
    keep_up: bool = typer.Option(
        False, "--keep-up", help="Don't run compose down after verify finishes."
    ),
    no_up: bool = typer.Option(
        False, "--no-up", help="Assume compose is already up; don't start it."
    ),
    rebuild: bool = typer.Option(
        False, "--rebuild",
        help="Force `docker compose up --build`. Default reuses the cached dbmazz image.",
    ),
) -> None:
    _show_banner_d()

    skip_ids = set(s.strip().upper() for s in skip.split(",")) if skip else set()

    if all_pairs:
        _verify_all(
            quick=quick, skip_ids=skip_ids, json_report=json_report,
            keep_up=keep_up, no_up=no_up, rebuild=rebuild,
        )
        return

    src_name, src_spec, sk_name, sk_spec, compose_path, _env_path = _resolve_pair(
        profile, source, sink,
    )
    exit_code = _verify_one(
        src_name, src_spec, sk_name, sk_spec, compose_path,
        quick=quick,
        skip_ids=skip_ids,
        json_report=json_report,
        keep_up=keep_up,
        no_up=no_up,
        rebuild=rebuild,
    )
    raise typer.Exit(exit_code)


def _verify_one(
    src_name: str,
    src_spec: SourceSpec,
    sk_name: str,
    sk_spec: SinkSpec,
    compose_path: Path,
    *,
    quick: bool,
    skip_ids: set[str],
    json_report: Optional[Path],
    keep_up: bool,
    no_up: bool,
    rebuild: bool,
) -> int:
    """Run verify for a single (source, sink) pair. Returns exit code."""
    console.print()
    mode = "read-only" if not src_spec.managed else "full"
    console.print(
        Text(
            f"EZ-CDC e2e   •   {src_name} → {sk_name}   •   "
            f"tier: {'1' if quick else '1+2'} ({mode})",
            style="brand",
        )
    )
    console.print()

    if not no_up:
        console.print(format_step(f"Starting compose: {src_name} → {sk_name}"))
        try:
            compose.up(compose_path, wait=True, build=rebuild)
        except ComposeError as e:
            console.print(Text(f"Failed to start compose: {e}", style="error"))
            return 2
        console.print(format_step_ok(f"Starting compose: {src_name} → {sk_name}"))

    # Run the verify suite using the spec-based runner.
    runner = VerifyRunner.from_specs(
        src_name=src_name,
        src_spec=src_spec,
        sk_name=sk_name,
        sk_spec=sk_spec,
        console=console,
        quick=quick,
        skip_ids=skip_ids,
    )
    report = runner.run()

    console.print(format_totals(report))

    if json_report:
        json_report.parent.mkdir(parents=True, exist_ok=True)
        json_report.write_text(json.dumps(report_to_json(report), indent=2))
        console.print(Text(f"JSON report written to {json_report}", style="muted"))

    if not keep_up:
        console.print()
        console.print(format_step("Tearing down compose..."))
        try:
            compose.down(compose_path, remove_volumes=True)
        except ComposeError as e:
            console.print(Text(f"Warning: compose down failed: {e}", style="warning"))
        else:
            console.print(format_step_ok("Tearing down compose"))

    return 0 if report.ok else 1


def _verify_all(
    *,
    quick: bool,
    skip_ids: set[str],
    json_report: Optional[Path],
    keep_up: bool,
    no_up: bool,
    rebuild: bool,
) -> None:
    """Run verify for every (managed source × managed sink) pair in the store.

    Per D38, --all only runs combinations where both source and sink are
    `managed: true` — those are the safe, no-credentials-needed pairs that
    matter for CI. User-managed datasources are excluded.
    """
    store = _load_store_for_flow()
    _ensure_datasources_or_setup(store)
    store.reload()

    # Build the list of (source, sink) pairs to run
    managed_sources = [
        n for n in store.list_sources() if store.get_source(n).managed
    ]
    managed_sinks = [
        n for n in store.list_sinks() if store.get_sink(n).managed
    ]

    if not managed_sources or not managed_sinks:
        console.print(Text(
            "Error: --all needs at least one managed source AND one managed sink. "
            "Run `ez-cdc datasource init-demos` to bootstrap them.",
            style="error",
        ))
        raise typer.Exit(2)

    pairs: list[tuple[str, SourceSpec, str, SinkSpec]] = []
    for src_name in managed_sources:
        for sk_name in managed_sinks:
            src_spec = store.get_source(src_name)
            sk_spec = store.get_sink(sk_name)
            pairs.append((src_name, src_spec, sk_name, sk_spec))

    aggregated: list[tuple[str, str, int]] = []
    for src_name, src_spec, sk_name, sk_spec in pairs:
        try:
            compose_path, _env_path = build_compose_for_pair(
                src_name, src_spec, sk_name, sk_spec,
            )
        except (FileNotFoundError, ValueError) as e:
            console.print(Text(f"Skipping {src_name} → {sk_name}: {e}", style="warning"))
            aggregated.append((src_name, sk_name, 2))
            continue

        report_path: Optional[Path] = None
        if json_report is not None:
            report_path = json_report.with_stem(
                f"{json_report.stem}-{src_name}-to-{sk_name}"
            )

        exit_code = _verify_one(
            src_name, src_spec, sk_name, sk_spec, compose_path,
            quick=quick,
            skip_ids=skip_ids,
            json_report=report_path,
            keep_up=keep_up,
            no_up=no_up,
            rebuild=rebuild,
        )
        aggregated.append((src_name, sk_name, exit_code))

    console.print()
    console.print(Text("━" * 60, style="rule"))
    any_failed = any(code != 0 for _, _, code in aggregated)
    for src_name, sk_name, code in aggregated:
        sym = "✓" if code == 0 else "✗"
        style = "pass" if code == 0 else "fail"
        console.print(Text(f"  {sym}  {src_name} → {sk_name}", style=style))
    console.print(Text("━" * 60, style="rule"))
    console.print()

    raise typer.Exit(1 if any_failed else 0)


# ── Subcommand: up ───────────────────────────────────────────────────────────

@app.command(help="Start the compose stack for a source/sink pair.")
def up(
    profile: Optional[str] = typer.Argument(
        None, help="Legacy profile name. Use --source/--sink for explicit pairs."
    ),
    source: Optional[str] = typer.Option(None, "--source"),
    sink: Optional[str] = typer.Option(None, "--sink"),
    rebuild: bool = typer.Option(
        False, "--rebuild",
        help="Force `docker compose up --build`. Default reuses the cached dbmazz image.",
    ),
) -> None:
    _show_banner_d()
    src_name, _src_spec, sk_name, _sk_spec, compose_path, _env_path = _resolve_pair(
        profile, source, sink,
    )
    console.print()
    console.print(format_step(f"Starting compose: {src_name} → {sk_name}"))
    try:
        compose.up(compose_path, wait=True, build=rebuild)
    except ComposeError as e:
        console.print(Text(f"Failed to start compose: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok(f"Starting compose: {src_name} → {sk_name}"))


# ── Subcommand: down ─────────────────────────────────────────────────────────

@app.command(help="Stop and destroy the compose stack for a source/sink pair.")
def down(
    profile: Optional[str] = typer.Argument(None),
    source: Optional[str] = typer.Option(None, "--source"),
    sink: Optional[str] = typer.Option(None, "--sink"),
    keep_volumes: bool = typer.Option(
        False, "--keep-volumes", help="Keep named volumes (don't run with -v)."
    ),
) -> None:
    _show_banner_d()
    src_name, _src_spec, sk_name, _sk_spec, compose_path, _env_path = _resolve_pair(
        profile, source, sink,
    )
    console.print()
    console.print(format_step(f"Stopping compose: {src_name} → {sk_name}"))
    try:
        compose.down(compose_path, remove_volumes=not keep_volumes)
    except ComposeError as e:
        console.print(Text(f"Failed to stop compose: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok(f"Stopping compose: {src_name} → {sk_name}"))


# ── Subcommand: logs ─────────────────────────────────────────────────────────

@app.command(help="Tail compose logs for a source/sink pair.")
def logs(
    profile: Optional[str] = typer.Argument(None),
    source: Optional[str] = typer.Option(None, "--source"),
    sink: Optional[str] = typer.Option(None, "--sink"),
    follow: bool = typer.Option(True, "--follow/--no-follow", "-f", help="Stream logs."),
    tail: int = typer.Option(100, "--tail", "-n", help="Number of lines to show from the end."),
) -> None:
    _show_banner_d()
    _src_name, _src_spec, _sk_name, _sk_spec, compose_path, _env_path = _resolve_pair(
        profile, source, sink,
    )
    console.print()
    try:
        compose.logs(compose_path, follow=follow, tail=tail)
    except ComposeError as e:
        console.print(Text(f"Failed to tail logs: {e}", style="error"))
        raise typer.Exit(2)


# ── Subcommand: status ───────────────────────────────────────────────────────

@app.command(help="Fetch a one-shot status snapshot from dbmazz.")
def status(
    profile: Optional[str] = typer.Argument(None),
    source: Optional[str] = typer.Option(None, "--source"),
    sink: Optional[str] = typer.Option(None, "--sink"),
) -> None:
    _show_banner_d()
    # status doesn't actually need the spec — it just hits localhost:8080.
    # We still resolve the pair so we can show what's running, and so the
    # gate runs (i.e., the user gets a useful error if there are no datasources).
    _src_name, _src_spec, _sk_name, _sk_spec, _compose_path, _env_path = _resolve_pair(
        profile, source, sink,
    )
    client = DbmazzClient("http://localhost:8080")
    try:
        s = client.status()
    except DbmazzError as e:
        console.print(Text(f"Failed to fetch status: {e}", style="error"))
        raise typer.Exit(2)
    finally:
        client.close()

    console.print()
    console.print(Text(f"  stage              {s.stage}", style="metric.label"))
    console.print(Text(f"  uptime             {s.uptime_secs}s", style="metric.label"))
    console.print(Text(f"  events total       {s.events_total:,}", style="metric.label"))
    console.print(Text(f"  events/sec         {s.events_per_sec:.0f}", style="metric.label"))
    console.print(Text(f"  replication lag    {s.replication_lag_ms} ms", style="metric.label"))
    console.print(Text(f"  confirmed LSN      {s.confirmed_lsn}", style="metric.label"))
    console.print(Text(f"  memory (RSS)       {s.memory_rss_mb:.1f} MB", style="metric.label"))
    console.print(Text(f"  CPU                {s.cpu_millicores / 10:.1f} %", style="metric.label"))
    if s.snapshot_active:
        console.print(Text(
            f"  snapshot           {s.snapshot_chunks_done}/{s.snapshot_chunks_total} chunks "
            f"({s.snapshot_rows_synced:,} rows)",
            style="metric.label",
        ))
    if s.error_detail:
        console.print(Text(f"  ERROR              {s.error_detail}", style="error"))
    console.print()


# ── Subcommand: load (placeholder for PR 3) ──────────────────────────────────

@app.command(help="Generate traffic and monitor replication (PR 3).")
def load(
    sink: Optional[str] = typer.Argument(None, help="Sink profile."),
    rate: int = typer.Option(500, "--rate", help="Target events/sec."),
    duration: int = typer.Option(60, "--duration", help="Duration in seconds."),
) -> None:
    _show_banner_d()
    console.print()
    console.print(Text(
        "Load test will be implemented in PR 3. For now only quickstart and "
        "verify are available — see `ez-cdc --help`.",
        style="warning",
    ))
    raise typer.Exit(3)


# ── Helpers ──────────────────────────────────────────────────────────────────

# Spec → client/backend instantiation lives in `instantiate.py` so the
# verify runner can use the same logic without importing cli.py.
from .instantiate import (  # noqa: E402
    instantiate_backend_from_spec as _instantiate_backend_from_spec,
    instantiate_source_from_spec as _instantiate_source_from_spec,
)


def _make_profile_shim(src_name: str, sk_name: str, src_spec: SourceSpec):
    """Build a minimal object the QuickstartDashboard accepts as `profile`.

    The dashboard from PR 1 expects an object with `.name`, `.dbmazz_http_url`,
    `.source_dsn`, `.compose_profile`, and `.tables`. With the datasources
    refactor we no longer have ProfileSpec instances, so we synthesize a
    duck-typed shim from the source spec for backward compat with the
    dashboard rendering code (which is unchanged in PR 4).
    """
    from types import SimpleNamespace
    return SimpleNamespace(
        name=f"{src_name}-to-{sk_name}",
        compose_profile=f"{src_name}-to-{sk_name}",
        dbmazz_http_url="http://localhost:8080",
        source_dsn="postgres://postgres:postgres@localhost:15432/dbmazz" if src_spec.managed else (src_spec.url or ""),
        tables=tuple(src_spec.tables),
    )


def _print_thanks() -> None:
    console.print()
    t = Text()
    t.append("  Thanks for trying EZ-CDC.   →   ", style="muted")
    t.append("https://ez-cdc.com", style="brand")
    console.print(t)
    console.print()
