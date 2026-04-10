"""ez-cdc CLI entry point.

Typer app with subcommands:

    ez-cdc                    → interactive main menu (banner A)
    ez-cdc quickstart [SINK]  → launch dashboard (banner A)
    ez-cdc verify   [SINK]    → run validation tests (banner D)
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
    final_padding,
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
    help="EZ-CDC test harness — quickstart and verify for dbmazz sinks.",
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
    """Interactive main menu: banner A + context-aware top-level choice loop.

    The menu detects on every iteration:
      - How many datasources are configured (sources / sinks)
      - Which (source, sink) pairs have currently-running stacks

    The status line at the top of the menu surfaces this state so the user
    sees what's available without having to navigate. When a stack is
    running, an "Open running stack" entry appears at the top of the
    choices for one-click reconnection.

    Loops so users can run multiple subcommands in one session. Any
    subcommand that raises _BackToMenu (Esc/Ctrl+C in a sub-prompt)
    returns here instead of exiting the program.
    """
    if not is_interactive():
        console.print(Text(
            "No subcommand given. Run `ez-cdc --help` to see available commands.",
            style="error",
        ))
        raise typer.Exit(3)

    while True:
        # Each iteration starts with a clean screen so the menu is always
        # at the top of the visible terminal. The shell scroll buffer
        # preserves previous output, so if the user wants to see what an
        # earlier action printed they can scroll up with the mouse — but
        # the *interactive* area is always anchored at the top.
        #
        # We use console.clear() (= ANSI clear screen) instead of an
        # alternate buffer (console.screen()) because Live displays in
        # quickstart already use alternate-screen mode internally and
        # nesting two alternate buffers breaks the dashboard rendering.
        console.clear()
        _show_banner_a()

        # Re-probe state on each iteration so the menu reflects current reality.
        ds_status = _probe_datasources_status()
        running_pair = _probe_first_running_pair(ds_status)

        _print_menu_status(ds_status, running_pair)

        # Build choices dynamically so the most relevant action is on top.
        choices: list[dict] = []

        if running_pair is not None:
            src_name, sk_name = running_pair
            choices.append({
                "name": f"Open running stack: {src_name} → {sk_name}",
                "value": "__open_running__",
                "description": "Reconnect the dashboard to the live stack",
            })

        choices += [
            {"name": "Quickstart",
             "value": "quickstart",
             "description": "Launch a source/sink pair and watch replication"},
            {"name": "Verify",
             "value": "verify",
             "description": "Run e2e validation tests"},
            {"name": "Datasources",
             "value": "datasources",
             "description": "List / add / remove source and sink configs"},
            {"name": "Clean target",
             "value": "clean",
             "description": "Truncate tables + drop audit columns + remove metadata"},
            {"name": "Stacks",
             "value": "compose",
             "description": "Manage docker stacks (up/down/logs/status)"},
            {"name": "Exit", "value": "exit", "description": ""},
        ]

        # Default to "open running" if a stack is up, else quickstart.
        default = "__open_running__" if running_pair is not None else "quickstart"

        choice = prompts.select(
            "What would you like to do?",
            choices=choices,
            default=default,
        )

        if choice is None or choice == "exit":
            # Final padding before returning to the shell prompt — without
            # this the user's `$` ends up flush against the menu's last line.
            console.clear()
            _print_thanks()
            return

        try:
            if choice == "__open_running__" and running_pair is not None:
                src_name, sk_name = running_pair
                quickstart(profile=None, source=src_name, sink=sk_name, keep_up=False, rebuild=False)
            elif choice == "quickstart":
                quickstart(profile=None, source=None, sink=None, keep_up=False, rebuild=False)
            elif choice == "verify":
                verify(
                    profile=None, source=None, sink=None,
                    quick=False, all_pairs=False, skip=None,
                    json_report=None, keep_up=False, no_up=False, rebuild=False,
                )
            elif choice == "clean":
                clean(profile=None, source=None, sink=None, yes=False)
            elif choice == "datasources":
                _datasources_menu()
            elif choice == "compose":
                _compose_menu()
        except _BackToMenu:
            pass
        except typer.Exit as e:
            if e.exit_code == 130:
                pass
            else:
                raise

        # Pause so the user can read the output of whatever action they
        # just ran. The next loop iteration will console.clear() and
        # redraw the menu, so this is the user's only chance to read it.
        # Quickstart, which has its own teardown prompt, returns here
        # *after* the user has already confirmed everything — so the
        # second pause is acceptable.
        _wait_for_keypress_to_continue()


def _probe_datasources_status() -> dict:
    """Snapshot the current datasources state for the menu status line.

    Returns a dict with:
      n_sources: int
      n_sinks:   int
      sources:   list[str]
      sinks:     list[str]
      ok:        bool — True if at least one source AND one sink exist
    """
    try:
        store = DatasourceStore(DEFAULT_CONFIG_PATH)
        store.load(allow_missing=True)
    except DatasourceError:
        return {"n_sources": 0, "n_sinks": 0, "sources": [], "sinks": [], "ok": False}

    sources = store.list_sources()
    sinks = store.list_sinks()
    return {
        "n_sources": len(sources),
        "n_sinks": len(sinks),
        "sources": sources,
        "sinks": sinks,
        "ok": bool(sources and sinks),
    }


def _probe_first_running_pair(ds_status: dict) -> Optional[tuple[str, str]]:
    """Return the first (source, sink) pair that has a running compose stack.

    Iterates over the cartesian product of sources × sinks and checks each
    cache_dir/compose.yml. Returns None if nothing is running. The "first"
    is well-defined because the source and sink lists are sorted.

    Skipped silently if docker isn't reachable — the menu just won't show
    the "Open running stack" entry.
    """
    if not ds_status["sources"] or not ds_status["sinks"]:
        return None
    try:
        for src_name in ds_status["sources"]:
            for sk_name in ds_status["sinks"]:
                cache = cache_dir_for(src_name, sk_name)
                cf = cache / "compose.yml"
                if compose.is_running(cf):
                    return (src_name, sk_name)
    except ComposeError:
        return None
    return None


def _print_menu_status(ds_status: dict, running_pair: Optional[tuple[str, str]]) -> None:
    """Print a one-line status banner above the menu prompt."""
    parts: list[Text] = []

    if ds_status["ok"]:
        parts.append(Text(
            f"  ✓ {ds_status['n_sources']} source(s), {ds_status['n_sinks']} sink(s) configured",
            style="success",
        ))
    elif ds_status["n_sources"] == 0 and ds_status["n_sinks"] == 0:
        parts.append(Text(
            "  ⚠ No datasources configured yet — start with Datasources or Quickstart.",
            style="warning",
        ))
    else:
        # Have one but not both
        parts.append(Text(
            f"  ⚠ {ds_status['n_sources']} source(s), {ds_status['n_sinks']} sink(s) — "
            "you need at least one of each before any flow can run.",
            style="warning",
        ))

    if running_pair is not None:
        src_name, sk_name = running_pair
        parts.append(Text(
            f"  ● 1+ stack running ({src_name} → {sk_name})",
            style="info",
        ))

    # Generous spacing: blank line above, status lines, two blanks below.
    # The two-below pattern gives the prompt some air before the question
    # appears, which makes the menu feel less crowded.
    console.print()
    for line in parts:
        console.print(line)
    console.print()
    console.print()


def _wait_for_keypress_to_continue() -> None:
    """Pause the menu loop so the user can read the previous action's output.

    Prints a hint and waits for Enter (or any key the input() call accepts).
    This is the user's only chance to read the output before the next loop
    iteration calls console.clear() and redraws the menu at the top.

    Catches EOFError / KeyboardInterrupt silently — both are equivalent to
    "the user is done reading, move on".
    """
    console.print()
    console.print(Text("  ─── Press Enter to return to the menu ───", style="dim"))
    try:
        input()
    except (EOFError, KeyboardInterrupt):
        pass


def _datasources_menu() -> None:
    """Submenu for `ez-cdc datasource` operations.

    Provides interactive access to list/show/add/remove/test/init-demos
    without having to remember the exact subcommand names.
    """
    while True:
        # Same anchor-at-top behavior as the main menu — clear before each
        # iteration so the submenu is always at the top of the visible
        # terminal, and the user reviews the previous action's output via
        # the Press Enter pause at the bottom of this loop.
        console.clear()
        _show_banner_d()
        console.print()

        action = prompts.select(
            "Datasource action:",
            choices=[
                {"name": "List all",        "value": "list",       "description": "Show every configured source and sink"},
                {"name": "Add new",         "value": "add",        "description": "Interactive wizard"},
                {"name": "Show details",    "value": "show",       "description": "Inspect one datasource"},
                {"name": "Test connection", "value": "test",       "description": "Verify a user-provided datasource is reachable"},
                {"name": "Remove",          "value": "remove",     "description": "Delete a datasource"},
                {"name": "Init bundled demos", "value": "init",    "description": "Add demo-pg, demo-starrocks, demo-pg-target"},
                {"name": "← Back",          "value": "back",       "description": ""},
            ],
        )
        if action in (None, "back"):
            return

        # Lazy import the subcommand functions from cli_datasource.py.
        from . import cli_datasource as ds_cmds

        try:
            if action == "list":
                ds_cmds.list_cmd(config=DEFAULT_CONFIG_PATH)
            elif action == "add":
                ds_cmds.add_cmd(config=DEFAULT_CONFIG_PATH)
            elif action == "show":
                name = _prompt_pick_any_datasource("show")
                if name:
                    ds_cmds.show_cmd(name=name, reveal=False, config=DEFAULT_CONFIG_PATH)
            elif action == "test":
                name = _prompt_pick_any_datasource("test")
                if name:
                    ds_cmds.test_cmd(name=name, config=DEFAULT_CONFIG_PATH)
            elif action == "remove":
                name = _prompt_pick_any_datasource("remove")
                if name:
                    ds_cmds.remove_cmd(name=name, yes=False, config=DEFAULT_CONFIG_PATH)
            elif action == "init":
                ds_cmds.init_demos_cmd(config=DEFAULT_CONFIG_PATH)
        except typer.Exit:
            # Subcommand decided to exit — swallow it so the menu loops.
            pass
        except _BackToMenu:
            pass

        # Pause so the user can read the output before we clear and redraw.
        _wait_for_keypress_to_continue()


def _prompt_pick_any_datasource(verb: str) -> Optional[str]:
    """Pick any datasource (source or sink) by name. Returns None on cancel."""
    store = _load_store_for_flow()
    if store.is_empty():
        console.print(Text(
            f"  No datasources configured. Nothing to {verb}.",
            style="warning",
        ))
        return None

    choices = []
    for name in store.list_sources():
        choices.append({"name": f"{name}  (source)", "value": name, "description": ""})
    for name in store.list_sinks():
        choices.append({"name": f"{name}  (sink)", "value": name, "description": ""})
    choices.append({"name": "← Back", "value": "__back__", "description": ""})

    result = prompts.select(
        f"Which datasource do you want to {verb}?",
        choices=choices,
    )
    if result is None or result == "__back__":
        return None
    return result


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
            {"name": "clean",  "value": "clean",  "description": "Clean target (truncate + drop audit cols)"},
            {"name": "← Back", "value": "back",   "description": ""},
        ],
    )
    if action in (None, "back"):
        raise _BackToMenu()

    if action == "clean":
        try:
            store = _load_store_for_flow()
            _ensure_datasources_or_setup(store)
            store.reload()
            source_name = _pick_source_interactive(store)
            sink_name = _pick_sink_interactive(store)
        except _BackToMenu:
            return
        clean(source=source_name, sink=sink_name, yes=False)
        return

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

# Default config file location (configurable per-flow with --config).
from .paths import E2E_DIR  # noqa: E402
DEFAULT_CONFIG_PATH = E2E_DIR / "ez-cdc.yaml"

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
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> DatasourceStore:
    """Load the datasources store for a flow command.

    On parse errors, prints a friendly message and exits with code 2.
    On a missing file, returns an empty store (the gate decides what to do).
    """
    store = DatasourceStore(config_path)
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
            {"name": "Edit ez-cdc.yaml manually", "value": "manual",
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
            "  See e2e/ez-cdc.example.yaml for the schema "
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
    config_path: Path = DEFAULT_CONFIG_PATH,
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
    store = _load_store_for_flow(config_path)

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
            settings=store.data.settings,
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
# ez-cdc.yaml with ${VAR} interpolation), and the `add` wizard
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

    # The traffic generator is **always created** but **always starts paused**
    # (rule from PR4 design discussion: never auto-generate writes — the user
    # opts in explicitly via the `[t]` keybinding). This applies to both
    # managed sources (demos) and user-managed sources (BYOD). For BYOD, the
    # user is responsible for the consequences of pressing `[t]` against their
    # real database — the dashboard surfaces a warning in the header so it's
    # not a silent surprise.
    dashboard = QuickstartDashboard(
        profile=_make_profile_shim(src_name, sk_name, src_spec),
        dbmazz=dbmazz_client,
        target=target,
        console=console,
        source_counts_fn=lambda: {t: source_client.count_rows(t) for t in src_spec.tables},
        traffic_rate_eps=15.0,
        source_is_managed=bool(getattr(src_spec, "managed", False)),
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
    _final_padding()
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
                settings=store.data.settings,
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
    _final_padding()

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
    _final_padding()


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
    _final_padding()


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
    _final_padding()


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
    _final_padding()



# ── Subcommand: clean ────────────────────────────────────────────────────────

@app.command(help="Clean target database — truncate tables, drop audit columns, remove dbmazz metadata.")
def clean(
    profile: Optional[str] = typer.Argument(
        None,
        help="Legacy profile name. For explicit pairs use --source/--sink.",
    ),
    source: Optional[str] = typer.Option(None, "--source", help="Source datasource name."),
    sink: Optional[str] = typer.Option(None, "--sink", help="Sink datasource name."),
    yes: bool = typer.Option(
        False, "--yes", "-y",
        help="Skip the confirmation prompt.",
    ),
) -> None:
    _show_banner_d()

    src_name, src_spec, sk_name, sk_spec, _compose_path, _env_path = _resolve_pair(
        profile, source, sink,
    )

    tables = list(getattr(src_spec, "tables", []))
    if not tables:
        console.print(Text("  No tables configured in the source spec.", style="error"))
        raise typer.Exit(2)

    console.print()
    console.print(Text(f"  Target: {sk_name} ({sk_spec.type})", style="info"))
    console.print(Text(f"  Tables: {', '.join(tables)}", style="info"))
    console.print(Text(
        "  This will TRUNCATE all data and DROP audit columns + dbmazz metadata.",
        style="warning",
    ))
    console.print()

    if not yes:
        from .tui import prompts as _prompts
        confirmed = _prompts.confirm("  Proceed?", default=False)
        if not confirmed:
            console.print(Text("  Cancelled.", style="muted"))
            _final_padding()
            return

    # 1. Tear down the compose stack if running — dbmazz must restart
    #    after clean so it re-runs setup (creates _DBMAZZ schema, audit cols).
    compose_path = _compose_path
    if compose_path.exists():
        try:
            if compose.is_running(compose_path):
                console.print(Text("  Stopping compose stack...", style="info"))
                compose.down(compose_path)
                console.print(Text("  ✓ Stack stopped", style="success"))
        except Exception:
            pass  # best-effort; stack might not be running

    # 2. Clean the target
    try:
        target = _instantiate_backend_from_spec(sk_spec)
        target.connect()
    except Exception as e:
        console.print(Text(f"  Failed to connect to target: {e}", style="error"))
        raise typer.Exit(2)

    try:
        actions = target.clean(tables)
    except Exception as e:
        console.print(Text(f"  Clean failed: {e}", style="error"))
        raise typer.Exit(1)
    finally:
        target.close()

    console.print()
    for action in actions:
        console.print(Text(f"  ✓ {action}", style="success"))
    console.print()
    console.print(Text("  Target is clean — ready for verify.", style="info"))
    _final_padding()


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
    console.print()


# _final_padding is a thin alias for tui.report.final_padding so existing
# call sites in this file keep working. New code should call
# `final_padding(console)` directly.
def _final_padding() -> None:
    final_padding(console)
