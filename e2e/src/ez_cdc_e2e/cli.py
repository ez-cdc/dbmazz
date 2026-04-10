"""ez-cdc CLI entry point.

Typer app with subcommands:

    ez-cdc                              → interactive main menu (banner A)
    ez-cdc quickstart --source/--sink   → launch dashboard (banner A)
    ez-cdc verify     --source/--sink   → run validation tests (banner D)
    ez-cdc up         --source/--sink   → start infra containers (banner D)
    ez-cdc down       --source/--sink   → stop compose stack (banner D)
    ez-cdc logs       --source/--sink   → tail compose logs (banner D)
    ez-cdc status     --source/--sink   → one-shot /status fetch (banner D)
    ez-cdc clean      --source/--sink   → clean target database (banner D)

Detects interactive TTY and prompts for missing args when possible. Falls
back to hard errors in non-interactive mode.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.text import Text

from . import __version__, compose
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
        infra_running = _is_infra_running()

        _print_menu_status(ds_status, infra_running)

        # Build choices dynamically based on state.
        choices: list[dict] = []
        config_exists = DEFAULT_CONFIG_PATH.exists()

        if not config_exists:
            # State 1: No ez-cdc.yaml
            choices = [
                {"name": "Init config", "value": "init_config",
                 "description": "Create ez-cdc.yaml with demo datasources"},
                {"name": "Datasources", "value": "datasources",
                 "description": "List / add / remove source and sink configs"},
                {"name": "Exit", "value": "exit", "description": ""},
            ]
            default = "init_config"
        elif infra_running:
            # State 3: Config exists, stack running
            choices = [
                {"name": "Quickstart", "value": "quickstart",
                 "description": "Open the live dashboard"},
                {"name": "Verify", "value": "verify",
                 "description": "Run e2e validation tests"},
                {"name": "Clean target", "value": "clean",
                 "description": "Truncate tables + drop audit columns + remove metadata"},
                {"name": "Logs", "value": "logs",
                 "description": "Tail infra container logs"},
                {"name": "Stop stack", "value": "stop_stack",
                 "description": "Stop all infra containers"},
                {"name": "Datasources", "value": "datasources",
                 "description": "List / add / remove source and sink configs"},
                {"name": "Exit", "value": "exit", "description": ""},
            ]
            default = "quickstart"
        else:
            # State 2: Config exists, no stack running
            choices = [
                {"name": "Start stack", "value": "start_stack",
                 "description": "Start all infra containers from ez-cdc.yaml"},
                {"name": "Quickstart", "value": "quickstart",
                 "description": "Open the live dashboard"},
                {"name": "Verify", "value": "verify",
                 "description": "Run e2e validation tests"},
                {"name": "Clean target", "value": "clean",
                 "description": "Truncate tables + drop audit columns + remove metadata"},
                {"name": "Datasources", "value": "datasources",
                 "description": "List / add / remove source and sink configs"},
                {"name": "Exit", "value": "exit", "description": ""},
            ]
            default = "start_stack"

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
            if choice == "init_config":
                store = _load_store_for_flow()
                _init_demos_inline(store)
            elif choice == "start_stack":
                up(rebuild=False)
            elif choice == "stop_stack":
                down()
            elif choice == "quickstart":
                quickstart(source=None, sink=None, keep_up=False, rebuild=False)
            elif choice == "verify":
                verify(
                    source=None, sink=None,
                    quick=False, all_pairs=False, skip=None,
                    json_report=None, keep_up=False, no_up=False, rebuild=False,
                )
            elif choice == "clean":
                clean(source=None, sink=None, yes=False)
            elif choice == "logs":
                logs()
            elif choice == "datasources":
                _datasources_menu()
        except _BackToMenu:
            pass
        except typer.Exit:
            # Subcommands signal completion/failure via typer.Exit.
            # In interactive menu mode we swallow ALL exit codes so the
            # user returns to the menu instead of being dumped to the shell.
            pass

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


def _is_infra_running() -> bool:
    """Check if the global infra compose stack has running containers."""
    try:
        from .compose_builder import infra_compose_path
        return compose.is_running(infra_compose_path())
    except (ComposeError, Exception):
        return False


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


def _print_menu_status(ds_status: dict, infra_running: bool) -> None:
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

    if infra_running:
        parts.append(Text(
            "  ● Infra stack running",
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


# ── Datasource pair resolution ──────────────────────────────────────────────

# Default config file location (configurable per-flow with --config).
from .paths import E2E_DIR  # noqa: E402
DEFAULT_CONFIG_PATH = E2E_DIR / "ez-cdc.yaml"


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

    In non-interactive mode, exits with a clear error. In interactive mode,
    offers to bootstrap the bundled demos as the simplest path forward.

    Raises _BackToMenu if the user cancels — caught by the main menu loop.
    """
    if store.has_any():
        return

    if not is_interactive():
        console.print(Text(
            f"Error: no datasources configured in {store.path}\n"
            f"  Run `ez-cdc` to set up, or `ez-cdc datasource init-demos` "
            f"to create the bundled demos.",
            style="error",
        ))
        raise typer.Exit(2)

    # Interactive: init demos as the simplest path.
    _init_demos_inline(store)


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
    source_name: Optional[str],
    sink_name: Optional[str],
    *,
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> tuple[str, SourceSpec, str, SinkSpec, Path, Path]:
    """Resolve a (source, sink) pair from flags / interactive picker.

    Drives the user through the gate (_ensure_datasources_or_setup) if no
    datasources are configured. Returns:

        (source_name, source_spec, sink_name, sink_spec, compose_path, env_path)

    Resolution order:
      1. Use the explicit --source/--sink flags if both given.
      2. Else, in interactive mode, prompt with selectors. In non-interactive
         mode, error out clearly.
    """
    store = _load_store_for_flow(config_path)

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
        return f"PG · {len(spec.tables)} tables"
    if isinstance(spec, PostgresSinkSpec):
        return f"PG ({_redact_url(spec.url)})"
    if isinstance(spec, StarRocksSinkSpec):
        return f"StarRocks ({_redact_url(spec.url)})"
    if isinstance(spec, SnowflakeSinkSpec):
        return f"Snowflake ({spec.account})"
    return spec.type


def _redact_url(url: str) -> str:
    """Redact credentials from a URL, keeping host:port."""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        host = parsed.hostname or "?"
        port = parsed.port
        return f"{host}:{port}" if port else host
    except Exception:
        return "..."


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
    source: Optional[str] = typer.Option(None, "--source", help="Source datasource name."),
    sink: Optional[str] = typer.Option(None, "--sink", help="Sink datasource name."),
    keep_up: bool = typer.Option(
        False, "--keep-up",
        help="Don't stop dbmazz on exit (leave it running).",
    ),
    rebuild: bool = typer.Option(
        False, "--rebuild",
        help="Force `docker compose up --build`. Default reuses the cached dbmazz image.",
    ),
) -> None:
    _show_banner_a()

    src_name, src_spec, sk_name, sk_spec, compose_path, _env_path = _resolve_pair(
        source, sink,
    )

    # Pre-flight: verify source + sink are reachable before starting dbmazz.
    from .preflight import run_preflight
    console.print()
    console.print(format_step("Running pre-flight checks..."))
    if not run_preflight(src_spec, sk_spec, console, src_name=src_name, sk_name=sk_name):
        console.print()
        console.print(Text(
            "  Fix the issues above, or run `ez-cdc up` if the stack isn't started yet.",
            style="error",
        ))
        raise typer.Exit(2)
    console.print(format_step_ok("Pre-flight checks passed"))

    # Stop any dbmazz from a different pair before starting this one
    # (only one dbmazz can run at a time — they share port 8080).
    _stop_other_dbmazz(compose_path)

    # Start dbmazz only (infra should already be running via `ez-cdc up`).
    console.print(format_step(f"Starting dbmazz: {src_name} → {sk_name}"))
    try:
        compose.up(compose_path, services=["dbmazz"], wait=True, build=rebuild, force_recreate=True)
    except ComposeError as e:
        console.print(Text(f"Failed to start dbmazz: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok(f"Starting dbmazz: {src_name} → {sk_name}"))

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
    # demo sources and user-managed sources (BYOD). For BYOD, the user is
    # responsible for the consequences of pressing `[t]` against their real
    # database — the dashboard surfaces a warning in the header so it's not
    # a silent surprise.
    dashboard = QuickstartDashboard(
        name=f"{src_name}-to-{sk_name}",
        source_dsn=src_spec.url,
        tables=tuple(src_spec.tables),
        compose_file=compose_path,
        dbmazz=dbmazz_client,
        target=target,
        console=console,
        source_counts_fn=lambda: {t: source_client.count_rows(t) for t in src_spec.tables},
        traffic_rate_eps=15.0,
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
    _print_thanks()


# ── Subcommand: verify ───────────────────────────────────────────────────────

@app.command(help="Run e2e validation tests for a source/sink pair (or all pairs with --all).")
def verify(
    source: Optional[str] = typer.Option(None, "--source", help="Source datasource name."),
    sink: Optional[str] = typer.Option(None, "--sink", help="Sink datasource name."),
    quick: bool = typer.Option(False, "--quick", help="Tier 1 only (~30s per pair)."),
    all_pairs: bool = typer.Option(
        False, "--all",
        help="Run verify for all source × sink pairs in your datasources.",
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
        source, sink,
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
    console.print(
        Text(
            f"EZ-CDC e2e   •   {src_name} → {sk_name}   •   "
            f"tier: {'1' if quick else '1+2'}",
            style="brand",
        )
    )
    console.print()

    if not no_up:
        # Pre-flight: verify source + sink are reachable before starting dbmazz.
        from .preflight import run_preflight
        console.print(format_step("Running pre-flight checks..."))
        if not run_preflight(src_spec, sk_spec, console, src_name=src_name, sk_name=sk_name):
            console.print()
            console.print(Text(
                "  Fix the issues above, or run `ez-cdc up` if the stack isn't started yet.",
                style="error",
            ))
            return 2
        console.print(format_step_ok("Pre-flight checks passed"))

        _stop_other_dbmazz(compose_path)

        console.print(format_step(f"Starting dbmazz: {src_name} → {sk_name}"))
        try:
            compose.up(compose_path, services=["dbmazz"], wait=True, build=rebuild, force_recreate=True)
        except ComposeError as e:
            console.print(Text(f"Failed to start dbmazz: {e}", style="error"))
            return 2
        console.print(format_step_ok(f"Starting dbmazz: {src_name} → {sk_name}"))

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
    """Run verify for every source × sink pair in the store."""
    store = _load_store_for_flow()
    _ensure_datasources_or_setup(store)
    store.reload()

    sources = store.list_sources()
    sinks = store.list_sinks()

    if not sources or not sinks:
        console.print(Text(
            "Error: --all needs at least one source AND one sink. "
            "Run `ez-cdc datasource init-demos` to bootstrap them.",
            style="error",
        ))
        raise typer.Exit(2)

    pairs: list[tuple[str, SourceSpec, str, SinkSpec]] = []
    for src_name in sources:
        for sk_name in sinks:
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


    return services


# ── Subcommand: up ───────────────────────────────────────────────────────────

@app.command(help="Start all infra containers (source + sink DBs) defined in ez-cdc.yaml.")
def up(
    rebuild: bool = typer.Option(
        False, "--rebuild",
        help="Force `docker compose up --build`. Default reuses cached images.",
    ),
) -> None:
    _show_banner_d()

    store = _load_store_for_flow()
    _ensure_datasources_or_setup(store)
    store.reload()

    from .compose_builder import build_infra_compose, list_infra_services

    services = list_infra_services(store.data.sources, store.data.sinks)
    if not services:
        console.print()
        console.print(Text(
            "  No infra containers needed — all datasources are remote.",
            style="muted",
        ))
        _final_padding()
        return

    compose_path = build_infra_compose(store.data.sources, store.data.sinks)

    console.print()
    console.print(format_step(f"Starting infra ({', '.join(services)})"))
    try:
        compose.up(compose_path, wait=True, build=rebuild)
    except ComposeError as e:
        console.print(Text(f"Failed to start infra: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok(f"Infra started ({', '.join(services)})"))

    # Pre-build the dbmazz image so the first quickstart/verify is fast.
    # We generate a throwaway pair compose just to get a valid Dockerfile
    # reference, then `docker compose build` without starting anything.
    console.print(format_step("Pre-building dbmazz image (cached for future runs)"))
    try:
        from .compose_builder import build_compose_for_pair
        src_names = store.list_sources()
        sk_names = store.list_sinks()
        if src_names and sk_names:
            src_spec = store.get_source(src_names[0])
            sk_spec = store.get_sink(sk_names[0])
            pair_compose, _ = build_compose_for_pair(
                src_names[0], src_spec, sk_names[0], sk_spec,
                settings=store.data.settings,
            )
            compose.build(pair_compose, services=["dbmazz"])
            console.print(format_step_ok("dbmazz image ready"))
    except Exception:
        console.print(Text("  dbmazz image will be built on first run.", style="muted"))

    _final_padding()


# ── Subcommand: down ─────────────────────────────────────────────────────────

@app.command(help="Stop and destroy all infra containers.")
def down(
    keep_volumes: bool = typer.Option(
        False, "--keep-volumes", help="Keep named volumes (don't run with -v)."
    ),
) -> None:
    _show_banner_d()

    from .compose_builder import infra_compose_path

    compose_path = infra_compose_path()
    if not compose_path.exists():
        console.print()
        console.print(Text("  No infra stack found. Nothing to stop.", style="muted"))
        _final_padding()
        return

    console.print()
    console.print(format_step("Stopping infra stack"))
    try:
        compose.down(compose_path, remove_volumes=not keep_volumes)
    except ComposeError as e:
        console.print(Text(f"Failed to stop infra: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok("Infra stack stopped"))
    _final_padding()


# ── Subcommand: logs ─────────────────────────────────────────────────────────

@app.command(help="Tail logs for the infra stack.")
def logs(
    follow: bool = typer.Option(True, "--follow/--no-follow", "-f", help="Stream logs."),
    tail: int = typer.Option(100, "--tail", "-n", help="Number of lines to show from the end."),
) -> None:
    _show_banner_d()

    from .compose_builder import infra_compose_path

    compose_path = infra_compose_path()
    if not compose_path.exists():
        console.print()
        console.print(Text("  No infra stack found. Run `ez-cdc up` first.", style="warning"))
        _final_padding()
        return

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
    source: Optional[str] = typer.Option(None, "--source"),
    sink: Optional[str] = typer.Option(None, "--sink"),
) -> None:
    _show_banner_d()
    # status doesn't actually need the spec — it just hits localhost:8080.
    # We still resolve the pair so we can show what's running, and so the
    # gate runs (i.e., the user gets a useful error if there are no datasources).
    _src_name, _src_spec, _sk_name, _sk_spec, _compose_path, _env_path = _resolve_pair(
        source, sink,
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
    source: Optional[str] = typer.Option(None, "--source", help="Source datasource name."),
    sink: Optional[str] = typer.Option(None, "--sink", help="Sink datasource name."),
    yes: bool = typer.Option(
        False, "--yes", "-y",
        help="Skip the confirmation prompt.",
    ),
) -> None:
    _show_banner_d()

    src_name, src_spec, sk_name, sk_spec, _compose_path, _env_path = _resolve_pair(
        source, sink,
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

    # Clean only touches the target database — it does NOT stop any
    # containers.  The user can re-run verify/quickstart after clean and
    # dbmazz will re-snapshot into the now-empty target.
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





def _stop_other_dbmazz(current_compose: Path) -> None:
    """Stop any dbmazz container from a different pair.

    Only one dbmazz can run at a time (they share port 8080). Before
    starting a new one, scan all pair cache dirs for a running dbmazz
    and stop it if it belongs to a different compose file.
    """
    from .compose_builder import CACHE_ROOT

    if not CACHE_ROOT.exists():
        return

    for pair_dir in CACHE_ROOT.iterdir():
        if not pair_dir.is_dir() or pair_dir.name.startswith("_"):
            continue  # skip _infra and non-directories
        other_compose = pair_dir / "compose.yml"
        if not other_compose.exists():
            continue
        if other_compose.resolve() == current_compose.resolve():
            continue  # same pair — will be force-recreated
        try:
            if compose.is_service_running(other_compose, "dbmazz"):
                compose.stop_services(other_compose, ["dbmazz"])
        except ComposeError:
            pass  # best effort


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
