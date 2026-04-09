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
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.text import Text

from . import __version__, compose
from .backends.base import TargetBackend
from .compose import ComposeError
from .dbmazz import DbmazzClient, DbmazzError
from .profiles import (
    ProfileSpec,
    get_profile,
    list_profiles,
    list_runnable_profiles,
    load_backend_class,
)
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


def _main_menu() -> None:
    """Interactive main menu: banner A + top-level choice."""
    _show_banner_a()

    if not is_interactive():
        console.print(Text(
            "No subcommand given. Run `ez-cdc --help` to see available commands.",
            style="error",
        ))
        raise typer.Exit(3)

    choice = prompts.select(
        "What would you like to do?",
        choices=[
            {"name": "Quickstart",   "value": "quickstart", "description": "Try dbmazz with a sink"},
            {"name": "Verify",       "value": "verify",     "description": "Run e2e validation tests"},
            {"name": "Load test",    "value": "load",       "description": "Generate traffic + monitor"},
            {"name": "Compose",      "value": "compose",    "description": "Manage docker stack"},
            {"name": "Exit",         "value": "exit",       "description": ""},
        ],
        default="quickstart",
    )

    if choice is None or choice == "exit":
        return
    if choice == "quickstart":
        quickstart(sink=None)
    elif choice == "verify":
        verify(sink=None)
    elif choice == "load":
        console.print(Text("Load test is coming in PR 3 — not yet implemented.", style="warning"))
    elif choice == "compose":
        _compose_menu()


def _compose_menu() -> None:
    """Submenu for compose operations (up, down, logs, status)."""
    action = prompts.select(
        "Compose action:",
        choices=[
            {"name": "up",     "value": "up",     "description": "Start a stack"},
            {"name": "down",   "value": "down",   "description": "Stop and destroy a stack"},
            {"name": "logs",   "value": "logs",   "description": "Tail logs"},
            {"name": "status", "value": "status", "description": "Fetch current status"},
            {"name": "back",   "value": "back",   "description": ""},
        ],
    )
    if action in (None, "back"):
        return

    sink = _prompt_sink()
    if sink is None:
        return

    if action == "up":
        up(sink=sink)
    elif action == "down":
        down(sink=sink)
    elif action == "logs":
        logs(sink=sink)
    elif action == "status":
        status(sink=sink)


# ── Sink resolution helper ───────────────────────────────────────────────────

def _resolve_sink(sink: Optional[str]) -> ProfileSpec:
    """Resolve a sink name to a ProfileSpec, prompting if interactive and missing."""
    if sink is None:
        if is_interactive():
            sink = _prompt_sink()
            if sink is None:
                raise typer.Exit(130)
        else:
            console.print(Text(
                "Error: --sink is required in non-interactive mode.",
                style="error",
            ))
            raise typer.Exit(3)

    try:
        return get_profile(sink)
    except KeyError as e:
        console.print(Text(str(e), style="error"))
        raise typer.Exit(3)


def _prompt_sink() -> Optional[str]:
    """Interactive sink selection via questionary."""
    choices = [
        {
            "name": p.name,
            "value": p.name,
            "description": p.description,
        }
        for p in list_profiles()
    ]
    return prompts.select(
        "Which sink would you like to use?",
        choices=choices,
        default="starrocks",
    )


# ── Subcommand: quickstart ───────────────────────────────────────────────────

@app.command(help="Launch a sink and watch replication live in a terminal dashboard.")
def quickstart(
    sink: Optional[str] = typer.Argument(
        None, help="Sink profile: starrocks, pg-target, snowflake"
    ),
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
    profile = _resolve_sink(sink)

    # Start compose
    console.print()
    console.print(format_step(f"Starting compose profile: {profile.compose_profile}"))
    try:
        compose.up(
            profile.compose_profile,
            env_file=profile.requires_env_file if profile.requires_env_file and profile.requires_env_file.exists() else None,
            wait=True,
            build=rebuild,
        )
    except ComposeError as e:
        console.print(Text(f"Failed to start compose: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok(f"Starting compose profile: {profile.compose_profile}"))

    # Connect clients
    dbmazz_client = DbmazzClient(profile.dbmazz_http_url)
    source = PostgresSource(profile.source_dsn)
    source.connect()
    backend_cls = load_backend_class(profile)
    target = _instantiate_backend(backend_cls, profile)
    target.connect()

    # Wait for CDC stage
    console.print(format_step("Waiting for dbmazz to reach CDC stage (this may take a minute on first run)..."))
    try:
        dbmazz_client.wait_for_stage("cdc", timeout=180.0)
    except DbmazzError as e:
        console.print(Text(f"dbmazz did not reach CDC stage: {e}", style="error"))
        source.close()
        target.close()
        dbmazz_client.close()
        raise typer.Exit(2)
    console.print(format_step_ok("Waiting for dbmazz to reach CDC stage"))
    console.print()
    console.print(Text("Stack is live. Opening dashboard...", style="success"))
    console.print()

    # Run dashboard
    dashboard = QuickstartDashboard(
        profile=profile,
        dbmazz=dbmazz_client,
        target=target,
        console=console,
        source_counts_fn=lambda: {t: source.count_rows(t) for t in profile.tables},
    )
    try:
        dashboard.run()
    except KeyboardInterrupt:
        pass
    finally:
        source.close()
        target.close()
        dbmazz_client.close()

    console.print()
    # Confirm teardown
    if keep_up:
        console.print(Text(
            f"Stack left running. Run `ez-cdc down {profile.name}` to stop it.",
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
            compose.down(profile.compose_profile, remove_volumes=True)
        except ComposeError as e:
            console.print(Text(f"Failed to stop compose: {e}", style="error"))
            raise typer.Exit(2)
        console.print(Text("Stack destroyed.", style="muted"))
    else:
        console.print(Text(
            f"Stack left running. Run `ez-cdc down {profile.name}` to stop it.",
            style="muted",
        ))

    _print_thanks()


# ── Subcommand: verify ───────────────────────────────────────────────────────

@app.command(help="Run e2e validation tests for a sink (or all sinks with --all).")
def verify(
    sink: Optional[str] = typer.Argument(
        None, help="Sink profile. Omit with --all to run everything."
    ),
    quick: bool = typer.Option(False, "--quick", help="Tier 1 only (~30s per sink)."),
    all_sinks: bool = typer.Option(
        False, "--all", help="Run verify for all runnable sinks (auto-detects snowflake)."
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

    if all_sinks:
        _verify_all(quick=quick, skip_ids=skip_ids, json_report=json_report, keep_up=keep_up, no_up=no_up, rebuild=rebuild)
        return

    profile = _resolve_sink(sink)
    exit_code = _verify_one(
        profile,
        quick=quick,
        skip_ids=skip_ids,
        json_report=json_report,
        keep_up=keep_up,
        no_up=no_up,
        rebuild=rebuild,
    )
    raise typer.Exit(exit_code)


def _verify_one(
    profile: ProfileSpec,
    *,
    quick: bool,
    skip_ids: set[str],
    json_report: Optional[Path],
    keep_up: bool,
    no_up: bool,
    rebuild: bool,
) -> int:
    """Run verify for a single profile. Returns exit code."""
    console.print()
    console.print(
        Text(f"EZ-CDC e2e   •   profile: {profile.name}   •   tier: {'1' if quick else '1+2'}",
             style="brand")
    )
    console.print()

    # Up compose (unless --no-up)
    if not no_up:
        console.print(format_step(f"Starting compose profile: {profile.compose_profile}"))
        try:
            compose.up(
                profile.compose_profile,
                env_file=profile.requires_env_file if profile.requires_env_file and profile.requires_env_file.exists() else None,
                wait=True,
                build=rebuild,
            )
        except ComposeError as e:
            console.print(Text(f"Failed to start compose: {e}", style="error"))
            return 2
        console.print(format_step_ok(f"Starting compose profile: {profile.compose_profile}"))

    # Run the verify suite
    runner = VerifyRunner(
        profile=profile,
        console=console,
        quick=quick,
        skip_ids=skip_ids,
    )
    report = runner.run()

    # Print final summary
    console.print(format_totals(report))

    # Optional JSON report
    if json_report:
        json_report.parent.mkdir(parents=True, exist_ok=True)
        json_report.write_text(json.dumps(report_to_json(report), indent=2))
        console.print(Text(f"JSON report written to {json_report}", style="muted"))

    # Tear down (unless --keep-up)
    if not keep_up:
        console.print()
        console.print(format_step("Tearing down compose..."))
        try:
            compose.down(profile.compose_profile, remove_volumes=True)
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
    """Run verify for every runnable profile. Exits non-zero if any fail."""
    runnable = list_runnable_profiles()
    if not runnable:
        console.print(Text("No runnable profiles found.", style="error"))
        raise typer.Exit(2)

    # Report which profiles are in scope and which are skipped.
    skipped_profiles = [p for p in list_profiles() if p not in runnable]
    if skipped_profiles:
        console.print()
        for p in skipped_profiles:
            reason = (
                f"{p.requires_env_file} not found"
                if p.requires_env_file is not None
                else "not runnable"
            )
            console.print(Text(
                f"  ⊘  {p.name} skipped — {reason}",
                style="warning",
            ))

    aggregated: list[tuple[ProfileSpec, int]] = []
    for profile in runnable:
        # Each profile gets its own JSON report path (suffix the sink name).
        report_path: Optional[Path] = None
        if json_report is not None:
            report_path = json_report.with_stem(f"{json_report.stem}-{profile.name}")

        exit_code = _verify_one(
            profile,
            quick=quick,
            skip_ids=skip_ids,
            json_report=report_path,
            keep_up=keep_up,
            no_up=no_up,
            rebuild=rebuild,
        )
        aggregated.append((profile, exit_code))

    # Final aggregate summary
    console.print()
    console.print(Text("━" * 60, style="rule"))
    any_failed = any(code != 0 for _, code in aggregated)
    for p, code in aggregated:
        sym = "✓" if code == 0 else "✗"
        style = "pass" if code == 0 else "fail"
        console.print(Text(f"  {sym}  {p.name}", style=style))
    console.print(Text("━" * 60, style="rule"))
    console.print()

    raise typer.Exit(1 if any_failed else 0)


# ── Subcommand: up ───────────────────────────────────────────────────────────

@app.command(help="Start the compose stack for a sink.")
def up(
    sink: Optional[str] = typer.Argument(None, help="Sink profile."),
    rebuild: bool = typer.Option(
        False, "--rebuild",
        help="Force `docker compose up --build`. Default reuses the cached dbmazz image.",
    ),
) -> None:
    _show_banner_d()
    profile = _resolve_sink(sink)
    console.print()
    console.print(format_step(f"Starting compose profile: {profile.compose_profile}"))
    try:
        compose.up(
            profile.compose_profile,
            env_file=profile.requires_env_file if profile.requires_env_file and profile.requires_env_file.exists() else None,
            wait=True,
            build=rebuild,
        )
    except ComposeError as e:
        console.print(Text(f"Failed to start compose: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok(f"Starting compose profile: {profile.compose_profile}"))


# ── Subcommand: down ─────────────────────────────────────────────────────────

@app.command(help="Stop and destroy the compose stack for a sink.")
def down(
    sink: Optional[str] = typer.Argument(None, help="Sink profile."),
    keep_volumes: bool = typer.Option(
        False, "--keep-volumes", help="Keep named volumes (don't run with -v)."
    ),
) -> None:
    _show_banner_d()
    profile = _resolve_sink(sink)
    console.print()
    console.print(format_step(f"Stopping compose profile: {profile.compose_profile}"))
    try:
        compose.down(profile.compose_profile, remove_volumes=not keep_volumes)
    except ComposeError as e:
        console.print(Text(f"Failed to stop compose: {e}", style="error"))
        raise typer.Exit(2)
    console.print(format_step_ok(f"Stopping compose profile: {profile.compose_profile}"))


# ── Subcommand: logs ─────────────────────────────────────────────────────────

@app.command(help="Tail compose logs for a sink.")
def logs(
    sink: Optional[str] = typer.Argument(None, help="Sink profile."),
    follow: bool = typer.Option(True, "--follow/--no-follow", "-f", help="Stream logs."),
    tail: int = typer.Option(100, "--tail", "-n", help="Number of lines to show from the end."),
) -> None:
    _show_banner_d()
    profile = _resolve_sink(sink)
    console.print()
    try:
        compose.logs(profile.compose_profile, follow=follow, tail=tail)
    except ComposeError as e:
        console.print(Text(f"Failed to tail logs: {e}", style="error"))
        raise typer.Exit(2)


# ── Subcommand: status ───────────────────────────────────────────────────────

@app.command(help="Fetch a one-shot status snapshot from dbmazz.")
def status(
    sink: Optional[str] = typer.Argument(None, help="Sink profile."),
) -> None:
    _show_banner_d()
    profile = _resolve_sink(sink)
    client = DbmazzClient(profile.dbmazz_http_url)
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

def _instantiate_backend(backend_cls, profile: ProfileSpec) -> TargetBackend:
    """Build a target backend from profile + env (kept in sync with verify/runner.py)."""
    if profile.name == "pg-target":
        return backend_cls(
            dsn="postgres://postgres:postgres@localhost:25432/dbmazz_target",
            schema="public",
        )
    if profile.name == "starrocks":
        return backend_cls(
            host="localhost",
            port=9030,
            user="root",
            password="",
            database="dbmazz",
        )
    if profile.name == "snowflake":
        return backend_cls(env_file=profile.requires_env_file)
    raise ValueError(f"unknown profile: {profile.name}")


def _print_thanks() -> None:
    console.print()
    t = Text()
    t.append("  Thanks for trying EZ-CDC.   →   ", style="muted")
    t.append("https://ez-cdc.com", style="brand")
    console.print(t)
    console.print()
