"""Interactive live dashboard for the `ez-cdc quickstart` subcommand.

Shows:
  - current dbmazz stage (CDC / snapshot / paused / setup)
  - replication lag
  - confirmed LSN
  - events/sec, batches, memory, CPU
  - source → target row counts per table
  - throughput sparkline (last 60 samples)
  - interactive keybindings: q/Ctrl+C quit, l tail logs, p pause, r resume, s snapshot

Polls the dbmazz /status endpoint every 500 ms. Uses rich.Live for the display
and a background thread (via tui/keys.py) to capture keypresses without
requiring Enter.

The dashboard is meant for exploration by a human. It's NOT the place for
assertions or validations — that's the verify command.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from rich.console import Console, Group, RenderableType
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..backends.base import TargetBackend
from ..compose import logs as compose_logs
from ..dbmazz import DaemonStatus, DbmazzClient, DbmazzError
from ..load.generator import TrafficGenerator
from ..profiles import ProfileSpec
from ..tui.keys import KeyReader


# ── Sparkline ────────────────────────────────────────────────────────────────

_SPARK_CHARS = "▁▂▃▄▅▆▇█"


def _sparkline(values: list[float], width: int = 40) -> str:
    """Render a list of values as a unicode sparkline of the given width.

    Values are normalized to the full range of _SPARK_CHARS. Empty or
    single-value inputs render as spaces.
    """
    if not values:
        return " " * width

    # Take the last `width` values (pad left with zeros if fewer).
    tail = values[-width:]
    if len(tail) < width:
        tail = [0.0] * (width - len(tail)) + tail

    lo = min(tail)
    hi = max(tail)
    if hi == lo:
        return _SPARK_CHARS[0] * width

    out = []
    for v in tail:
        t = (v - lo) / (hi - lo)
        idx = min(int(t * (len(_SPARK_CHARS) - 1)), len(_SPARK_CHARS) - 1)
        out.append(_SPARK_CHARS[idx])
    return "".join(out)


# ── Stage presentation ──────────────────────────────────────────────────────

_STAGE_LABELS = {
    "cdc":       ("CDC", "replicating", "success"),
    "snapshot":  ("SNAPSHOT", "backfilling", "info"),
    "setup":     ("SETUP", "initializing", "warning"),
    "paused":    ("PAUSED", "idle", "warning"),
    "stopped":   ("STOPPED", "halted", "error"),
}


def _format_uptime(secs: int) -> str:
    hours = secs // 3600
    minutes = (secs % 3600) // 60
    seconds = secs % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _format_stage_indicator(status: DaemonStatus) -> Text:
    """Render a colored stage indicator like '● CDC  replicating'."""
    label, verb, style = _STAGE_LABELS.get(
        status.stage, (status.stage.upper(), "unknown", "muted")
    )
    t = Text()
    t.append("● ", style=style)
    t.append(label, style=f"bold {style}")
    t.append("  ")
    t.append(verb, style="muted")
    return t


# ── Table count state (for source ↔ target panel) ───────────────────────────

@dataclass
class TableCounts:
    source: int
    target: int


# ── Dashboard ───────────────────────────────────────────────────────────────

class QuickstartDashboard:
    """Interactive live dashboard.

    Starts a background `TrafficGenerator` for the lifetime of the dashboard
    so the user sees actual CDC activity (events/sec, changing counts,
    non-flat throughput) instead of a static "zero" display. The generator
    produces a realistic low-rate e-commerce workload (~15 eps of INSERTs,
    UPDATEs, and DELETEs) against the source PostgreSQL. It starts when
    run() is called and stops on exit via the finally block.

    The caller is responsible for:
      - having already called compose.up() and dbmazz.wait_for_stage("cdc")
      - connecting the source, target, and dbmazz clients
      - handling the confirm-and-teardown flow after run() returns
    """

    def __init__(
        self,
        profile: ProfileSpec,
        dbmazz: DbmazzClient,
        target: TargetBackend,
        console: Console,
        source_counts_fn,  # callable[[], dict[str, int]] — avoids coupling to SourceClient
        traffic_rate_eps: float = 15.0,
    ) -> None:
        self.profile = profile
        self.dbmazz = dbmazz
        self.target = target
        self.console = console
        self.source_counts_fn = source_counts_fn
        self.traffic_rate_eps = traffic_rate_eps

        self.start_time = time.time()
        self.throughput_history: list[float] = []
        self.should_exit = False
        self.status_message: Optional[str] = None
        self.status_message_ttl: float = 0.0

        # Background workload generator — started in run(), stopped in finally.
        self.traffic_generator: Optional[TrafficGenerator] = None

    # ── main loop ────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Run the dashboard until the user exits (q or Ctrl+C)."""
        # Start the background traffic generator so the dashboard shows
        # real activity instead of a frozen "zero" display.
        self.traffic_generator = TrafficGenerator(
            source_dsn=self.profile.source_dsn,
            rate_eps=self.traffic_rate_eps,
        )
        self.traffic_generator.start()

        try:
            with KeyReader() as keys:
                with Live(
                    self._render_loading(),
                    console=self.console,
                    refresh_per_second=2,
                    screen=True,
                    transient=True,
                ) as live:
                    last_render = 0.0
                    while not self.should_exit:
                        # Handle any queued keypresses first (snappier input).
                        self._drain_keys(keys, live)
                        if self.should_exit:
                            break

                        # Render at ~2 Hz (tick every 500ms).
                        now = time.time()
                        if now - last_render >= 0.5:
                            live.update(self._render_frame())
                            last_render = now

                        time.sleep(0.05)
        finally:
            # Stop the traffic generator before returning. Use a short timeout
            # so exit is snappy even if the source is hanging.
            if self.traffic_generator is not None:
                self.traffic_generator.stop(timeout=2.0)

    # ── key handling ─────────────────────────────────────────────────────────

    def _drain_keys(self, keys: KeyReader, live: Live) -> None:
        while True:
            key = keys.get_nowait()
            if key is None:
                return
            self._handle_key(key, live, keys)

    def _handle_key(self, key: str, live: Live, keys: KeyReader) -> None:
        if key in ("q", "Q", "\x03"):  # q or Ctrl+C
            self.should_exit = True
            return

        if key in ("l", "L"):
            self._tail_logs(live, keys)
            return

        if key in ("p", "P"):
            self._pause_daemon()
            return

        if key in ("r", "R"):
            self._resume_daemon()
            return

        if key in ("s", "S"):
            self._trigger_snapshot()
            return

    def _tail_logs(self, live: Live, keys: KeyReader) -> None:
        """Pause Live, tail docker logs, resume Live on any key."""
        live.stop()
        self.console.print()
        self.console.print(Text(
            "Tailing dbmazz logs. Press Ctrl+C to return to the dashboard.",
            style="info",
        ))
        self.console.print()

        # Drain any queued keys so a stale 'l' doesn't immediately return.
        keys.drain()

        try:
            compose_logs(self.profile.compose_profile, service=None, follow=True, tail=50)
        except KeyboardInterrupt:
            pass

        self.console.print()
        self.console.print(Text("Returning to dashboard...", style="muted"))
        live.start()

    def _pause_daemon(self) -> None:
        try:
            self.dbmazz.pause()
            self._set_status("Pause request sent.", ttl=3.0)
        except DbmazzError as e:
            self._set_status(f"Pause failed: {e}", ttl=5.0)

    def _resume_daemon(self) -> None:
        try:
            self.dbmazz.resume()
            self._set_status("Resume request sent.", ttl=3.0)
        except DbmazzError as e:
            self._set_status(f"Resume failed: {e}", ttl=5.0)

    def _trigger_snapshot(self) -> None:
        # Triggering a snapshot requires gRPC (CdcControlService/StartSnapshot).
        # PR 1 doesn't add a new HTTP endpoint for this, so we show a helpful
        # fallback message instead of pretending to trigger it.
        self._set_status(
            "Snapshot trigger requires grpcurl — run: "
            "grpcurl -plaintext -d '{}' localhost:50051 dbmazz.CdcControlService/StartSnapshot",
            ttl=6.0,
        )

    def _set_status(self, message: str, ttl: float) -> None:
        self.status_message = message
        self.status_message_ttl = time.time() + ttl

    # ── rendering ────────────────────────────────────────────────────────────

    def _render_loading(self) -> RenderableType:
        return Panel(
            Text("Loading dashboard...", style="info"),
            border_style="panel.border",
        )

    def _render_frame(self) -> RenderableType:
        """Build the full dashboard layout for one frame."""
        try:
            status = self.dbmazz.status()
        except DbmazzError:
            return self._render_disconnected()

        # Update throughput history (last 60 samples = ~30 seconds at 2 Hz).
        self.throughput_history.append(status.events_per_sec)
        if len(self.throughput_history) > 60:
            self.throughput_history = self.throughput_history[-60:]

        counts = self._fetch_counts()

        # Build layout.
        layout = Layout()
        layout.split_column(
            Layout(self._render_header(status), name="header", size=6),
            Layout(self._render_body(status, counts), name="body", ratio=1),
            Layout(self._render_footer(), name="footer", size=3),
        )
        return Panel(
            layout,
            title=self._render_panel_title(),
            border_style="panel.border",
            padding=(0, 1),
        )

    def _render_panel_title(self) -> Text:
        t = Text()
        t.append("EZ-CDC ", style="brand")
        t.append("•  ", style="muted")
        t.append(f"{self.profile.name} profile", style="default")
        t.append("  •  ", style="muted")
        uptime = _format_uptime(int(time.time() - self.start_time))
        t.append(f"uptime {uptime}", style="muted")
        t.append("  •  ", style="muted")
        t.append("q to quit", style="muted")
        return t

    def _render_disconnected(self) -> RenderableType:
        return Panel(
            Text(
                f"Cannot reach dbmazz at {self.profile.dbmazz_http_url}. "
                "Is the daemon still running?",
                style="error",
            ),
            title=Text("EZ-CDC", style="brand"),
            border_style="error",
        )

    def _render_header(self, status: DaemonStatus) -> RenderableType:
        """Stage indicator + lag + LSN + traffic generator status."""
        lines: list[RenderableType] = [Text("")]

        stage_line = Text()
        stage_line.append("  ")
        stage_line.append_text(_format_stage_indicator(status))
        stage_line.append(" " * max(1, 50 - len(stage_line)))
        stage_line.append("lag  ", style="metric.label")
        stage_line.append(f"{status.replication_lag_ms} ms", style="metric.number")
        lines.append(stage_line)

        lsn_line = Text()
        lsn_line.append(" " * 42)
        lsn_line.append("confirmed LSN  ", style="metric.label")
        lsn_line.append(status.confirmed_lsn, style="metric.number")
        lines.append(lsn_line)

        # Traffic generator indicator — tells the user WHY the numbers
        # are moving (otherwise the dashboard just looks busy for no
        # obvious reason on a fresh stack).
        if self.traffic_generator is not None and self.traffic_generator.is_running():
            gen_stats = self.traffic_generator.stats
            gen_line = Text()
            gen_line.append("  ")
            gen_line.append("● ", style="info")
            gen_line.append("traffic  ", style="metric.label")
            gen_line.append(
                f"generating ~{self.traffic_rate_eps:.0f} ops/s",
                style="metric.number",
            )
            gen_line.append(
                f"   ({gen_stats.inserts} ins, {gen_stats.updates} upd, "
                f"{gen_stats.deletes} del)",
                style="muted",
            )
            lines.append(gen_line)

        lines.append(Text(""))
        return Group(*lines)

    def _render_body(self, status: DaemonStatus, counts: dict[str, TableCounts]) -> RenderableType:
        """Pipeline panel + source→target panel side by side + sparkline below."""
        body = Layout()
        body.split_column(
            Layout(name="panels", ratio=2),
            Layout(self._render_throughput(), name="throughput", size=4),
        )

        panels_row = Layout()
        panels_row.split_row(
            Layout(self._render_pipeline_panel(status), name="pipeline", ratio=1),
            Layout(self._render_tables_panel(counts), name="tables", ratio=1),
        )
        body["panels"].update(panels_row)
        return body

    def _render_pipeline_panel(self, status: DaemonStatus) -> RenderableType:
        t = Table.grid(padding=(0, 2))
        t.add_column(style="metric.label", justify="left")
        t.add_column(style="metric.number", justify="right")

        t.add_row("Events/sec",     f"{status.events_per_sec:,.0f}")
        t.add_row("Events total",   f"{status.events_total:,}")
        t.add_row("Batches sent",   f"{status.batches_sent:,}")
        t.add_row("Memory (RSS)",   f"{status.memory_rss_mb:.1f} MB")
        t.add_row("CPU",            f"{status.cpu_millicores / 10:.1f} %")

        return Panel(
            t,
            title=Text("Pipeline", style="panel.header"),
            border_style="panel.border",
            padding=(1, 2),
        )

    def _render_tables_panel(self, counts: dict[str, TableCounts]) -> RenderableType:
        t = Table.grid(padding=(0, 2))
        t.add_column(style="metric.label", justify="left")
        t.add_column(style="metric.number", justify="right")
        t.add_column(style="metric.number", justify="right")

        t.add_row(
            Text("Table", style="metric.label"),
            Text("Source", style="metric.label"),
            Text("Target", style="metric.label"),
        )
        t.add_row(Text("─" * 12, style="dim"), Text("─" * 6, style="dim"), Text("─" * 6, style="dim"))

        for table_name, tc in counts.items():
            t.add_row(table_name, f"{tc.source:,}", f"{tc.target:,}")

        return Panel(
            t,
            title=Text("Source → Target", style="panel.header"),
            border_style="panel.border",
            padding=(1, 2),
        )

    def _render_throughput(self) -> RenderableType:
        spark = _sparkline(self.throughput_history, width=50)
        t = Text()
        t.append("  ")
        t.append("Throughput (last 30s)  ", style="metric.label")
        t.append(spark, style="brand")
        return Group(Text(""), t, Text(""))

    def _render_footer(self) -> RenderableType:
        # If there's a status message and it hasn't expired, show it; else keybindings.
        if self.status_message and time.time() < self.status_message_ttl:
            return Group(
                Text(""),
                Text(f"  {self.status_message}", style="info"),
                Text(""),
            )

        t = Text()
        t.append("  ")
        for key, label in [
            ("[q]", "quit"),
            ("[l]", "logs"),
            ("[p]", "pause"),
            ("[r]", "resume"),
            ("[s]", "snapshot"),
        ]:
            t.append(key, style="brand")
            t.append(f" {label}   ", style="muted")
        return Group(Text(""), t, Text(""))

    # ── data fetching ────────────────────────────────────────────────────────

    def _fetch_counts(self) -> dict[str, TableCounts]:
        """Fetch source + target row counts for each replicated table."""
        out: dict[str, TableCounts] = {}
        try:
            source_counts = self.source_counts_fn()
        except Exception:
            source_counts = {}

        for table in self.profile.tables:
            src = int(source_counts.get(table, 0))
            try:
                tgt = self.target.count_rows(table)
            except Exception:
                tgt = 0
            out[table] = TableCounts(source=src, target=tgt)

        return out
