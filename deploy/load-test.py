#!/usr/bin/env python3
"""
dbmazz Load Test — unified traffic generator + replication monitor.

Usage:
    deploy/load-test.py pg-target                    # default: 500 eps, 60s
    deploy/load-test.py pg-target --rate 3000 --duration 120
    deploy/load-test.py starrocks --rate 1000

Requires: pip install psycopg2-binary rich requests
"""

import argparse
import json
import random
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Event, Lock
from urllib.request import Request, urlopen
from urllib.error import URLError

try:
    import psycopg2
except ImportError:
    print("Missing dependency: pip install psycopg2-binary")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.text import Text
    from rich.columns import Columns
except ImportError:
    print("Missing dependency: pip install rich")
    sys.exit(1)

# =============================================================================
# Profile configuration
# =============================================================================

PROFILES = {
    "pg-target": {
        "source_dsn": "postgres://postgres:postgres@localhost:15432/dbmazz",
        "target_dsn": "postgres://postgres:postgres@localhost:25432/dbmazz_target",
        "target_type": "postgres",
        "dbmazz_api": "http://localhost:8080",
        "description": "PostgreSQL -> PostgreSQL",
    },
    "starrocks": {
        "source_dsn": "postgres://postgres:postgres@localhost:15432/dbmazz",
        "target_dsn": None,  # StarRocks — count via dbmazz API
        "target_type": "starrocks",
        "dbmazz_api": "http://localhost:8080",
        "description": "PostgreSQL -> StarRocks",
    },
}

# =============================================================================
# Traffic generator
# =============================================================================

class TrafficGenerator:
    def __init__(self, source_dsn: str, rate: int):
        self.source_dsn = source_dsn
        self.rate = rate
        self.num_workers = max(1, rate // 200)
        self.stop_event = Event()
        self.lock = Lock()
        self.stats = {
            "ops": 0,
            "events": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
        }
        self._last_events = 0
        self._last_ts = time.time()
        self.current_eps = 0

    def _connect(self):
        while not self.stop_event.is_set():
            try:
                conn = psycopg2.connect(self.source_dsn)
                conn.autocommit = True
                return conn
            except Exception:
                time.sleep(1)
        return None

    def _do_insert(self, cur):
        customer_id = random.randint(1, 1000)
        total = round(random.uniform(10, 500), 2)
        cur.execute(
            "INSERT INTO orders (customer_id, total, status) VALUES (%s, %s, 'pending') RETURNING id",
            (customer_id, total),
        )
        order_id = cur.fetchone()[0]
        n_items = random.randint(1, 3)
        for _ in range(n_items):
            cur.execute(
                "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES (%s, %s, %s, %s)",
                (order_id, f"Product {random.randint(1,100)}", random.randint(1, 5), round(random.uniform(5, 100), 2)),
            )
        return 1 + n_items

    def _do_update(self, cur):
        transitions = {"pending": "processing", "processing": "shipped", "shipped": "delivered"}
        cur.execute("SELECT id, status FROM orders WHERE status != 'delivered' ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return 0
        new_status = transitions.get(row[1], "processing")
        cur.execute("UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s", (new_status, row[0]))
        return 1

    def _do_delete(self, cur):
        cur.execute(
            "DELETE FROM orders WHERE id IN (SELECT id FROM orders WHERE status = 'pending' ORDER BY random() LIMIT 1) RETURNING id"
        )
        return 1 if cur.fetchone() else 0

    def _worker(self, worker_id: int, ops_per_sec: int):
        conn = self._connect()
        if not conn:
            return
        cur = conn.cursor()
        sleep_time = 1.0 / max(1, ops_per_sec)

        try:
            while not self.stop_event.is_set():
                start = time.time()
                try:
                    r = random.random()
                    if r < 0.70:
                        events = self._do_insert(cur)
                        op = "inserts"
                    elif r < 0.95:
                        events = self._do_update(cur)
                        op = "updates"
                    else:
                        events = self._do_delete(cur)
                        op = "deletes"

                    with self.lock:
                        self.stats["ops"] += 1
                        self.stats["events"] += events
                        self.stats[op] += 1
                except Exception:
                    with self.lock:
                        self.stats["errors"] += 1

                elapsed = time.time() - start
                remaining = sleep_time - elapsed
                if remaining > 0:
                    time.sleep(remaining)
        finally:
            cur.close()
            conn.close()

    def start(self, executor: ThreadPoolExecutor):
        target_ops = max(1, self.rate // 3)  # ~3 events per op average
        ops_per_worker = max(1, target_ops // self.num_workers)
        for i in range(self.num_workers):
            executor.submit(self._worker, i, ops_per_worker)

    def stop(self):
        self.stop_event.set()

    def snapshot(self):
        now = time.time()
        with self.lock:
            events = self.stats["events"]
            dt = now - self._last_ts
            if dt > 0:
                self.current_eps = (events - self._last_events) / dt
            self._last_events = events
            self._last_ts = now
            return dict(self.stats), self.current_eps


# =============================================================================
# Metrics collector
# =============================================================================

class MetricsCollector:
    def __init__(self, profile: dict):
        self.profile = profile
        self.dbmazz_api = profile["dbmazz_api"]
        self.source_dsn = profile["source_dsn"]
        self.target_dsn = profile.get("target_dsn")
        self.target_type = profile["target_type"]

    def fetch_dbmazz_status(self) -> dict:
        try:
            req = Request(f"{self.dbmazz_api}/status", headers={"Accept": "application/json"})
            with urlopen(req, timeout=2) as resp:
                return json.loads(resp.read())
        except Exception:
            return {}

    def fetch_counts(self) -> dict:
        """Returns {table: {source: N, target: N}} for all tables."""
        counts = {}
        try:
            conn = psycopg2.connect(self.source_dsn)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename NOT LIKE 'dbmazz_%'"
            )
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur.execute(f'SELECT count(*) FROM "{t}"')
                counts[t] = {"source": cur.fetchone()[0], "target": 0}
            cur.close()
            conn.close()
        except Exception:
            return counts

        if self.target_dsn and self.target_type == "postgres":
            try:
                conn = psycopg2.connect(self.target_dsn)
                conn.autocommit = True
                cur = conn.cursor()
                for t in counts:
                    try:
                        cur.execute(f'SELECT count(*) FROM "{t}"')
                        counts[t]["target"] = cur.fetchone()[0]
                    except Exception:
                        pass
                cur.close()
                conn.close()
            except Exception:
                pass

        return counts


# =============================================================================
# Dashboard renderer
# =============================================================================

class Dashboard:
    def __init__(self, profile_name: str, profile: dict, rate: int, duration: int, num_workers: int):
        self.profile_name = profile_name
        self.profile = profile
        self.rate = rate
        self.duration = duration
        self.num_workers = num_workers
        self.start_time = time.time()
        self.peak_eps = 0
        self.peak_lag = 0

    def render(self, gen_stats: dict, gen_eps: float, dbmazz: dict, counts: dict) -> Layout:
        elapsed = time.time() - self.start_time
        remaining = max(0, self.duration - elapsed)
        self.peak_eps = max(self.peak_eps, gen_eps)

        lag_ms = dbmazz.get("replication_lag_ms", 0)
        self.peak_lag = max(self.peak_lag, lag_ms)

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )
        layout["body"].split_row(
            Layout(name="left", ratio=3),
            Layout(name="right", ratio=2),
        )

        # -- Header --
        elapsed_str = f"{int(elapsed)}s"
        remaining_str = f"{int(remaining)}s" if self.duration < 86400 else "continuous"
        header_text = Text(justify="center")
        header_text.append("dbmazz load test", style="bold")
        header_text.append(f"  |  {self.profile['description']}  |  ", style="dim")
        header_text.append(f"elapsed {elapsed_str}", style="cyan")
        header_text.append("  remaining ", style="dim")
        header_text.append(remaining_str, style="cyan")
        layout["header"].update(Panel(header_text, style="blue"))

        # -- Left: traffic + replication table --
        left_layout = Layout()
        left_layout.split_column(
            Layout(name="traffic", size=12),
            Layout(name="replication"),
        )

        # Traffic stats
        traffic_table = Table(show_header=True, header_style="bold cyan", expand=True, title="Traffic Generator")
        traffic_table.add_column("Metric", style="white", ratio=2)
        traffic_table.add_column("Value", justify="right", style="green", ratio=1)

        eps_color = "green" if gen_eps > 0 else "red"
        traffic_table.add_row("Target rate", f"{self.rate:,} eps")
        traffic_table.add_row("Current rate", f"[{eps_color}]{gen_eps:,.0f} eps[/{eps_color}]")
        traffic_table.add_row("Peak rate", f"{self.peak_eps:,.0f} eps")
        traffic_table.add_row("Total events", f"{gen_stats.get('events', 0):,}")
        traffic_table.add_row("Operations", f"{gen_stats.get('ops', 0):,}")
        i, u, d = gen_stats.get("inserts", 0), gen_stats.get("updates", 0), gen_stats.get("deletes", 0)
        total_ops = i + u + d or 1
        traffic_table.add_row(
            "Distribution",
            f"I:{i/total_ops*100:.0f}% U:{u/total_ops*100:.0f}% D:{d/total_ops*100:.0f}%",
        )
        errors = gen_stats.get("errors", 0)
        if errors > 0:
            traffic_table.add_row("Errors", f"[red]{errors:,}[/red]")
        traffic_table.add_row("Workers", f"{self.num_workers}")
        left_layout["traffic"].update(Panel(traffic_table, border_style="cyan"))

        # Replication table (source vs target counts)
        rep_table = Table(show_header=True, header_style="bold yellow", expand=True, title="Row Counts (source vs target)")
        rep_table.add_column("Table", style="white")
        rep_table.add_column("Source", justify="right", style="green")
        rep_table.add_column("Target", justify="right", style="yellow")
        rep_table.add_column("Diff", justify="right")

        total_src, total_tgt = 0, 0
        for table, c in sorted(counts.items()):
            src, tgt = c["source"], c["target"]
            total_src += src
            total_tgt += tgt
            diff = src - tgt
            if diff == 0:
                diff_str = "[green]0[/green]"
            elif diff < 100:
                diff_str = f"[yellow]{diff:,}[/yellow]"
            else:
                diff_str = f"[red]{diff:,}[/red]"
            rep_table.add_row(table, f"{src:,}", f"{tgt:,}", diff_str)

        if len(counts) > 1:
            total_diff = total_src - total_tgt
            if total_diff == 0:
                td_str = "[green]0[/green]"
            else:
                td_str = f"[yellow]{total_diff:,}[/yellow]"
            rep_table.add_row("[bold]TOTAL[/bold]", f"[bold]{total_src:,}[/bold]", f"[bold]{total_tgt:,}[/bold]", td_str)

        left_layout["replication"].update(Panel(rep_table, border_style="yellow"))
        layout["left"].update(left_layout)

        # -- Right: dbmazz engine metrics --
        engine_table = Table(show_header=True, header_style="bold magenta", expand=True, title="dbmazz Engine")
        engine_table.add_column("Metric", style="white", ratio=2)
        engine_table.add_column("Value", justify="right", style="magenta", ratio=1)

        stage = dbmazz.get("stage", "?")
        state = dbmazz.get("state", "?")
        stage_style = "green" if stage == "cdc" else "yellow" if stage == "snapshot" else "red"
        state_style = "green" if state == "running" else "yellow"

        engine_table.add_row("Stage", f"[{stage_style}]{stage}[/{stage_style}]")
        engine_table.add_row("State", f"[{state_style}]{state}[/{state_style}]")
        engine_table.add_row("", "")

        dbmazz_eps = dbmazz.get("events_per_second", 0)
        eps_style = "green" if dbmazz_eps > 0 else "dim"
        engine_table.add_row("Throughput", f"[{eps_style}]{dbmazz_eps:,} eps[/{eps_style}]")
        engine_table.add_row("Events processed", f"{dbmazz.get('events_processed', 0):,}")
        engine_table.add_row("Batches sent", f"{dbmazz.get('batches_sent', 0):,}")
        engine_table.add_row("Pending events", f"{dbmazz.get('pending_events', 0):,}")
        engine_table.add_row("", "")

        if lag_ms == 0:
            lag_str = "[green]0ms[/green]"
        elif lag_ms < 1000:
            lag_str = f"[green]{lag_ms:,}ms[/green]"
        elif lag_ms < 5000:
            lag_str = f"[yellow]{lag_ms:,}ms[/yellow]"
        else:
            lag_str = f"[red]{lag_ms:,}ms[/red]"
        engine_table.add_row("Replication lag", lag_str)
        peak_lag_str = f"{self.peak_lag:,}ms"
        engine_table.add_row("Peak lag", peak_lag_str)
        engine_table.add_row("", "")

        engine_table.add_row("Current LSN", f"[dim]{dbmazz.get('current_lsn', '?')}[/dim]")
        engine_table.add_row("Confirmed LSN", f"[dim]{dbmazz.get('confirmed_lsn', '?')}[/dim]")
        engine_table.add_row("Uptime", f"{dbmazz.get('uptime_secs', 0)}s")

        layout["right"].update(Panel(engine_table, border_style="magenta"))

        # -- Footer --
        footer_text = Text(justify="center")
        footer_text.append("Ctrl+C to stop", style="dim")
        footer_text.append("  |  ", style="dim")
        footer_text.append(f"profile: {self.profile_name}", style="dim cyan")
        footer_text.append(f"  |  source: localhost:15432", style="dim")
        footer_text.append(f"  |  dbmazz: localhost:8080", style="dim")
        layout["footer"].update(Panel(footer_text, style="blue"))

        return layout

    def render_summary(self, gen_stats: dict, gen_eps: float, dbmazz: dict, counts: dict) -> Panel:
        elapsed = time.time() - self.start_time
        total_events = gen_stats.get("events", 0)
        avg_eps = total_events / elapsed if elapsed > 0 else 0
        lag_ms = dbmazz.get("replication_lag_ms", 0)

        total_src = sum(c["source"] for c in counts.values())
        total_tgt = sum(c["target"] for c in counts.values())
        diff = total_src - total_tgt

        lines = []
        lines.append("")
        lines.append(f"  [bold]Duration[/bold]          {elapsed:.1f}s")
        lines.append(f"  [bold]Events generated[/bold]  {total_events:,}")
        lines.append(f"  [bold]Avg throughput[/bold]    {avg_eps:,.0f} eps")
        lines.append(f"  [bold]Peak throughput[/bold]   {self.peak_eps:,.0f} eps")
        lines.append("")
        lines.append(f"  [bold]Operations[/bold]        I:{gen_stats.get('inserts',0):,}  U:{gen_stats.get('updates',0):,}  D:{gen_stats.get('deletes',0):,}")
        errors = gen_stats.get("errors", 0)
        if errors:
            lines.append(f"  [bold]Errors[/bold]            [red]{errors:,}[/red]")
        lines.append("")
        lines.append(f"  [bold]Source rows[/bold]       {total_src:,}")
        lines.append(f"  [bold]Target rows[/bold]       {total_tgt:,}")

        if diff == 0:
            lines.append(f"  [bold]Row diff[/bold]          [green]0 (in sync)[/green]")
        else:
            lines.append(f"  [bold]Row diff[/bold]          [yellow]{diff:,} (may still be flushing)[/yellow]")

        lines.append("")
        lines.append(f"  [bold]Final lag[/bold]         {lag_ms:,}ms")
        lines.append(f"  [bold]Peak lag[/bold]          {self.peak_lag:,}ms")
        lines.append(f"  [bold]dbmazz throughput[/bold]  {dbmazz.get('events_per_second', 0):,} eps")
        lines.append(f"  [bold]Batches sent[/bold]      {dbmazz.get('batches_sent', 0):,}")
        lines.append("")

        return Panel("\n".join(lines), title="[bold]Load Test Summary[/bold]", border_style="green", expand=False)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="dbmazz load test — generate traffic and monitor replication",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  deploy/load-test.py pg-target\n"
               "  deploy/load-test.py pg-target --rate 3000 --duration 120\n"
               "  deploy/load-test.py starrocks --rate 1000\n",
    )
    parser.add_argument("profile", choices=PROFILES.keys(), help="docker-compose profile")
    parser.add_argument("--rate", type=int, default=500, help="target events/sec (default: 500)")
    parser.add_argument("--duration", type=int, default=60, help="duration in seconds (default: 60)")

    args = parser.parse_args()
    profile = PROFILES[args.profile]
    console = Console()

    # Verify dbmazz is reachable
    console.print()
    console.print("[bold]Preflight checks[/bold]")
    try:
        req = Request(f"{profile['dbmazz_api']}/healthz", headers={"Accept": "application/json"})
        with urlopen(req, timeout=3) as resp:
            health = json.loads(resp.read())
        stage = health.get("stage", "unknown")
        console.print(f"  [green]OK[/green]  dbmazz reachable (stage: {stage})")
    except Exception as e:
        console.print(f"  [red]FAIL[/red]  Cannot reach dbmazz at {profile['dbmazz_api']}: {e}")
        console.print(f"  Run: docker compose -f deploy/docker-compose.yml --profile {args.profile} up -d")
        sys.exit(1)

    # Verify source PG
    try:
        conn = psycopg2.connect(profile["source_dsn"])
        conn.close()
        console.print(f"  [green]OK[/green]  Source PostgreSQL reachable")
    except Exception as e:
        console.print(f"  [red]FAIL[/red]  Cannot connect to source: {e}")
        sys.exit(1)

    # Verify target
    if profile.get("target_dsn"):
        try:
            conn = psycopg2.connect(profile["target_dsn"])
            conn.close()
            console.print(f"  [green]OK[/green]  Target PostgreSQL reachable")
        except Exception as e:
            console.print(f"  [red]FAIL[/red]  Cannot connect to target: {e}")
            sys.exit(1)

    console.print()

    # Init components
    gen = TrafficGenerator(profile["source_dsn"], args.rate)
    metrics = MetricsCollector(profile)
    dashboard = Dashboard(args.profile, profile, args.rate, args.duration, gen.num_workers)

    # Handle Ctrl+C
    stop = Event()
    def on_signal(sig, frame):
        stop.set()
        gen.stop()
    signal.signal(signal.SIGINT, on_signal)

    # Start traffic
    executor = ThreadPoolExecutor(max_workers=gen.num_workers + 2)
    gen.start(executor)

    # Live dashboard loop
    last_dbmazz = {}
    last_counts = {}

    try:
        with Live(console=console, refresh_per_second=2, screen=True) as live:
            while not stop.is_set():
                elapsed = time.time() - dashboard.start_time
                if elapsed >= args.duration:
                    break

                gen_stats, gen_eps = gen.snapshot()
                last_dbmazz = metrics.fetch_dbmazz_status()
                last_counts = metrics.fetch_counts()

                rendered = dashboard.render(gen_stats, gen_eps, last_dbmazz, last_counts)
                live.update(rendered)
                time.sleep(0.5)
    except Exception:
        pass

    # Stop generator and wait for flush
    gen.stop()
    console.print()
    console.print("[dim]Stopping traffic generator, waiting for final flush...[/dim]")
    time.sleep(3)

    # Final metrics
    gen_stats, gen_eps = gen.snapshot()
    last_dbmazz = metrics.fetch_dbmazz_status()
    last_counts = metrics.fetch_counts()

    summary = dashboard.render_summary(gen_stats, gen_eps, last_dbmazz, last_counts)
    console.print(summary)

    executor.shutdown(wait=False)


if __name__ == "__main__":
    main()
