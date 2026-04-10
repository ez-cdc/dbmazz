"""Background traffic generator for the quickstart dashboard (and, later,
the `ez-cdc load` subcommand in PR 3).

Runs a thread that generates realistic e-commerce traffic against the
source PostgreSQL: INSERTs new orders with 1-3 order_items each, UPDATEs
the status of existing orders along a realistic lifecycle, and
occasionally DELETEs pending orders. The ratio of ops (70/25/5
insert/update/delete) is tuned to produce a healthy mix of CDC events
that exercises the pipeline without being insert-dominated.

The generator is **daemon-thread based** so the quickstart dashboard can
start it, let it run in the background during the lifetime of the
`rich.Live` display, and stop it cleanly when the user exits with `q`
or `Ctrl+C`. Rate and duration are configurable for PR 3's `load`
subcommand; the quickstart uses a low fixed rate (~15 eps) so the
dashboard visibly animates without overwhelming the source.

Connection failures are silent: if the source is unreachable when the
thread starts, the thread exits cleanly and `stats["errors"]` stays
zero. The dashboard will continue to render (just with zeros) rather
than crashing.
"""

from __future__ import annotations

import logging
import random
import threading
from dataclasses import dataclass, field
from typing import Optional

import psycopg2

log = logging.getLogger(__name__)


@dataclass
class GeneratorStats:
    """Snapshot of what the generator has produced so far.

    Counts are row-level ops issued to the source, not CDC events seen
    by the target. One INSERT produces 1 row in `orders` plus 1-3 rows
    in `order_items`, so the number of CDC events is higher than the
    `inserts` counter.
    """
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    errors: int = 0

    @property
    def total(self) -> int:
        return self.inserts + self.updates + self.deletes


class TrafficGenerator:
    """Thread-based e-commerce traffic generator against a PostgreSQL source.

    Not thread-safe for concurrent use from multiple callers; use one
    generator per quickstart / load session.
    """

    def __init__(self, source_dsn: str, rate_eps: float = 15.0) -> None:
        """
        Args:
            source_dsn: Postgres DSN reachable from the host (e.g.,
                "postgres://postgres:postgres@localhost:15432/dbmazz").
            rate_eps: Target ops per second. "Ops" means one INSERT or
                UPDATE or DELETE — the actual CDC event rate at the
                target is higher because each INSERT also creates
                1-3 order_items rows.
        """
        self.source_dsn = source_dsn
        self.rate_eps = max(rate_eps, 0.1)

        # stop_event: set to terminate the thread
        # pause_event: set to pause (thread keeps running but generates nothing)
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._stats = GeneratorStats()

    # ── lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Spawn the background thread. Safe to call multiple times — only the
        first call has effect."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="ez-cdc-traffic-gen",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """Signal the thread to stop and wait up to `timeout` seconds for it
        to exit. Safe to call multiple times."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # ── pause / resume ───────────────────────────────────────────────────────

    def pause(self) -> None:
        """Pause generation. The thread stays alive but stops emitting ops."""
        self._pause_event.set()

    def resume(self) -> None:
        """Resume generation after a pause."""
        self._pause_event.clear()

    def toggle(self) -> bool:
        """Toggle paused state. Returns the new state (True = now paused)."""
        if self._pause_event.is_set():
            self._pause_event.clear()
            return False
        self._pause_event.set()
        return True

    @property
    def is_paused(self) -> bool:
        return self._pause_event.is_set()

    # ── stats ────────────────────────────────────────────────────────────────

    @property
    def stats(self) -> GeneratorStats:
        """Return a snapshot of the current stats. Safe to call from any thread."""
        with self._lock:
            return GeneratorStats(
                inserts=self._stats.inserts,
                updates=self._stats.updates,
                deletes=self._stats.deletes,
                errors=self._stats.errors,
            )

    def _bump(self, field_name: str) -> None:
        with self._lock:
            current = getattr(self._stats, field_name)
            setattr(self._stats, field_name, current + 1)

    # ── worker loop ──────────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        """Background worker. Opens a single connection, loops until stop_event
        is set, and closes on exit.

        On connection failure, logs and exits — the dashboard will still
        render (the source_counts_fn in the dashboard will also fail and
        report zero source rows, which is a reasonable failure mode).
        """
        try:
            conn = psycopg2.connect(self.source_dsn)
            conn.autocommit = True
        except psycopg2.Error as e:
            log.warning("TrafficGenerator: failed to connect to source: %s", e)
            return

        sleep_time = 1.0 / self.rate_eps

        try:
            with conn.cursor() as cur:
                while not self._stop_event.is_set():
                    # If paused, idle until either resumed or stopped. Poll
                    # every 100 ms so stop() is still snappy.
                    while self._pause_event.is_set() and not self._stop_event.is_set():
                        if self._stop_event.wait(0.1):
                            break
                    if self._stop_event.is_set():
                        break

                    r = random.random()
                    try:
                        if r < 0.70:
                            self._do_insert(cur)
                            self._bump("inserts")
                        elif r < 0.95:
                            ok = self._do_update(cur)
                            if ok:
                                self._bump("updates")
                        else:
                            ok = self._do_delete(cur)
                            if ok:
                                self._bump("deletes")
                    except psycopg2.Error as e:
                        log.debug("TrafficGenerator op failed: %s", e)
                        self._bump("errors")
                        # Try to recover the transaction if one is stuck.
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                    # Use Event.wait so stop() wakes us up immediately
                    # instead of finishing the current sleep cycle.
                    if self._stop_event.wait(sleep_time):
                        break
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ── ops ──────────────────────────────────────────────────────────────────

    def _do_insert(self, cur) -> None:
        """Insert one order plus 1-3 order_items (simulates a shopping cart checkout)."""
        customer_id = random.randint(1, 1000)
        total = round(random.uniform(10, 500), 2)
        # Use short status values — see verify/tier1.py comment about VARCHAR(20).
        status = random.choice(["pending", "processing", "shipped"])
        cur.execute(
            "INSERT INTO orders (customer_id, total, status) VALUES (%s, %s, %s) RETURNING id",
            (customer_id, total, status),
        )
        row = cur.fetchone()
        if not row:
            return
        order_id = row[0]

        n_items = random.randint(1, 3)
        for _ in range(n_items):
            cur.execute(
                "INSERT INTO order_items (order_id, product_name, quantity, price) "
                "VALUES (%s, %s, %s, %s)",
                (
                    order_id,
                    f"Product {random.randint(1, 100)}",
                    random.randint(1, 5),
                    round(random.uniform(5, 100), 2),
                ),
            )

    def _do_update(self, cur) -> bool:
        """Advance one random non-delivered order along its lifecycle.

        Returns True if an update was applied, False if nothing to update.
        """
        transitions = {
            "pending":    "processing",
            "processing": "shipped",
            "shipped":    "delivered",
        }
        cur.execute(
            "SELECT id, status FROM orders "
            "WHERE status != 'delivered' "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return False
        order_id, current_status = row[0], row[1]
        new_status = transitions.get(current_status, "processing")
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
            (new_status, order_id),
        )
        return True

    def _do_delete(self, cur) -> bool:
        """Delete one random pending order (simulates cart abandonment).

        Returns True if a row was deleted, False if nothing to delete.
        """
        cur.execute(
            "DELETE FROM orders "
            "WHERE id = (SELECT id FROM orders WHERE status = 'pending' "
            "            ORDER BY random() LIMIT 1)"
        )
        return cur.rowcount > 0
