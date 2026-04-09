"""Raw-mode stdin key reader for interactive dashboards.

Used by the quickstart dashboard to capture single keypresses like 'q',
'l', 'p', 'r', 's' without requiring Enter. Designed to coexist with
rich.Live: runs in a background thread and pushes keys to a queue that
the main render loop consumes.

Usage:

    with KeyReader() as keys:
        while running:
            # ... update display ...
            key = keys.get_nowait()
            if key == "q":
                break

The context manager handles raw-mode setup and restoration. If the
current process is not attached to a TTY (e.g., CI, piped output), the
KeyReader becomes a no-op and get_nowait() always returns None.
"""

from __future__ import annotations

import os
import queue
import select
import sys
import termios
import threading
import tty
from typing import Optional


class KeyReader:
    """Context manager that captures single keypresses in a background thread."""

    def __init__(self) -> None:
        self._queue: queue.Queue[str] = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._old_tty_attrs = None
        self._fd: Optional[int] = None
        self._active = False

    def __enter__(self) -> "KeyReader":
        if not sys.stdin.isatty():
            # Not a terminal — KeyReader becomes a no-op.
            return self

        try:
            self._fd = sys.stdin.fileno()
            self._old_tty_attrs = termios.tcgetattr(self._fd)
            tty.setcbreak(self._fd)  # cbreak, not raw — lets Ctrl+C still work
            self._active = True
        except (termios.error, OSError):
            # Failed to enter raw mode (e.g., stdin is a pipe) — no-op mode.
            self._active = False
            return self

        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._stop.set()

        if self._thread is not None:
            self._thread.join(timeout=0.5)

        if self._active and self._fd is not None and self._old_tty_attrs is not None:
            try:
                termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_tty_attrs)
            except (termios.error, OSError):
                pass

        self._active = False

    def _read_loop(self) -> None:
        """Background thread: read keys from stdin and push to the queue."""
        while not self._stop.is_set():
            try:
                # Use select with a short timeout so we can check the stop flag
                # periodically without blocking indefinitely on read().
                ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                if ready:
                    ch = os.read(sys.stdin.fileno(), 1)
                    if ch:
                        try:
                            self._queue.put_nowait(ch.decode("utf-8", errors="ignore"))
                        except queue.Full:
                            pass
            except (OSError, ValueError):
                # stdin closed or fd invalid — exit cleanly.
                break

    def get_nowait(self) -> Optional[str]:
        """Return the next queued key, or None if the queue is empty."""
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def drain(self) -> None:
        """Discard any queued keys."""
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break

    @property
    def active(self) -> bool:
        """True if the reader actually captured raw mode (TTY available)."""
        return self._active
