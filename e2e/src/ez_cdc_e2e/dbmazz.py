"""HTTP client for the dbmazz daemon.

The daemon exposes /healthz, /status, /pause, /resume, and metrics endpoints
when built with --features http-api. All test profiles use that build.

This client is synchronous (blocks on each request) because the verify
runner is sequential and the quickstart dashboard polls on a 500ms tick,
neither of which benefit from async.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import httpx


class DbmazzError(Exception):
    """Raised when a dbmazz request fails or returns unexpected data."""


@dataclass
class DaemonStatus:
    """Snapshot of the dbmazz daemon state from /status."""
    stage: str                          # "setup", "snapshot", "cdc", "paused", "stopped"
    uptime_secs: int
    confirmed_lsn: str                  # e.g., "0/1A3F8E2"
    current_lsn: str
    events_total: int
    events_per_sec: float
    batches_sent: int
    replication_lag_ms: int
    memory_rss_mb: float
    cpu_millicores: int
    snapshot_active: bool
    snapshot_chunks_total: int
    snapshot_chunks_done: int
    snapshot_rows_synced: int
    error_detail: Optional[str] = None

    @classmethod
    def from_api(cls, data: dict) -> "DaemonStatus":
        """Parse the /status JSON response into a DaemonStatus.

        Only extracts fields we care about, ignoring anything else. Missing
        fields get sensible defaults so the dashboard keeps rendering even
        while the daemon is still in setup phase.
        """
        return cls(
            stage=data.get("stage", "unknown"),
            uptime_secs=int(data.get("uptime_secs", 0)),
            confirmed_lsn=str(data.get("confirmed_lsn", "0/0")),
            current_lsn=str(data.get("current_lsn", "0/0")),
            events_total=int(data.get("events_total", 0)),
            events_per_sec=float(data.get("events_per_sec", 0.0)),
            batches_sent=int(data.get("batches_sent", 0)),
            replication_lag_ms=int(data.get("replication_lag_ms", 0)),
            memory_rss_mb=float(data.get("memory_rss_mb", 0.0)),
            cpu_millicores=int(data.get("cpu_millicores", 0)),
            snapshot_active=bool(data.get("snapshot_active", False)),
            snapshot_chunks_total=int(data.get("snapshot_chunks_total", 0)),
            snapshot_chunks_done=int(data.get("snapshot_chunks_done", 0)),
            snapshot_rows_synced=int(data.get("snapshot_rows_synced", 0)),
            error_detail=data.get("error_detail"),
        )


class DbmazzClient:
    """Synchronous HTTP client for the dbmazz daemon."""

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout)

    def __enter__(self) -> "DbmazzClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # ── endpoints ────────────────────────────────────────────────────────────

    def health(self) -> bool:
        """Return True if /healthz returns 200 and body indicates ok."""
        try:
            resp = self._client.get(f"{self.base_url}/healthz")
        except httpx.HTTPError:
            return False
        if resp.status_code != 200:
            return False
        try:
            data = resp.json()
        except ValueError:
            return False
        return data.get("status") == "ok"

    def status(self) -> DaemonStatus:
        """Fetch /status and return a DaemonStatus."""
        try:
            resp = self._client.get(f"{self.base_url}/status")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise DbmazzError(f"Failed to fetch /status from {self.base_url}: {e}") from e
        try:
            data = resp.json()
        except ValueError as e:
            raise DbmazzError(f"Invalid JSON from /status: {e}") from e
        return DaemonStatus.from_api(data)

    def pause(self) -> None:
        """POST /pause."""
        try:
            resp = self._client.post(f"{self.base_url}/pause")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise DbmazzError(f"Failed to pause daemon: {e}") from e

    def resume(self) -> None:
        """POST /resume."""
        try:
            resp = self._client.post(f"{self.base_url}/resume")
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise DbmazzError(f"Failed to resume daemon: {e}") from e

    # ── higher-level helpers ─────────────────────────────────────────────────

    def wait_for_stage(self, expected: str, timeout: float = 180.0, poll_interval: float = 1.0) -> DaemonStatus:
        """Poll /status until `stage == expected` or timeout expires.

        Returns the final DaemonStatus on success. Raises DbmazzError on timeout
        or on setup errors (error_detail set).
        """
        start = time.time()
        last_status: Optional[DaemonStatus] = None

        while time.time() - start < timeout:
            try:
                status = self.status()
            except DbmazzError:
                # Daemon may still be starting up — retry.
                time.sleep(poll_interval)
                continue

            last_status = status

            # Propagate setup errors immediately — no point in waiting 3 minutes
            # if the daemon is already in an error state.
            if status.error_detail:
                raise DbmazzError(f"dbmazz setup error: {status.error_detail}")

            if status.stage == expected:
                return status

            time.sleep(poll_interval)

        elapsed = time.time() - start
        if last_status is not None:
            raise DbmazzError(
                f"Timed out waiting for stage '{expected}' after {elapsed:.0f}s "
                f"(last stage: '{last_status.stage}')"
            )
        raise DbmazzError(
            f"Timed out waiting for stage '{expected}' after {elapsed:.0f}s "
            f"(daemon never responded to /status)"
        )

    def wait_healthy(self, timeout: float = 60.0, poll_interval: float = 1.0) -> None:
        """Poll /healthz until it returns ok, or timeout."""
        start = time.time()
        while time.time() - start < timeout:
            if self.health():
                return
            time.sleep(poll_interval)
        raise DbmazzError(f"dbmazz did not become healthy within {timeout:.0f}s")
