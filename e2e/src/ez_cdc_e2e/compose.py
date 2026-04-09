"""Docker compose wrapper.

Shells out to `docker compose` to manage the test stack. Streams output
to the terminal so the user can see build progress. All operations are
blocking; the caller is responsible for spinners / progress indicators.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Optional

from .paths import ensure_compose_file


class ComposeError(Exception):
    """Raised when a compose operation fails."""


def check_docker_compose() -> None:
    """Verify that `docker compose` is available. Raises if not."""
    if shutil.which("docker") is None:
        raise ComposeError(
            "docker is not installed or not in PATH. "
            "Install it from https://docs.docker.com/get-docker/"
        )

    # Try `docker compose version` (the modern subcommand).
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (subprocess.TimeoutExpired, OSError) as e:
        raise ComposeError(f"Failed to run 'docker compose version': {e}") from e

    if result.returncode != 0:
        raise ComposeError(
            "'docker compose' is not available. The old 'docker-compose' binary is "
            "not supported — install Docker Desktop or the compose plugin: "
            "https://docs.docker.com/compose/install/"
        )


def _base_cmd(profile: Optional[str] = None, env_file: Optional[Path] = None) -> list[str]:
    """Build the base `docker compose -f e2e/compose.yml [--profile X]` command."""
    cmd = ["docker", "compose", "-f", str(ensure_compose_file())]
    if env_file is not None:
        cmd += ["--env-file", str(env_file)]
    if profile is not None:
        cmd += ["--profile", profile]
    return cmd


def up(
    profile: str,
    env_file: Optional[Path] = None,
    wait: bool = True,
    build: bool = False,
) -> None:
    """Start the stack for a given compose profile.

    If `wait=True`, blocks until all services report healthy (via
    docker-compose's own --wait flag, which respects healthchecks defined
    in compose.yml).

    If `build=True`, passes `--build` to `docker compose up`, forcing a
    rebuild of the dbmazz image. **Default is False** so repeat runs are
    fast: docker compose will reuse the existing image if present, and
    only `ez-cdc up --rebuild` or `ez-cdc verify --rebuild` force a new
    build. Combined with the tight `.dockerignore` at the repo root
    (which excludes `e2e/`, `docs/`, etc. from the build context), this
    means Python-only edits to the test harness never trigger a Rust
    recompile.

    When you genuinely change Rust source or the Dockerfile, pass
    `--rebuild` explicitly.
    """
    check_docker_compose()

    cmd = _base_cmd(profile, env_file) + ["up", "-d"]
    if build:
        cmd.append("--build")
    if wait:
        cmd.append("--wait")

    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        raise ComposeError(
            f"docker compose up failed for profile '{profile}' (exit {result.returncode})"
        )


def down(profile: str, remove_volumes: bool = True) -> None:
    """Stop and remove the stack for a given profile.

    With remove_volumes=True (the default), also removes named volumes so
    each test run starts from a clean state.
    """
    check_docker_compose()

    cmd = _base_cmd(profile) + ["down"]
    if remove_volumes:
        cmd.append("-v")

    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        raise ComposeError(
            f"docker compose down failed for profile '{profile}' (exit {result.returncode})"
        )


def logs(profile: str, service: Optional[str] = None, follow: bool = False, tail: int = 100) -> None:
    """Stream logs for a profile (or a specific service within it)."""
    check_docker_compose()

    cmd = _base_cmd(profile) + ["logs", f"--tail={tail}"]
    if follow:
        cmd.append("-f")
    if service is not None:
        cmd.append(service)

    # Inherit stdio — user sees logs in real time, Ctrl+C stops.
    try:
        subprocess.run(cmd)
    except KeyboardInterrupt:
        pass


def ps(profile: str) -> str:
    """Return the output of `docker compose ps` for a profile (for status display)."""
    check_docker_compose()

    cmd = _base_cmd(profile) + ["ps"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout


def is_running(profile: str) -> bool:
    """Check whether any container for this profile is currently running."""
    check_docker_compose()

    cmd = _base_cmd(profile) + ["ps", "-q"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0 and result.stdout.strip() != ""
