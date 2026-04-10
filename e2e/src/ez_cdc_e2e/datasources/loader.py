"""YAML loader and variable interpolation for datasources files.

Pipeline:
  1. Read raw YAML text from disk.
  2. Optionally load `.env` files into `os.environ` (for `${VAR}` resolution).
  3. Walk every string value in the parsed YAML and substitute `${VAR}` with
     the corresponding env var. Missing vars raise InterpolationError unless
     a default is given via `${VAR:-default}`.
  4. Hand the substituted dict to pydantic for schema validation.

Errors are wrapped in DatasourceError subclasses with messages a CLI user
can act on (file path, line number when available, field hierarchy from
pydantic).

Variable interpolation syntax:
  ${VAR}              required — raises if VAR is not in environment
  ${VAR:-default}     optional — uses 'default' if VAR is unset or empty
  $$VAR or $${VAR}    literal $ — escape mechanism

This is intentionally minimal: no command substitution, no shell expansion,
no nested expansions. The YAML stays predictable.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from .schema import DatasourcesFile


# ── Exceptions ───────────────────────────────────────────────────────────────

class DatasourceError(Exception):
    """Base class for all datasource-related errors."""


class DatasourceNotFoundError(DatasourceError):
    """A named datasource was requested but doesn't exist in the file."""


class DatasourceValidationError(DatasourceError):
    """The YAML loaded but failed schema validation."""


class InterpolationError(DatasourceError):
    """A `${VAR}` reference couldn't be resolved."""


# ── Variable interpolation ───────────────────────────────────────────────────

# Match either ${VAR}, ${VAR:-default}, or the escape $${...} / $$VAR.
# We capture the optional default after `:-` so we can apply it.
_VAR_PATTERN = re.compile(
    r"""
    \$\$ (?: \{[^}]*\} | [A-Za-z_][A-Za-z0-9_]* )    # escape: $${VAR} or $$VAR
    |
    \$\{ (?P<name>[A-Za-z_][A-Za-z0-9_]*) (?: :- (?P<default>[^}]*) )? \}
    |
    \$ (?P<bare> [A-Za-z_][A-Za-z0-9_]* )            # bare $VAR (also supported)
    """,
    re.VERBOSE,
)


def interpolate_string(value: str, source_path: Path | None = None) -> str:
    """Substitute ${VAR} placeholders in a string.

    Args:
        value: the string to interpolate.
        source_path: file path used for error messages, if known.

    Raises:
        InterpolationError: when a referenced variable is not in os.environ
            and no default was supplied.
    """
    if "$" not in value:
        return value  # fast path

    def _replace(match: re.Match) -> str:
        full = match.group(0)
        # Escape sequence: $${...} or $$VAR — keep one literal $
        if full.startswith("$$"):
            return full[1:]  # strip the leading $

        var_name = match.group("name") or match.group("bare")
        default = match.group("default")

        if var_name is None:
            # Shouldn't happen given the regex, but defensive.
            return full

        env_value = os.environ.get(var_name)
        if env_value is None or env_value == "":
            if default is not None:
                return default
            location = f" in {source_path}" if source_path else ""
            raise InterpolationError(
                f"environment variable ${{{var_name}}} is not set{location}. "
                f"Either export it before running ez-cdc, add it to a .env file, "
                f"or provide a default with ${{{var_name}:-default}}."
            )
        return env_value

    return _VAR_PATTERN.sub(_replace, value)


def _interpolate_recursive(obj: Any, source_path: Path | None) -> Any:
    """Recursively walk a parsed YAML structure interpolating string values."""
    if isinstance(obj, str):
        return interpolate_string(obj, source_path)
    if isinstance(obj, dict):
        return {k: _interpolate_recursive(v, source_path) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_recursive(item, source_path) for item in obj]
    return obj


# ── .env file loading ────────────────────────────────────────────────────────

def load_env_files(*paths: Path) -> None:
    """Load one or more `.env` files into os.environ.

    Existing variables are NOT overwritten — the user's shell environment
    always wins. Missing files are silently ignored. Lines that don't parse
    as KEY=VALUE are skipped.

    This is the same minimal parser as in backends/snowflake.py — kept here
    so the loader is self-contained.
    """
    for path in paths:
        if not path.exists():
            continue
        try:
            text = path.read_text()
        except OSError:
            continue

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Strip surrounding quotes if balanced
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)


# ── Top-level loader ─────────────────────────────────────────────────────────

def load_datasources(
    path: Path,
    *,
    env_files: list[Path] | None = None,
    allow_missing: bool = False,
) -> DatasourcesFile:
    """Load and validate a datasources YAML file.

    Args:
        path: path to the datasources YAML.
        env_files: optional list of `.env` files to load into os.environ
            BEFORE interpolation. Use this to make `${PROD_PG_PASSWORD}`
            references resolve from a local `.env.prod` file without
            requiring the user to export the variable manually.
        allow_missing: if True and the file doesn't exist, return an empty
            DatasourcesFile instead of raising. Useful for the gate that
            asks "do we have datasources yet?" without failing.

    Returns:
        A DatasourcesFile with the parsed and validated content.

    Raises:
        DatasourceError: if the file doesn't exist (when allow_missing=False),
            can't be read, can't be parsed as YAML, has unresolved `${VAR}`
            references, or fails schema validation.
    """
    if env_files:
        load_env_files(*env_files)

    if not path.exists():
        if allow_missing:
            return DatasourcesFile()
        raise DatasourceError(
            f"datasources file not found: {path}\n"
            f"  Run `ez-cdc datasource init-demos` to create the bundled demo datasources, "
            f"or `ez-cdc datasource add` to configure your own."
        )

    try:
        raw_text = path.read_text()
    except OSError as e:
        raise DatasourceError(f"failed to read {path}: {e}") from e

    try:
        parsed = yaml.safe_load(raw_text) or {}
    except yaml.YAMLError as e:
        # yaml errors include line/column info; surface them as-is.
        raise DatasourceError(f"failed to parse {path} as YAML: {e}") from e

    if not isinstance(parsed, dict):
        raise DatasourceError(
            f"top-level YAML in {path} must be a mapping, got {type(parsed).__name__}"
        )

    # Resolve ${VAR} interpolation across the whole tree before validating.
    try:
        interpolated = _interpolate_recursive(parsed, path)
    except InterpolationError:
        raise  # already has a good message

    # Hand to pydantic.
    try:
        return DatasourcesFile.model_validate(interpolated)
    except ValidationError as e:
        # Format pydantic errors with the YAML field path so the user can find
        # the offending line. Pydantic v2 errors are a list of {loc, msg, type}.
        lines = [f"datasources file {path} failed schema validation:"]
        for err in e.errors():
            loc = ".".join(str(part) for part in err["loc"])
            msg = err["msg"]
            lines.append(f"  • {loc}: {msg}")
        raise DatasourceValidationError("\n".join(lines)) from e
