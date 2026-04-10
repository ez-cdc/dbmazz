"""Persistence layer for the datasources YAML file.

DatasourceStore wraps a single `e2e/ez-cdc.yaml` file and provides:

  - load() / reload() — read from disk and validate
  - save() — atomic write back to disk (write to temp + rename)
  - add_source(name, spec) / add_sink(name, spec) — add or replace
  - remove(name) — drop a source or sink by name
  - get_source(name) / get_sink(name) — fetch with clear errors
  - list_sources() / list_sinks() — sorted name lists
  - has_any() — true if at least 1 source AND 1 sink
  - exists(name) — true if any datasource (source or sink) has that name

Atomicity:
  Writes go through a temp file in the same directory followed by os.rename
  to avoid leaving a half-written YAML on crash. A `.bak` snapshot of the
  previous content is kept next to the file (overwritten on each save) so
  the user can recover from a wizard mistake.

Threading:
  Not thread-safe. The CLI is single-threaded; if that ever changes, wrap
  the store in a threading.Lock at the call site.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Optional

import yaml

from .loader import (
    DatasourceError,
    DatasourceNotFoundError,
    load_datasources,
)
from .schema import (
    DatasourcesFile,
    PostgresSinkSpec,
    PostgresSourceSpec,
    SinkSpec,
    SnowflakeSinkSpec,
    SourceSpec,
    StarRocksSinkSpec,
)


class DatasourceStore:
    """File-backed CRUD for datasources.

    Constructed with a path. The file may not exist yet — use `load()` with
    `allow_missing=True` to handle the "first run" case gracefully, or
    construct an empty store with `DatasourceStore.empty(path)`.
    """

    def __init__(self, path: Path, env_files: Optional[list[Path]] = None) -> None:
        self.path = Path(path).resolve()
        self.env_files = env_files or []
        self._data: DatasourcesFile = DatasourcesFile()
        self._loaded = False

    # ── factory ──────────────────────────────────────────────────────────────

    @classmethod
    def empty(cls, path: Path) -> "DatasourceStore":
        """Build an in-memory empty store. Doesn't read disk. Useful for the
        first-run flow where the wizard creates datasources from scratch."""
        store = cls(path)
        store._data = DatasourcesFile()
        store._loaded = True
        return store

    # ── loading ──────────────────────────────────────────────────────────────

    def load(self, *, allow_missing: bool = True) -> None:
        """Read and validate the file. Replaces in-memory state.

        With allow_missing=True (default), a non-existent file becomes an
        empty DatasourcesFile rather than raising. This is the right
        behavior for the gate that asks "do we have datasources yet?"
        """
        self._data = load_datasources(
            self.path,
            env_files=self.env_files,
            allow_missing=allow_missing,
        )
        self._loaded = True

    def reload(self) -> None:
        """Re-read the file from disk, discarding any in-memory mutations."""
        self.load(allow_missing=True)

    @property
    def loaded(self) -> bool:
        return self._loaded

    @property
    def data(self) -> DatasourcesFile:
        """Direct access to the underlying DatasourcesFile.

        Use this for read-only operations that need fields not exposed by
        the convenience methods. For mutations, prefer the `add_*` / `remove`
        methods so the change goes through the validator and gets persisted.
        """
        if not self._loaded:
            self.load()
        return self._data

    # ── inspection ───────────────────────────────────────────────────────────

    def has_any(self) -> bool:
        return self.data.has_any()

    def is_empty(self) -> bool:
        return self.data.is_empty()

    def list_sources(self) -> list[str]:
        return self.data.list_source_names()

    def list_sinks(self) -> list[str]:
        return self.data.list_sink_names()

    def get_source(self, name: str) -> SourceSpec:
        return self.data.get_source(name)

    def get_sink(self, name: str) -> SinkSpec:
        return self.data.get_sink(name)

    def exists(self, name: str) -> bool:
        """True if any datasource (source or sink) has this name. Used by the
        wizard to detect collisions before saving."""
        d = self.data
        return name in d.sources or name in d.sinks

    # ── mutation ─────────────────────────────────────────────────────────────

    def add_source(self, name: str, spec: SourceSpec, *, replace: bool = False) -> None:
        """Add a source datasource. Raises if the name already exists in
        either sources or sinks (use replace=True to override an existing source)."""
        self._check_name_collision(name, kind="source", replace=replace)
        self.data.sources[name] = spec  # pydantic re-validates because validate_assignment=True

    def add_sink(self, name: str, spec: SinkSpec, *, replace: bool = False) -> None:
        """Add a sink datasource. Same collision rules as add_source."""
        self._check_name_collision(name, kind="sink", replace=replace)
        self.data.sinks[name] = spec

    def remove(self, name: str) -> None:
        """Remove a datasource by name (source or sink). Raises if not found."""
        if name in self.data.sources:
            del self.data.sources[name]
            return
        if name in self.data.sinks:
            del self.data.sinks[name]
            return
        raise DatasourceNotFoundError(
            f"datasource {name!r} not found in {self.path}"
        )

    def _check_name_collision(self, name: str, *, kind: str, replace: bool) -> None:
        d = self.data
        # Cross-kind collision: a name can't be both a source and a sink.
        if kind == "source" and name in d.sinks:
            raise DatasourceError(
                f"name {name!r} is already used by a sink. "
                f"Choose a different name or remove the sink first."
            )
        if kind == "sink" and name in d.sources:
            raise DatasourceError(
                f"name {name!r} is already used by a source. "
                f"Choose a different name or remove the source first."
            )
        # Same-kind collision: only allowed with replace=True.
        existing = d.sources if kind == "source" else d.sinks
        if name in existing and not replace:
            raise DatasourceError(
                f"{kind} {name!r} already exists. Pass replace=True to overwrite, "
                f"or remove it first."
            )

    # ── persistence ──────────────────────────────────────────────────────────

    def save(self, *, mode: int = 0o600) -> None:
        """Write the in-memory state back to disk atomically.

        Steps:
          1. Serialize to YAML.
          2. Write to a temporary file in the same directory.
          3. fsync the temp file.
          4. Backup the existing file to .bak (if any).
          5. os.rename(temp, target) — atomic on POSIX.
          6. chmod the target to `mode` (default 0o600 — credentials).

        On any failure, the original file is left untouched.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        yaml_text = self._render_yaml()

        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(yaml_text)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass  # not all filesystems support fsync; non-fatal

            # Backup the existing file before overwriting.
            if self.path.exists():
                bak_path = self.path.with_suffix(self.path.suffix + ".bak")
                try:
                    shutil.copy2(self.path, bak_path)
                except OSError:
                    pass  # backup failure is non-fatal — proceed with the save

            os.replace(tmp_path, self.path)

            try:
                os.chmod(self.path, mode)
            except OSError:
                pass  # non-POSIX filesystem; non-fatal

        finally:
            # Clean up the temp file if it's still there (e.g., on error).
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def _render_yaml(self) -> str:
        """Convert the in-memory DatasourcesFile to a YAML string.

        We render via pydantic's model_dump (with by_alias=True so the
        `schema_` field becomes `schema:` in the output) and then call
        yaml.safe_dump with sort_keys=False to preserve our intended order.
        """
        # by_alias=True so PostgresSinkSpec.schema_ → "schema" in output.
        # exclude_none=True so optional fields aren't littered as `null`.
        as_dict = self._data.model_dump(by_alias=True, exclude_none=True)

        # Render with a header comment for context.
        header = (
            "# ez-cdc configuration file\n"
            "# Datasources + pipeline settings. Managed by `ez-cdc datasource ...`\n"
            "# or edited manually. See e2e/ez-cdc.example.yaml for the schema.\n"
            "# Credentials may use ${VAR} interpolation; see ez-cdc docs.\n"
            "\n"
        )

        body = yaml.safe_dump(
            as_dict,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=True,
            indent=2,
        )

        return header + body
