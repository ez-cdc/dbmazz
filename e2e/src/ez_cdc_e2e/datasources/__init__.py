"""Datasource configuration package.

A datasource is a named, reusable database connection definition (source or
sink) loaded from `e2e/ez-cdc.yaml`. Datasources are the unit of
selection for all flows: quickstart, verify, load, etc. operate on
`(source, sink)` pairs identified by name.

Public surface:

    from ez_cdc_e2e.datasources import (
        load_datasources,           # parse the YAML and return a DatasourcesFile
        DatasourcesFile,            # top-level container with sources/sinks dicts
        SourceSpec, SinkSpec,       # discriminated unions of all source/sink types
        PostgresSourceSpec,
        PostgresSinkSpec,
        StarRocksSinkSpec,
        SnowflakeSinkSpec,
        DatasourceError,            # base exception
    )

See `.plans/e2e-refactor.md` §17 for the design rationale.
"""

from .loader import (
    DatasourceError,
    DatasourceNotFoundError,
    DatasourceValidationError,
    InterpolationError,
    interpolate_string,
    load_datasources,
    load_env_files,
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

__all__ = [
    # Schema types
    "DatasourcesFile",
    "SourceSpec",
    "SinkSpec",
    "PostgresSourceSpec",
    "PostgresSinkSpec",
    "StarRocksSinkSpec",
    "SnowflakeSinkSpec",
    # Loader functions
    "load_datasources",
    "load_env_files",
    "interpolate_string",
    # Exceptions
    "DatasourceError",
    "DatasourceNotFoundError",
    "DatasourceValidationError",
    "InterpolationError",
]
