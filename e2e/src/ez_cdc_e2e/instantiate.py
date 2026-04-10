"""Spec → client/backend instantiation helpers.

Lives in its own module so both `cli.py` and `verify/runner.py` can import
without creating a cycle. Builds:

  - PostgresSource for a SourceSpec
  - PostgresTarget / StarRocksTarget / SnowflakeTarget for a SinkSpec

Connection details are always taken from the spec's fields directly.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

from .backends.base import TargetBackend
from .backends.postgres import PostgresTarget
from .backends.snowflake import SnowflakeTarget
from .backends.starrocks import StarRocksTarget
from .datasources.schema import (
    PostgresSinkSpec,
    PostgresSourceSpec,
    SinkSpec,
    SnowflakeSinkSpec,
    SourceSpec,
    StarRocksSinkSpec,
)
from .source.postgres import PostgresSource


def instantiate_source_from_spec(spec: SourceSpec) -> PostgresSource:
    """Build a SourceClient from a SourceSpec.

    Returns an unconnected client; caller must call .connect().
    """
    if isinstance(spec, PostgresSourceSpec):
        return PostgresSource(spec.url)
    raise ValueError(f"unsupported source spec type: {type(spec).__name__}")


def instantiate_backend_from_spec(spec: SinkSpec) -> TargetBackend:
    """Build a TargetBackend from a SinkSpec.

    Returns an unconnected backend; caller must call .connect().
    """
    if isinstance(spec, PostgresSinkSpec):
        return PostgresTarget(dsn=spec.url, schema=spec.schema_)

    if isinstance(spec, StarRocksSinkSpec):
        parsed = urlparse(spec.url)
        return StarRocksTarget(
            host=parsed.hostname or "localhost",
            port=spec.mysql_port,
            user=spec.user,
            password=spec.password,
            database=spec.database,
        )

    if isinstance(spec, SnowflakeSinkSpec):
        # SnowflakeTarget reads creds from os.environ. Push the spec's
        # values into the env so the existing class works unchanged.
        # The YAML is the source of truth — there is no .env file anywhere
        # in the loop, just spec → os.environ → SnowflakeTarget.
        os.environ["SINK_SNOWFLAKE_ACCOUNT"]   = spec.account
        os.environ["SINK_USER"]                = spec.user
        os.environ["SINK_PASSWORD"]            = spec.password
        os.environ["SINK_DATABASE"]            = spec.database
        os.environ["SINK_SCHEMA"]              = spec.schema_
        os.environ["SINK_SNOWFLAKE_WAREHOUSE"] = spec.warehouse
        if spec.role:
            os.environ["SINK_SNOWFLAKE_ROLE"] = spec.role
        os.environ["SINK_SNOWFLAKE_SOFT_DELETE"] = "true" if spec.soft_delete else "false"
        return SnowflakeTarget()

    raise ValueError(f"unsupported sink spec type: {type(spec).__name__}")
