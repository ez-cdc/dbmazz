"""Spec → client/backend instantiation helpers.

Lives in its own module so both `cli.py` and `verify/runner.py` can import
without creating a cycle. Builds:

  - PostgresSource for a SourceSpec
  - PostgresTarget / StarRocksTarget / SnowflakeTarget for a SinkSpec

For managed datasources, connection details are hardcoded to localhost
ports exposed by compose_builder's generated services. For user-managed,
the spec's connection fields are used directly.
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


# Hardcoded host ports exposed by compose_builder for managed services.
# Keep these in sync with `compose_builder._build_managed_*` ports.
_MANAGED_PG_SOURCE_DSN  = "postgres://postgres:postgres@localhost:15432/dbmazz"
_MANAGED_PG_SINK_DSN    = "postgres://postgres:postgres@localhost:25432/dbmazz_target"
_MANAGED_PG_SINK_DB     = "dbmazz_target"
_MANAGED_PG_SINK_SCHEMA = "public"
_MANAGED_STARROCKS_HOST = "localhost"
_MANAGED_STARROCKS_PORT = 9030
_MANAGED_STARROCKS_USER = "root"
_MANAGED_STARROCKS_DB   = "dbmazz"


def instantiate_source_from_spec(spec: SourceSpec) -> PostgresSource:
    """Build a SourceClient from a SourceSpec.

    Returns an unconnected client; caller must call .connect().
    """
    if isinstance(spec, PostgresSourceSpec):
        if spec.managed:
            return PostgresSource(_MANAGED_PG_SOURCE_DSN)
        return PostgresSource(spec.url or "")
    raise ValueError(f"unsupported source spec type: {type(spec).__name__}")


def instantiate_backend_from_spec(spec: SinkSpec) -> TargetBackend:
    """Build a TargetBackend from a SinkSpec.

    Returns an unconnected backend; caller must call .connect().
    """
    if isinstance(spec, PostgresSinkSpec):
        if spec.managed:
            return PostgresTarget(
                dsn=_MANAGED_PG_SINK_DSN,
                schema=_MANAGED_PG_SINK_SCHEMA,
            )
        return PostgresTarget(dsn=spec.url or "", schema=spec.schema_)

    if isinstance(spec, StarRocksSinkSpec):
        if spec.managed:
            return StarRocksTarget(
                host=_MANAGED_STARROCKS_HOST,
                port=_MANAGED_STARROCKS_PORT,
                user=_MANAGED_STARROCKS_USER,
                password="",
                database=_MANAGED_STARROCKS_DB,
            )
        parsed = urlparse(spec.url or "")
        return StarRocksTarget(
            host=parsed.hostname or "localhost",
            port=spec.mysql_port,
            user=spec.user,
            password=spec.password,
            database=spec.database or "dbmazz",
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
