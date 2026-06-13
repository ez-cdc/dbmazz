# Apache Iceberg Sink Connector

CDC sink connector for Apache Iceberg via the REST catalog protocol.

## Overview

Records are accumulated in memory, serialized to Parquet files, written to the warehouse
path, and committed as new Iceberg snapshots through the REST catalog API.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `SINK_TYPE` | — | Must be `apache_iceberg` |
| `SINK_URL` | — | REST catalog URL (e.g. `http://localhost:8181`) |
| `SINK_WAREHOUSE` | — | Warehouse path (e.g. `file:///tmp/warehouse`) |
| `SINK_DATABASE` | — | Target namespace / database in the catalog |
| `SINK_ICEBERG_PREFIX` | `v1` | REST API prefix |

## Table creation

Tables and namespaces are created automatically during sink setup. Four audit columns are
added to every table: `_dbmazz_op_type`, `_dbmazz_is_deleted`, `_dbmazz_synced_at`, `_dbmazz_cdc_version`.

## Schema Evolution

New columns from the source are detected automatically and added to the Iceberg schema via
the REST catalog's schema commit endpoint.
