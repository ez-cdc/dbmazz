# MySQL source (BETA)

dbmazz can stream changes from MySQL 5.7+ / 8.0+ to any of the supported sinks (StarRocks, PostgreSQL, Snowflake) via the `mysql-source` cargo feature.

This document covers MySQL-specific setup, configuration, and the BETA scope. For general dbmazz architecture, see [architecture.md](architecture.md).

## Prerequisites

The source MySQL server **must** have:

| Setting | Required value | Why |
|---|---|---|
| `log_bin` | enabled (any non-empty value) | Binary log is the change stream |
| `binlog_format` | `ROW` | Statement / mixed formats don't carry full row state |
| `binlog_row_image` | `FULL` | Needed for `UPDATE` / `DELETE` reconciliation |
| `gtid_mode` | `ON` | Required for the read-only DBLog incremental snapshot |
| `enforce_gtid_consistency` | `ON` | Pairs with `gtid_mode=ON` |
| `server_id` | unique non-zero | Replication client identity |

Privileges required for the connection user: `REPLICATION SLAVE`, `REPLICATION CLIENT`, plus `CREATE`, `INSERT`, `UPDATE`, `DELETE` on the source database (dbmazz writes its checkpoint and snapshot-state tables there — `dbmazz_checkpoints` and `dbmazz_snapshot_state`).

## Configuration

```yaml
sources:
  my-mysql:
    type: mysql
    url: mysql://user:password@host:3306/dbname
    tables:
      - orders
      - order_items
    tls_skip_verify: false   # default; set true ONLY for dev MySQL without TLS
    gtid_enabled: true       # default
    # server_id: 5500        # optional; dbmazz auto-assigns if omitted
```

Environment variables (set on the dbmazz container by `ez-cdc-cli`, or set directly when running dbmazz standalone):

| Env var | Default | Notes |
|---|---|---|
| `SOURCE_TYPE` | — | Must be `mysql` |
| `SOURCE_URL` | — | `mysql://user:pass@host:port/dbname` |
| `MYSQL_TLS_SKIP_VERIFY` | `false` | Disables TLS verification — dev only |
| `MYSQL_GTID_ENABLED` | `true` | Required for incremental snapshot |
| `MYSQL_SERVER_ID` | random in [5400, 6400) | Set explicitly when running multiple dbmazz instances against one source |

## Local development

Spin up a dev MySQL with the right flags in one line:

```bash
docker run -d --rm --name mysql-source \
  -p 13306:3306 \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
  -e MYSQL_DATABASE=dbmazz \
  mysql:8.0 \
  --gtid-mode=ON --enforce-gtid-consistency=ON \
  --log-bin=mysql-bin --server-id=1 \
  --binlog-format=ROW --binlog-row-image=FULL
```

The default `ghcr.io/ez-cdc/dbmazz:latest` image is built without the `mysql-source` cargo feature. For now, build a local image:

```bash
docker build -t dbmazz-mysql:dev -f Dockerfile.mysql .
```

Then point `ez-cdc verify` at it via `--image dbmazz-mysql:dev`. The release pipeline will eventually publish a `mysql-source`-enabled image; this manual step goes away then.

## How it works

dbmazz follows Debezium's MySQL connector design, adapted to dbmazz's single-binary architecture (no Kafka Connect, no signal table):

- **Binlog streaming** — ROW format, GTID-aware. Checkpoints are persisted as `(binlog_file, position, gtid_executed)` only at transaction commit boundaries; restart resumes from the last committed GTID set.
- **Incremental snapshot** — read-only DBLog (Andreakis & Papapanagiotou 2020), adapted to use the binlog consumer's live GTID set as low/high watermarks instead of a writable signal table. Concurrent CDC + snapshot is supported; per-PK reconciliation drops snapshot rows that lost a race to a concurrent binlog event.
- **Schema introspection** — `information_schema` query at startup; the schema is cached in-memory and used to map binlog row payloads to the engine's generic `CdcRecord` shape.

## BETA scope

What works today:

- Binlog streaming (INSERT / UPDATE / DELETE) for ROW format, GTID mode.
- Incremental snapshot for tables with single-column integer primary keys.
- Concurrent snapshot + CDC reconciliation.
- Crash-recovery startup probe (validates persisted GTID set vs `@@global.gtid_executed`).
- Verified end-to-end against PostgreSQL and StarRocks sinks via `ez-cdc verify` (Tier 1: A1-A4, B1/B1b/B2/B3, CDC, D4, D5, C10, H1).

What's known to be incomplete (tracked in `openspec/changes/mysql-cdc-beta-to-ga`):

- **Type fidelity**: `BIGINT UNSIGNED` overflow at the `i64` boundary; `DECIMAL` precision/scale not propagated from `information_schema`; `DATETIME` microseconds discarded; `ENUM`/`SET` semantics; full JSON typed roundtrip. The `C11` (Type roundtrip) verify check is expected to FAIL until this lands — use `ez-cdc verify --skip C11` in CI.
- **Snapshot bootstrap from arbitrary binlog position**: today the daemon resumes from the persisted checkpoint or starts at the binlog tail; no first-run bootstrap that walks backwards.
- **Cursor-based snapshot chunker**: today's chunker is range-based; cursor-based gives more uniform chunk sizes for non-contiguous PK distributions.
- **Non-integer primary keys**: composite, UUID, and string PKs are not yet supported in the snapshot path.

## Verifying

The `ez-cdc-cli` test harness has full MySQL support. After bringing up a MySQL container per the recipe above:

```bash
# Configure dbmazz to point at it
ez-cdc datasource add   # interactive wizard, prompts for tls_skip_verify etc.

# Run the verify suite
ez-cdc verify --image dbmazz-mysql:dev \
  --source my-mysql \
  --sink <your-sink> \
  --skip C11
```

See the `ez-cdc-cli` repository's README for the full verify command surface.

## Related documents

- [architecture.md](architecture.md) — how the engine, source, and sink layers fit together.
- [configuration.md](configuration.md) — env vars, pipeline tuning, sink-specific options.
- [contributing-connectors.md](contributing-connectors.md) — adding a new source or sink.
- [CHANGELOG.md](../CHANGELOG.md) — version-by-version notes including the MySQL beta milestone.
