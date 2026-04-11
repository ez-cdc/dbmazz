#!/bin/sh
# Wait for StarRocks to be fully ready, then create database and tables.
# Used by the ez-cdc compose builder when both source and sink are managed.
#
# StarRocks startup has two phases that need to be waited for separately:
#   1. The FE accepts MySQL connections (SELECT 1 works)
#   2. The BE backends register with the FE (SHOW BACKENDS shows alive=true)
#
# CREATE TABLE with replication_num=1 fails if no BE is alive yet:
#   "Table replication num should be less than or equal to the number of
#    available BE nodes. Current alive backend is []."
#
# The compose healthcheck only checks phase 1 (because that's what `SELECT 1`
# tests), so this script has to also wait for phase 2 explicitly.

set -e

HOST="${SR_HOST:-starrocks}"
PORT="${SR_PORT:-9030}"
DB="${SR_DATABASE:-dbmazz}"

# ── Phase 1: wait for the FE to accept MySQL connections ────────────────────
echo "Waiting for StarRocks FE at ${HOST}:${PORT}..."
until mysql -h "$HOST" -P "$PORT" -u root -e "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done
echo "StarRocks FE is ready."

# ── Phase 2: wait for at least one BE to register and be alive ──────────────
# `SHOW BACKENDS` output (with -N -B for tab-separated batch mode) has the
# Alive column somewhere in the middle. We grep for tab-true-tab to count
# rows where Alive='true' without depending on a specific column index
# (which can shift between StarRocks versions).
echo "Waiting for StarRocks backends to register..."
BE_ATTEMPTS=0
BE_MAX_ATTEMPTS=60   # 60 * 2s = 2 minutes max
while [ $BE_ATTEMPTS -lt $BE_MAX_ATTEMPTS ]; do
  ALIVE_COUNT=$(mysql -h "$HOST" -P "$PORT" -u root -N -B -e "SHOW BACKENDS" 2>/dev/null \
    | grep -c "	true	" 2>/dev/null || echo 0)
  if [ "$ALIVE_COUNT" -gt 0 ] 2>/dev/null; then
    echo "StarRocks backends ready (${ALIVE_COUNT} alive)"
    break
  fi
  BE_ATTEMPTS=$((BE_ATTEMPTS + 1))
  sleep 2
done

if [ $BE_ATTEMPTS -ge $BE_MAX_ATTEMPTS ]; then
  echo "ERROR: StarRocks backends never came online after $((BE_MAX_ATTEMPTS * 2))s"
  echo "Final SHOW BACKENDS output:"
  mysql -h "$HOST" -P "$PORT" -u root -e "SHOW BACKENDS" || true
  exit 1
fi

# ── Phase 3: create the target database ─────────────────────────────────────
echo "Creating database ${DB}..."
mysql -h "$HOST" -P "$PORT" -u root -e "CREATE DATABASE IF NOT EXISTS \`${DB}\`"

echo "Creating tables..."
mysql -h "$HOST" -P "$PORT" -u root "$DB" <<'SQL'
CREATE TABLE IF NOT EXISTS orders (
    id INT NOT NULL,
    customer_id INT,
    total DECIMAL(10,2),
    status VARCHAR(20),
    description VARCHAR(20480),
    created_at DATETIME,
    updated_at DATETIME,
    dbmazz_op_type TINYINT,
    dbmazz_is_deleted BOOLEAN,
    dbmazz_synced_at DATETIME,
    dbmazz_cdc_version BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS order_items (
    id INT NOT NULL,
    order_id INT,
    product_name VARCHAR(200),
    quantity INT,
    price DECIMAL(10,2),
    created_at DATETIME,
    dbmazz_op_type TINYINT,
    dbmazz_is_deleted BOOLEAN,
    dbmazz_synced_at DATETIME,
    dbmazz_cdc_version BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
SQL

echo "StarRocks init complete: database '${DB}' with tables orders, order_items"
