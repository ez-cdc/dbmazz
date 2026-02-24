#!/bin/sh
# Wait for StarRocks to be fully ready, then create database and tables.
# Used by docker-compose.production.yml --profile quickstart

set -e

HOST="${SR_HOST:-starrocks}"
PORT="${SR_PORT:-9030}"
DB="${SR_DATABASE:-dbmazz}"

echo "Waiting for StarRocks at ${HOST}:${PORT}..."
until mysql -h "$HOST" -P "$PORT" -u root -e "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done
echo "StarRocks is ready."

echo "Creating database ${DB}..."
mysql -h "$HOST" -P "$PORT" -u root -e "CREATE DATABASE IF NOT EXISTS \`${DB}\`"

echo "Creating tables..."
mysql -h "$HOST" -P "$PORT" -u root "$DB" <<'SQL'
CREATE TABLE IF NOT EXISTS orders (
    id INT NOT NULL,
    customer_id INT,
    total DECIMAL(10,2),
    status VARCHAR(20),
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
