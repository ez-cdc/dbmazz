#!/usr/bin/env bash
# =============================================================================
# dbmazz sink verification script
# =============================================================================
#
# Usage:
#   deploy/test-sink.sh <profile>
#
# Examples:
#   deploy/test-sink.sh pg-target     # Test PostgreSQL target
#   deploy/test-sink.sh quickstart    # Test StarRocks target (future)
#
# Prerequisites:
#   docker compose -f deploy/docker-compose.yml --profile <profile> down -v
#   docker compose -f deploy/docker-compose.yml --profile <profile> up -d

set -euo pipefail

PROFILE="${1:-}"
if [ -z "$PROFILE" ]; then
    echo "Usage: $0 <profile>"
    echo "Available profiles: pg-target, snowflake"
    exit 1
fi

DBMAZZ_API="http://localhost:8080"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; exit 1; }
info() { echo -e "  ${YELLOW}…${NC} $1"; }

# ---------------------------------------------------------------------------
# wait_for_stage: poll /status until stage matches expected value
# ---------------------------------------------------------------------------
wait_for_stage() {
    local expected="$1"
    local max_wait="$2"
    local waited=0

    while [ $waited -lt "$max_wait" ]; do
        local stage
        stage=$(curl -sf "$DBMAZZ_API/status" 2>/dev/null | grep -o '"stage":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "unknown")

        if [ "$stage" = "$expected" ]; then
            return 0
        fi
        info "stage=$stage, waiting for $expected... (${waited}s)"
        sleep 2
        waited=$((waited + 2))
    done
    return 1
}

# ---------------------------------------------------------------------------
# snowsql_query: execute a query against Snowflake and return trimmed output
# Reads SNOWSQL_ACCOUNT, SNOWSQL_USER, SNOWSQL_PWD, SNOWSQL_DATABASE,
# SNOWSQL_SCHEMA, SNOWSQL_WAREHOUSE from environment.
# ---------------------------------------------------------------------------
snowsql_query() {
    local sql="$1"
    snowsql \
        -a "$SNOWSQL_ACCOUNT" \
        -u "$SNOWSQL_USER" \
        -d "$SNOWSQL_DATABASE" \
        -s "$SNOWSQL_SCHEMA" \
        -w "$SNOWSQL_WAREHOUSE" \
        -o output_format=tsv \
        -o header=false \
        -o timing=false \
        -o friendly=false \
        -q "$sql" 2>/dev/null | sed '/^$/d' | tail -1 | tr -d '[:space:]'
}

# ---------------------------------------------------------------------------
# pg-target: PostgreSQL → PostgreSQL
# ---------------------------------------------------------------------------
test_pg_target() {
    local SOURCE_DSN="postgres://postgres:postgres@localhost:15432/dbmazz"
    local TARGET_DSN="postgres://postgres:postgres@localhost:25432/dbmazz_target"

    echo "Testing profile: pg-target (PostgreSQL → PostgreSQL)"
    echo ""

    # 1. Verify source has seed data
    echo "Step 1: Verify source data"
    local src_orders
    src_orders=$(psql "$SOURCE_DSN" -t -A -c "SELECT count(*) FROM orders" 2>/dev/null)
    [ "$src_orders" -ge 500 ] && pass "Source has $src_orders orders" || fail "Source has $src_orders orders (expected >= 500)"

    local src_items
    src_items=$(psql "$SOURCE_DSN" -t -A -c "SELECT count(*) FROM order_items" 2>/dev/null)
    [ "$src_items" -ge 1500 ] && pass "Source has $src_items order_items" || fail "Source has $src_items order_items (expected >= 1500)"

    # 2. Verify target starts empty
    echo ""
    echo "Step 2: Verify target is empty"
    local tgt_before
    tgt_before=$(psql "$TARGET_DSN" -t -A -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders'" 2>/dev/null || echo "0")
    if [ "$tgt_before" = "0" ]; then
        pass "Target has no orders table yet (fresh start)"
    else
        local tgt_count
        tgt_count=$(psql "$TARGET_DSN" -t -A -c "SELECT count(*) FROM orders" 2>/dev/null || echo "0")
        [ "$tgt_count" -eq 0 ] && pass "Target orders table is empty" || fail "Target already has $tgt_count orders — run 'docker compose ... down -v' first for a clean test"
    fi

    # 3. Wait for snapshot → CDC transition
    echo ""
    echo "Step 3: Wait for snapshot to complete (max 90s)"
    wait_for_stage "cdc" 90 || fail "dbmazz did not reach CDC stage within 90s"
    pass "dbmazz transitioned to CDC stage"

    # 4. Verify snapshot data landed
    echo ""
    echo "Step 4: Verify snapshot data"
    local tgt_orders
    tgt_orders=$(psql "$TARGET_DSN" -t -A -c "SELECT count(*) FROM orders" 2>/dev/null || echo "0")
    [ "$tgt_orders" -ge "$src_orders" ] && pass "Snapshot: $tgt_orders orders in target (source: $src_orders)" || fail "Snapshot incomplete: target has $tgt_orders orders (expected >= $src_orders)"

    local tgt_items
    tgt_items=$(psql "$TARGET_DSN" -t -A -c "SELECT count(*) FROM order_items" 2>/dev/null || echo "0")
    [ "$tgt_items" -ge "$src_items" ] && pass "Snapshot: $tgt_items order_items in target (source: $src_items)" || fail "Snapshot incomplete: target has $tgt_items items (expected >= $src_items)"

    # 5. Test CDC: insert
    echo ""
    echo "Step 5: Test CDC INSERT"
    local test_id
    test_id=$(psql "$SOURCE_DSN" -t -A -c \
        "INSERT INTO orders (customer_id, total, status) VALUES (99999, 123.45, 'cdc_test') RETURNING id" 2>/dev/null | head -1)
    pass "Inserted test order id=$test_id in source"

    local found=0
    for i in $(seq 1 15); do
        local check
        check=$(psql "$TARGET_DSN" -t -A -c "SELECT count(*) FROM orders WHERE id = $test_id" 2>/dev/null || echo "0")
        if [ "$check" -ge 1 ]; then
            found=1
            pass "CDC INSERT: order id=$test_id replicated to target (${i}s)"
            break
        fi
        sleep 1
    done
    [ $found -eq 1 ] || fail "CDC INSERT: order id=$test_id not found in target after 15s"

    # 6. Test CDC: update
    echo ""
    echo "Step 6: Test CDC UPDATE"
    psql "$SOURCE_DSN" -c "UPDATE orders SET status = 'updated' WHERE id = $test_id" >/dev/null 2>&1

    local update_found=0
    for i in $(seq 1 10); do
        local updated_status
        updated_status=$(psql "$TARGET_DSN" -t -A -c "SELECT status FROM orders WHERE id = $test_id" 2>/dev/null || echo "")
        if [ "$updated_status" = "updated" ]; then
            update_found=1
            pass "CDC UPDATE: status='updated' replicated (${i}s)"
            break
        fi
        sleep 1
    done
    [ $update_found -eq 1 ] || fail "CDC UPDATE: status not updated after 10s"

    # 7. Test CDC: delete
    echo ""
    echo "Step 7: Test CDC DELETE"
    psql "$SOURCE_DSN" -c "DELETE FROM orders WHERE id = $test_id" >/dev/null 2>&1

    local delete_found=0
    for i in $(seq 1 10); do
        local deleted_count
        deleted_count=$(psql "$TARGET_DSN" -t -A -c "SELECT count(*) FROM orders WHERE id = $test_id" 2>/dev/null || echo "1")
        if [ "$deleted_count" -eq 0 ]; then
            delete_found=1
            pass "CDC DELETE: order removed from target (${i}s)"
            break
        fi
        sleep 1
    done
    [ $delete_found -eq 1 ] || fail "CDC DELETE: order still in target after 10s"

    # 8. Verify metadata
    echo ""
    echo "Step 8: Verify metadata"
    local meta_exists
    meta_exists=$(psql "$TARGET_DSN" -t -A -c "SELECT count(*) FROM _dbmazz.\"_metadata\"" 2>/dev/null || echo "0")
    [ "$meta_exists" -ge 1 ] && pass "Metadata table has $meta_exists rows" || fail "Metadata table empty"

    echo ""
    echo -e "${GREEN}All tests passed!${NC}"
}

# ---------------------------------------------------------------------------
# snowflake: PostgreSQL → Snowflake
# ---------------------------------------------------------------------------
test_snowflake() {
    local SOURCE_DSN="postgres://postgres:postgres@localhost:15432/dbmazz"

    echo "Testing profile: snowflake (PostgreSQL → Snowflake)"
    echo ""

    # Pre-flight: check snowsql is installed
    if ! command -v snowsql &>/dev/null; then
        fail "snowsql not found. Install: brew install --cask snowflake-snowsql"
    fi

    # Load credentials from .env.snowflake
    local ENV_FILE
    ENV_FILE="$(cd "$(dirname "$0")" && pwd)/.env.snowflake"
    if [ ! -f "$ENV_FILE" ]; then
        fail ".env.snowflake not found. Copy the example and fill in your credentials:\n  cp deploy/.env.snowflake.example deploy/.env.snowflake"
    fi
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a

    # Map to snowsql env vars
    export SNOWSQL_ACCOUNT="${SINK_SNOWFLAKE_ACCOUNT}"
    export SNOWSQL_USER="${SINK_USER}"
    export SNOWSQL_PWD="${SINK_PASSWORD}"
    export SNOWSQL_DATABASE="${SINK_DATABASE}"
    export SNOWSQL_SCHEMA="${SINK_SCHEMA:-PUBLIC}"
    export SNOWSQL_WAREHOUSE="${SINK_SNOWFLAKE_WAREHOUSE}"

    # Validate Snowflake connection
    info "Validating Snowflake connection..."
    local sf_test
    sf_test=$(snowsql_query "SELECT 1")
    [ "$sf_test" = "1" ] && pass "Snowflake connection OK (account: $SNOWSQL_ACCOUNT)" || fail "Cannot connect to Snowflake. Check credentials in .env.snowflake"

    # 1. Verify source has seed data
    echo ""
    echo "Step 1: Verify source data"
    local src_orders
    src_orders=$(psql "$SOURCE_DSN" -t -A -c "SELECT count(*) FROM orders" 2>/dev/null)
    [ "$src_orders" -ge 500 ] && pass "Source has $src_orders orders" || fail "Source has $src_orders orders (expected >= 500)"

    local src_items
    src_items=$(psql "$SOURCE_DSN" -t -A -c "SELECT count(*) FROM order_items" 2>/dev/null)
    [ "$src_items" -ge 1500 ] && pass "Source has $src_items order_items" || fail "Source has $src_items order_items (expected >= 1500)"

    # 2. Verify target state
    echo ""
    echo "Step 2: Verify Snowflake target"
    local tgt_before
    tgt_before=$(snowsql_query "SELECT COUNT(*) FROM ${SNOWSQL_DATABASE}.${SNOWSQL_SCHEMA}.\"ORDERS\"" 2>/dev/null || echo "0")
    if [ -z "$tgt_before" ] || [ "$tgt_before" = "0" ]; then
        pass "Target has no orders yet (clean state)"
    else
        info "Target has $tgt_before existing orders (dbmazz setup will handle schema)"
    fi

    # 3. Wait for snapshot → CDC transition
    echo ""
    echo "Step 3: Wait for snapshot to complete (max 120s)"
    wait_for_stage "cdc" 120 || fail "dbmazz did not reach CDC stage within 120s"
    pass "dbmazz transitioned to CDC stage"

    # 4. Verify snapshot data in Snowflake (allow normalizer MERGE to finish)
    echo ""
    echo "Step 4: Verify snapshot data"
    info "Waiting 10s for normalizer MERGE to complete..."
    sleep 10

    local tgt_orders
    tgt_orders=$(snowsql_query "SELECT COUNT(*) FROM ${SNOWSQL_DATABASE}.${SNOWSQL_SCHEMA}.\"ORDERS\"")
    [ "$tgt_orders" -ge "$src_orders" ] && pass "Snapshot: $tgt_orders orders in Snowflake (source: $src_orders)" || fail "Snapshot incomplete: Snowflake has $tgt_orders orders (expected >= $src_orders)"

    local tgt_items
    tgt_items=$(snowsql_query "SELECT COUNT(*) FROM ${SNOWSQL_DATABASE}.${SNOWSQL_SCHEMA}.\"ORDER_ITEMS\"")
    [ "$tgt_items" -ge "$src_items" ] && pass "Snapshot: $tgt_items order_items in Snowflake (source: $src_items)" || fail "Snapshot incomplete: Snowflake has $tgt_items items (expected >= $src_items)"

    # 5. Test CDC: INSERT
    echo ""
    echo "Step 5: Test CDC INSERT"
    local test_id
    test_id=$(psql "$SOURCE_DSN" -t -A -c \
        "INSERT INTO orders (customer_id, total, status) VALUES (99999, 123.45, 'cdc_test') RETURNING id" 2>/dev/null | head -1)
    pass "Inserted test order id=$test_id in source"

    local found=0
    for i in $(seq 1 30); do
        local check
        check=$(snowsql_query "SELECT COUNT(*) FROM ${SNOWSQL_DATABASE}.${SNOWSQL_SCHEMA}.\"ORDERS\" WHERE \"ID\" = $test_id")
        if [ "$check" -ge 1 ] 2>/dev/null; then
            found=1
            pass "CDC INSERT: order id=$test_id replicated to Snowflake (${i}s)"
            break
        fi
        sleep 1
    done
    [ $found -eq 1 ] || fail "CDC INSERT: order id=$test_id not found in Snowflake after 30s"

    # 6. Test CDC: UPDATE
    echo ""
    echo "Step 6: Test CDC UPDATE"
    psql "$SOURCE_DSN" -c "UPDATE orders SET status = 'updated' WHERE id = $test_id" >/dev/null 2>&1

    local update_found=0
    for i in $(seq 1 30); do
        local updated_status
        updated_status=$(snowsql_query "SELECT \"STATUS\" FROM ${SNOWSQL_DATABASE}.${SNOWSQL_SCHEMA}.\"ORDERS\" WHERE \"ID\" = $test_id")
        if [ "$updated_status" = "updated" ]; then
            update_found=1
            pass "CDC UPDATE: status='updated' replicated (${i}s)"
            break
        fi
        sleep 1
    done
    [ $update_found -eq 1 ] || fail "CDC UPDATE: status not updated after 30s"

    # 7. Test CDC: DELETE (hard delete — SOFT_DELETE=false in .env.snowflake)
    echo ""
    echo "Step 7: Test CDC DELETE"
    psql "$SOURCE_DSN" -c "DELETE FROM orders WHERE id = $test_id" >/dev/null 2>&1

    local delete_found=0
    for i in $(seq 1 30); do
        local deleted_count
        deleted_count=$(snowsql_query "SELECT COUNT(*) FROM ${SNOWSQL_DATABASE}.${SNOWSQL_SCHEMA}.\"ORDERS\" WHERE \"ID\" = $test_id")
        if [ "$deleted_count" = "0" ]; then
            delete_found=1
            pass "CDC DELETE: order removed from Snowflake (${i}s)"
            break
        fi
        sleep 1
    done
    [ $delete_found -eq 1 ] || fail "CDC DELETE: order still in Snowflake after 30s"

    # 8. Verify metadata
    echo ""
    echo "Step 8: Verify metadata"
    local meta_exists
    meta_exists=$(snowsql_query "SELECT COUNT(*) FROM ${SNOWSQL_DATABASE}._DBMAZZ._METADATA")
    [ "$meta_exists" -ge 1 ] && pass "Metadata table has $meta_exists rows" || fail "Metadata table empty"

    echo ""
    echo -e "${GREEN}All tests passed!${NC}"
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "$PROFILE" in
    pg-target)
        test_pg_target
        ;;
    snowflake)
        test_snowflake
        ;;
    *)
        echo "Unknown profile: $PROFILE"
        echo "Available profiles: pg-target, snowflake"
        exit 1
        ;;
esac
