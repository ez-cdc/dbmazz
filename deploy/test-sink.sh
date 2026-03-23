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
    echo "Available profiles: pg-target"
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
# Dispatch
# ---------------------------------------------------------------------------
case "$PROFILE" in
    pg-target)
        test_pg_target
        ;;
    *)
        echo "Unknown profile: $PROFILE"
        echo "Available profiles: pg-target"
        exit 1
        ;;
esac
