#!/bin/bash

# dbmazz Demo Verification Script

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}🔍 dbmazz Verification Report${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# PostgreSQL counts
echo -e "\n${CYAN}📊 PostgreSQL (Source):${NC}"
PG_ORDERS=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM orders;" | xargs)
PG_ITEMS=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM order_items;" | xargs)
echo "   Orders: $PG_ORDERS"
echo "   Order Items: $PG_ITEMS"

# StarRocks counts
echo -e "\n${CYAN}📊 StarRocks (Target):${NC}"
SR_ORDERS=$(docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root -D demo_db -sNe "SELECT COUNT(*) FROM orders WHERE __op = 0;" 2>/dev/null || echo "0")
SR_ITEMS=$(docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root -D demo_db -sNe "SELECT COUNT(*) FROM order_items WHERE __op = 0;" 2>/dev/null || echo "0")
SR_DELETED=$(docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root -D demo_db -sNe "SELECT COUNT(*) FROM orders WHERE __op = 1;" 2>/dev/null || echo "0")
echo "   Orders: $SR_ORDERS"
echo "   Order Items: $SR_ITEMS"
echo "   Deleted Orders: $SR_DELETED"

# Compare
echo -e "\n${CYAN}🔄 Sync Status:${NC}"
if [ "$PG_ORDERS" -eq "$SR_ORDERS" ]; then
    echo -e "   ${GREEN}✅ Orders: 100% synced${NC}"
else
    SYNC_PERCENT=$((SR_ORDERS * 100 / PG_ORDERS))
    echo -e "   ${YELLOW}⏳ Orders: ${SYNC_PERCENT}% synced ($SR_ORDERS/$PG_ORDERS)${NC}"
fi

if [ "$PG_ITEMS" -eq "$SR_ITEMS" ]; then
    echo -e "   ${GREEN}✅ Order Items: 100% synced${NC}"
else
    SYNC_PERCENT=$((SR_ITEMS * 100 / PG_ITEMS))
    echo -e "   ${YELLOW}⏳ Order Items: ${SYNC_PERCENT}% synced ($SR_ITEMS/$PG_ITEMS)${NC}"
fi

echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${GREEN}✅ Verification complete${NC}"

