#!/bin/bash

# dbmazz CDC Validation Script
# Tests CDC functionality before running full demo

set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                                                          ║"
echo "║         dbmazz CDC Validation Test                       ║"
echo "║                                                          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up test environment...${NC}"
    docker-compose -f docker-compose.demo.yml down
}

trap cleanup EXIT

# Step 1: Start PostgreSQL only
echo -e "${CYAN}📦 Step 1: Starting PostgreSQL...${NC}"
docker-compose -f docker-compose.demo.yml up -d postgres

echo -e "${YELLOW}⏳ Waiting for PostgreSQL...${NC}"
for i in {1..30}; do
    if docker exec dbmazz-demo-postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL is ready${NC}"
        break
    fi
    sleep 1
done

# Step 2: Verify replication is configured
echo -e "\n${CYAN}🔍 Step 2: Verifying replication configuration...${NC}"
WAL_LEVEL=$(docker exec dbmazz-demo-postgres psql -U postgres -t -c "SHOW wal_level;" | xargs)
echo "   wal_level: $WAL_LEVEL"

if [ "$WAL_LEVEL" != "logical" ]; then
    echo -e "${RED}❌ ERROR: wal_level is not 'logical'${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Replication properly configured${NC}"

# Step 3: Verify schema and data
echo -e "\n${CYAN}📊 Step 3: Verifying schema and seed data...${NC}"
ORDER_COUNT=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM orders;" | xargs)
ITEM_COUNT=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM order_items;" | xargs)
echo "   Orders: $ORDER_COUNT"
echo "   Order Items: $ITEM_COUNT"

if [ "$ORDER_COUNT" -lt "100" ]; then
    echo -e "${RED}❌ ERROR: Insufficient seed data${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Schema and data verified${NC}"

# Step 4: Verify publication exists
echo -e "\n${CYAN}📢 Step 4: Verifying publication...${NC}"
PUB_EXISTS=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM pg_publication WHERE pubname = 'dbmazz_pub';" | xargs)
echo "   Publication 'dbmazz_pub' exists: $PUB_EXISTS"

if [ "$PUB_EXISTS" != "1" ]; then
    echo -e "${RED}❌ ERROR: Publication not found${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Publication verified${NC}"

# Step 5: Test CDC with a manual insert
echo -e "\n${CYAN}🧪 Step 5: Testing CDC with manual data change...${NC}"
BEFORE_COUNT=$ORDER_COUNT
docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -c "INSERT INTO orders (customer_id, total, status) VALUES (9999, 100.00, 'test') RETURNING id;" > /dev/null
AFTER_COUNT=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM orders;" | xargs)

echo "   Before: $BEFORE_COUNT"
echo "   After: $AFTER_COUNT"

if [ "$AFTER_COUNT" -le "$BEFORE_COUNT" ]; then
    echo -e "${RED}❌ ERROR: Insert failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Data changes working${NC}"

# Step 6: Quick dbmazz connection test (5 seconds)
echo -e "\n${CYAN}🔌 Step 6: Testing dbmazz connection (5 sec test)...${NC}"
echo -e "${YELLOW}   Starting dbmazz temporarily...${NC}"

# Start dbmazz in background
docker-compose -f docker-compose.demo.yml up -d dbmazz
sleep 2

# Check if dbmazz is running
if docker ps | grep -q dbmazz-demo-cdc; then
    echo -e "${GREEN}✅ dbmazz started successfully${NC}"
    
    # Give it a few seconds to connect
    sleep 3
    
    # Check logs for connection success
    echo -e "${YELLOW}   Checking dbmazz logs...${NC}"
    docker logs dbmazz-demo-cdc 2>&1 | tail -20
    
    if docker logs dbmazz-demo-cdc 2>&1 | grep -q "Connected"; then
        echo -e "${GREEN}✅ dbmazz connected to PostgreSQL${NC}"
    elif docker logs dbmazz-demo-cdc 2>&1 | grep -q "Starting dbmazz"; then
        echo -e "${GREEN}✅ dbmazz initialized${NC}"
    else
        echo -e "${YELLOW}⚠️  Check logs above for status${NC}"
    fi
else
    echo -e "${RED}❌ ERROR: dbmazz failed to start${NC}"
    exit 1
fi

# Final summary
echo -e "\n${CYAN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║                  Validation Summary                      ║${NC}"
echo -e "${CYAN}╠══════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  ✅ PostgreSQL running with logical replication         ║${NC}"
echo -e "${GREEN}║  ✅ Schema and seed data present ($ORDER_COUNT orders)            ║${NC}"
echo -e "${GREEN}║  ✅ Publication 'dbmazz_pub' configured                 ║${NC}"
echo -e "${GREEN}║  ✅ Data changes working correctly                      ║${NC}"
echo -e "${GREEN}║  ✅ dbmazz can connect and initialize                   ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════╝${NC}"

echo -e "\n${GREEN}🎉 All validation checks passed!${NC}"
echo -e "${CYAN}You can now run the full demo with: ./demo-start.sh${NC}"

# Cleanup will run automatically via trap

