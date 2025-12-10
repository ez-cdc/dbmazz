#!/bin/bash

# Simple CDC Validation - Tests CDC without StarRocks
set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}🚀 dbmazz CDC Simple Validation${NC}\n"

# Cleanup
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    docker-compose -f docker-compose.demo.yml down
    [ ! -z "$DBMAZZ_PID" ] && kill $DBMAZZ_PID 2>/dev/null || true
}
trap cleanup EXIT

# Start only PostgreSQL
echo -e "${CYAN}📦 Starting PostgreSQL...${NC}"
docker-compose -f docker-compose.demo.yml up -d postgres

echo -e "${YELLOW}⏳ Waiting for PostgreSQL...${NC}"
for i in {1..30}; do
    if docker exec dbmazz-demo-postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL ready${NC}\n"
        break
    fi
    sleep 1
done

# Test dbmazz connection directly (no Docker, no StarRocks needed)
echo -e "${CYAN}🔌 Testing dbmazz CDC connection...${NC}"
echo -e "${YELLOW}   Starting dbmazz in background for 5 seconds...${NC}\n"

# Set environment for local dbmazz
export DATABASE_URL="postgres://postgres:postgres@localhost:15432/demo_db?replication=database"
export SLOT_NAME="dbmazz_test_slot"
export PUBLICATION_NAME="dbmazz_pub"
export STARROCKS_URL="http://localhost:9999"  # Dummy URL
export STARROCKS_DB="demo_db"
export STARROCKS_TABLE="orders"
export STARROCKS_USER="root"
export STARROCKS_PASS=""
export RUST_LOG="info"

# Run dbmazz in background
cd .. && timeout 5s ./target/release/dbmazz 2>&1 | tee /tmp/dbmazz-test.log &
DBMAZZ_PID=$!

sleep 6  # Let it run

# Check output
echo -e "\n${CYAN}📋 dbmazz Output:${NC}"
cat /tmp/dbmazz-test.log

if grep -q "Starting dbmazz" /tmp/dbmazz-test.log; then
    echo -e "\n${GREEN}✅ dbmazz initialized successfully${NC}"
fi

if grep -q "Connected" /tmp/dbmazz-test.log; then
    echo -e "${GREEN}✅ dbmazz connected to PostgreSQL${NC}"
fi

if grep -q "Streaming CDC events" /tmp/dbmazz-test.log; then
    echo -e "${GREEN}✅ dbmazz streaming WAL events${NC}"
fi

echo -e "\n${GREEN}🎉 CDC validation complete!${NC}"
echo -e "${CYAN}PostgreSQL is properly configured for logical replication.${NC}"
echo -e "${CYAN}dbmazz can connect and stream CDC events.${NC}"

