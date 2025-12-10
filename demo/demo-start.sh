#!/bin/bash

# dbmazz Demo Launcher
# One-command setup for commercial demonstration

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${MAGENTA}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                                                          ║"
echo "║              dbmazz - CDC Demo Launcher                  ║"
echo "║          PostgreSQL → StarRocks in Real-Time             ║"
echo "║                                                          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Check Docker
echo -e "${CYAN}🔍 Checking prerequisites...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not found. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}❌ Docker Compose not found. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker is available${NC}"

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    docker-compose -f docker-compose.demo.yml down
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

trap cleanup EXIT

# Start services
echo -e "\n${CYAN}🚀 Starting services...${NC}"
docker-compose -f docker-compose.demo.yml up -d postgres starrocks

echo -e "${YELLOW}⏳ Waiting for PostgreSQL to be ready...${NC}"
TIMEOUT=60  # 60 segundos máximo
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    if docker exec dbmazz-demo-postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL is ready${NC}"
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo -e "${YELLOW}   Checking... (${ELAPSED}s/${TIMEOUT}s)${NC}"
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo -e "${RED}❌ PostgreSQL timeout${NC}"
    exit 1
fi

echo -e "${YELLOW}⏳ Waiting for StarRocks FE to be ready...${NC}"
TIMEOUT=300  # 5 minutos máximo para StarRocks
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    if docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ StarRocks FE is ready${NC}"
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo -e "${YELLOW}   Checking FE... (${ELAPSED}s/${TIMEOUT}s)${NC}"
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo -e "${RED}❌ StarRocks FE timeout${NC}"
    exit 1
fi

# Esperar a que el Backend (BE) esté listo
echo -e "${YELLOW}⏳ Waiting for StarRocks BE (Backend) to be ready...${NC}"
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    # Verificar que hay al menos un BE alive
    BE_ALIVE=$(docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root -sNe "SHOW BACKENDS;" 2>/dev/null | grep -c "true" || echo "0")
    if [ "$BE_ALIVE" -gt "0" ]; then
        echo -e "${GREEN}✅ StarRocks BE is ready ($BE_ALIVE backends alive)${NC}"
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo -e "${YELLOW}   Checking BE... (${ELAPSED}s/${TIMEOUT}s)${NC}"
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo -e "${RED}❌ StarRocks BE timeout - no backends available${NC}"
    exit 1
fi

# Initialize StarRocks schema (manual execution needed as allin1 doesn't auto-run init scripts)
echo -e "${CYAN}📝 Initializing StarRocks schema...${NC}"
# Ejecutar script de inicialización
docker exec -i dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root < starrocks/init.sql 2>&1

# Verificar que demo_db se creó
DEMO_DB_EXISTS=$(docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW DATABASES LIKE 'demo_db';" 2>/dev/null | grep -c "demo_db" || echo "0")

if [ "$DEMO_DB_EXISTS" -gt "0" ]; then
    # Verificar que las tablas se crearon
    TABLE_COUNT=$(docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root -D demo_db -e "SHOW TABLES;" 2>/dev/null | grep -c -E "(orders|order_items)" || echo "0")
    
    if [ "$TABLE_COUNT" -ge "2" ]; then
        echo -e "${GREEN}✅ StarRocks schema initialized (demo_db + $TABLE_COUNT tables)${NC}"
    else
        echo -e "${RED}❌ Failed to create tables in demo_db${NC}"
        exit 1
    fi
else
    echo -e "${RED}❌ Failed to create demo_db${NC}"
    exit 1
fi

# Start dbmazz
echo -e "\n${CYAN}🔧 Starting dbmazz CDC service...${NC}"
docker-compose -f docker-compose.demo.yml up -d dbmazz
sleep 3
echo -e "${GREEN}✅ dbmazz is running${NC}"

# Start traffic generator
echo -e "\n${CYAN}📊 Starting traffic generator...${NC}"
docker-compose -f docker-compose.demo.yml up -d traffic-generator
sleep 2
echo -e "${GREEN}✅ Traffic generator is running${NC}"

# Start monitor
echo -e "\n${CYAN}📺 Starting real-time monitor...${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop the demo${NC}\n"
sleep 2

docker-compose -f docker-compose.demo.yml up monitor

# Note: cleanup() will run automatically on exit

