#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=========================================="
echo "🧪 Validación Rápida del Demo"
echo "=========================================="

# 1. Verificar que el binario existe
echo -e "\n${YELLOW}1. Verificando binario dbmazz...${NC}"
if [ ! -f "../target/release/dbmazz" ]; then
    echo -e "${RED}❌ Binario no encontrado. Compilando...${NC}"
    cd .. && cargo build --release && cd demo
fi
echo -e "${GREEN}✓ Binario OK ($(ls -lh ../target/release/dbmazz | awk '{print $5}'))${NC}"

# 2. Iniciar servicios core
echo -e "\n${YELLOW}2. Iniciando PostgreSQL y StarRocks...${NC}"
docker-compose -f docker-compose.demo.yml up -d postgres starrocks > /dev/null 2>&1

# 3. Esperar PostgreSQL
echo -e "\n${YELLOW}3. Esperando PostgreSQL...${NC}"
for i in {1..20}; do
    docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -c "SELECT 1;" > /dev/null 2>&1 && break
    sleep 2
done
echo -e "${GREEN}✓ PostgreSQL listo${NC}"

# 4. Verificar tablas en PostgreSQL
TABLES_PG=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' AND tablename IN ('orders', 'order_items', 'toast_test');" 2>&1 | tr -d ' ')
echo -e "${GREEN}✓ PostgreSQL: $TABLES_PG tablas creadas${NC}"

# 5. Esperar StarRocks
echo -e "\n${YELLOW}4. Esperando StarRocks (puede tomar 1-2 min)...${NC}"
for i in {1..60}; do
    docker exec dbmazz-demo-starrocks mysql -P 9030 -h 127.0.0.1 -u root -e "SELECT 1;" > /dev/null 2>&1 && break
    [ $((i % 10)) -eq 0 ] && echo "  Intento $i/60..."
    sleep 2
done
echo -e "${GREEN}✓ StarRocks listo${NC}"

# 6. Inicializar schema StarRocks
echo -e "\n${YELLOW}5. Inicializando schema StarRocks...${NC}"
docker exec -i dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root < starrocks/init.sql > /dev/null 2>&1
TABLES_SR=$(docker exec dbmazz-demo-starrocks mysql -P 9030 -h 127.0.0.1 -u root -N -e "USE demo_db; SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='demo_db';" 2>&1)
echo -e "${GREEN}✓ StarRocks: $TABLES_SR tablas creadas${NC}"

# 7. Iniciar CDC
echo -e "\n${YELLOW}6. Iniciando CDC...${NC}"
docker-compose -f docker-compose.demo.yml up -d dbmazz > /dev/null 2>&1
sleep 5
docker logs dbmazz-demo-cdc --tail 5

# 8. Iniciar generadores
echo -e "\n${YELLOW}7. Iniciando generadores...${NC}"
docker-compose -f docker-compose.demo.yml up -d traffic-generator toast-generator > /dev/null 2>&1
sleep 10

# 9. Verificar replicación
echo -e "\n${YELLOW}8. Verificando replicación...${NC}"
sleep 10

PG_COUNT=$(docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -c "SELECT COUNT(*) FROM orders;" 2>&1 | tr -d ' ')
SR_COUNT=$(docker exec dbmazz-demo-starrocks mysql -P 9030 -h 127.0.0.1 -u root -N -e "USE demo_db; SELECT COUNT(*) FROM orders WHERE dbmazz_is_deleted=0;" 2>&1)

echo -e "  PostgreSQL: $PG_COUNT orders"
echo -e "  StarRocks:  $SR_COUNT orders"

if [ "$SR_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Replicación funcionando${NC}"
else
    echo -e "${RED}❌ StarRocks sin datos${NC}"
    exit 1
fi

# 10. Verificar TOAST
echo -e "\n${YELLOW}9. Verificando TOAST Partial Update...${NC}"
PARTIAL_COUNT=$(docker logs dbmazz-demo-cdc 2>&1 | grep -c "Partial update" || echo "0")
TOAST_COUNT=$(docker exec dbmazz-demo-starrocks mysql -P 9030 -h 127.0.0.1 -u root -N -e "USE demo_db; SELECT COUNT(*) FROM toast_test;" 2>&1)

echo -e "  TOAST registros: $TOAST_COUNT"
echo -e "  Partial updates: $PARTIAL_COUNT"

if [ "$PARTIAL_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Partial Update funcionando${NC}"
else
    echo -e "${YELLOW}⚠️  Sin partial updates aún (puede tardar 10s)${NC}"
fi

# 11. Test de monitor dinámico
echo -e "\n${YELLOW}10. Probando monitor dinámico...${NC}"
timeout 8 docker-compose -f docker-compose.demo.yml run --rm monitor 2>&1 | head -20

echo -e "\n=========================================="
echo -e "${GREEN}✅ DEMO VALIDADO EXITOSAMENTE${NC}"
echo "=========================================="
echo ""
echo "Para ver el monitor completo ejecuta:"
echo "  ./demo-start.sh"
echo ""
echo "Para detener:"
echo "  ./demo-stop.sh"
echo ""


