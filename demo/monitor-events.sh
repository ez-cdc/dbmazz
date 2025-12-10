#!/bin/bash
# Monitor events per second in real-time

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}🔍 dbmazz Event Rate Monitor${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""

while true; do
    clear
    echo -e "${CYAN}🔍 dbmazz Event Rate Monitor - $(date '+%H:%M:%S')${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo ""
    
    # PostgreSQL source
    echo -e "${GREEN}📊 PostgreSQL (Source)${NC}"
    docker exec dbmazz-demo-postgres psql -U postgres -d demo_db -t -A -F'|' -c "
    SELECT 
        table_name,
        total_records,
        last_10s,
        events_per_sec_10s || ' eps',
        last_60s,
        events_per_sec_60s || ' eps'
    FROM (
        SELECT 
            'orders' as table_name,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '10 seconds') as last_10s,
            ROUND(COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '10 seconds')::numeric / 10, 2) as events_per_sec_10s,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '60 seconds') as last_60s,
            ROUND(COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '60 seconds')::numeric / 60, 2) as events_per_sec_60s
        FROM orders
        UNION ALL
        SELECT 
            'order_items' as table_name,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '10 seconds') as last_10s,
            ROUND(COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '10 seconds')::numeric / 10, 2) as events_per_sec_10s,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '60 seconds') as last_60s,
            ROUND(COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '60 seconds')::numeric / 60, 2) as events_per_sec_60s
        FROM order_items
        UNION ALL
        SELECT 
            'TOTAL' as table_name,
            (SELECT COUNT(*) FROM orders) + (SELECT COUNT(*) FROM order_items) as total_records,
            (SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '10 seconds') + 
            (SELECT COUNT(*) FROM order_items WHERE created_at > NOW() - INTERVAL '10 seconds') as last_10s,
            ROUND(((SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '10 seconds') + 
                   (SELECT COUNT(*) FROM order_items WHERE created_at > NOW() - INTERVAL '10 seconds'))::numeric / 10, 2) as events_per_sec_10s,
            (SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '60 seconds') + 
            (SELECT COUNT(*) FROM order_items WHERE created_at > NOW() - INTERVAL '60 seconds') as last_60s,
            ROUND(((SELECT COUNT(*) FROM orders WHERE created_at > NOW() - INTERVAL '60 seconds') + 
                   (SELECT COUNT(*) FROM order_items WHERE created_at > NOW() - INTERVAL '60 seconds'))::numeric / 60, 2) as events_per_sec_60s
    ) sub;
    " | column -t -s'|'
    
    echo ""
    echo -e "${YELLOW}📈 StarRocks (Sink)${NC}"
    docker exec dbmazz-demo-starrocks mysql -h 127.0.0.1 -P 9030 -u root demo_db -sNe "
    SELECT 
        'orders' as table_name,
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)) as last_10s,
        ROUND(COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)) / 10, 2) as events_per_sec_10s,
        COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 60 SECOND)) as last_60s,
        ROUND(COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 60 SECOND)) / 60, 2) as events_per_sec_60s
    FROM orders
    WHERE dbmazz_is_deleted = FALSE
    UNION ALL
    SELECT 
        'order_items' as table_name,
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)) as last_10s,
        ROUND(COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)) / 10, 2) as events_per_sec_10s,
        COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 60 SECOND)) as last_60s,
        ROUND(COUNT(*) FILTER (WHERE dbmazz_synced_at > DATE_SUB(NOW(), INTERVAL 60 SECOND)) / 60, 2) as events_per_sec_60s
    FROM order_items
    WHERE dbmazz_is_deleted = FALSE;
    " | column -t
    
    echo ""
    echo -e "${CYAN}────────────────────────────────────────────────────────────${NC}"
    echo -e "${CYAN}Press Ctrl+C to exit | Refreshing every 2 seconds${NC}"
    
    sleep 2
done

