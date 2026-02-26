-- dbmazz Demo: Seed Data
-- Inserts initial realistic e-commerce data

-- Insert 1000 initial orders
INSERT INTO orders (customer_id, total, status, created_at, updated_at)
SELECT 
    (random() * 1000 + 1)::int as customer_id,
    (random() * 500 + 10)::decimal(10,2) as total,
    CASE (random() * 4)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        ELSE 'delivered'
    END as status,
    NOW() - (random() * interval '30 days') as created_at,
    NOW() - (random() * interval '30 days') as updated_at
FROM generate_series(1, 1000);

-- Insert 3000 order items (avg 3 items per order)
INSERT INTO order_items (order_id, product_name, quantity, price, created_at)
SELECT 
    (random() * 999 + 1)::int as order_id,  -- Changed from 1000+1 to 999+1 to ensure range 1-1000
    'Product ' || (random() * 100 + 1)::int as product_name,
    (random() * 5 + 1)::int as quantity,
    (random() * 100 + 5)::decimal(10,2) as price,
    NOW() - (random() * interval '30 days') as created_at
FROM generate_series(1, 3000);

-- Log success
DO $$
BEGIN
    RAISE NOTICE 'Seed data inserted: % orders, % order_items', 
        (SELECT COUNT(*) FROM orders),
        (SELECT COUNT(*) FROM order_items);
END $$;

