-- dbmazz Quickstart: E-commerce Schema + Seed Data
-- Used by docker-compose.production.yml --profile quickstart

-- Tables
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_name VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order ON order_items(order_id);

-- REPLICA IDENTITY FULL required for CDC with deletes
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Publication for dbmazz
CREATE PUBLICATION dbmazz_pub FOR TABLE orders, order_items;

-- Seed: 500 orders + 1500 order items
INSERT INTO orders (customer_id, total, status, created_at, updated_at)
SELECT
    (random() * 1000 + 1)::int,
    (random() * 500 + 10)::decimal(10,2),
    CASE (random() * 3)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        ELSE 'delivered'
    END,
    NOW() - (random() * interval '30 days'),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 500);

INSERT INTO order_items (order_id, product_name, quantity, price, created_at)
SELECT
    (random() * 499 + 1)::int,
    'Product ' || (random() * 100 + 1)::int,
    (random() * 5 + 1)::int,
    (random() * 100 + 5)::decimal(10,2),
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 1500);
