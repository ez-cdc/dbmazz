-- dbmazz Demo: StarRocks Schema
-- Target tables for CDC replication

-- Create database
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

-- Orders table (with op_type for CDC)
CREATE TABLE IF NOT EXISTS orders (
    id INT NOT NULL,
    customer_id INT NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at DATETIME,
    updated_at DATETIME,
    op_type TINYINT COMMENT '0=UPSERT, 1=DELETE'
) 
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replication_num" = "1"
);

-- Order Items table (with op_type for CDC)
CREATE TABLE IF NOT EXISTS order_items (
    id INT NOT NULL,
    order_id INT NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at DATETIME,
    op_type TINYINT COMMENT '0=UPSERT, 1=DELETE'
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replication_num" = "1"
);

