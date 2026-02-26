-- dbmazz Demo: StarRocks Schema
-- Target tables for CDC replication

-- Create database
CREATE DATABASE IF NOT EXISTS demo_db;
USE demo_db;

-- Orders table (with CDC audit columns)
CREATE TABLE IF NOT EXISTS orders (
    id INT NOT NULL,
    customer_id INT NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at DATETIME,
    updated_at DATETIME,
    dbmazz_op_type TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE',
    dbmazz_is_deleted BOOLEAN COMMENT 'Soft delete flag',
    dbmazz_synced_at DATETIME COMMENT 'Timestamp cuando CDC procesó este registro',
    dbmazz_cdc_version BIGINT COMMENT 'PostgreSQL LSN para ordenamiento'
) 
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replication_num" = "1"
);

-- Order Items table (with CDC audit columns)
CREATE TABLE IF NOT EXISTS order_items (
    id INT NOT NULL,
    order_id INT NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at DATETIME,
    dbmazz_op_type TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE',
    dbmazz_is_deleted BOOLEAN COMMENT 'Soft delete flag',
    dbmazz_synced_at DATETIME COMMENT 'Timestamp cuando CDC procesó este registro',
    dbmazz_cdc_version BIGINT COMMENT 'PostgreSQL LSN para ordenamiento'
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replication_num" = "1"
);

-- TOAST Test Table (for testing large JSON replication)
-- This table tests the TOAST/Partial Update feature
CREATE TABLE IF NOT EXISTS toast_test (
    id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    large_json JSON COMMENT 'Large JSON field for TOAST testing',
    created_at DATETIME,
    updated_at DATETIME,
    dbmazz_op_type TINYINT COMMENT '0=INSERT, 1=UPDATE, 2=DELETE',
    dbmazz_is_deleted BOOLEAN COMMENT 'Soft delete flag',
    dbmazz_synced_at DATETIME COMMENT 'Timestamp cuando CDC procesó este registro',
    dbmazz_cdc_version BIGINT COMMENT 'PostgreSQL LSN para ordenamiento'
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
    "replication_num" = "1"
);

