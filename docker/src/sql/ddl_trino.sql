-- ============================================================
-- Trino DDL for File-Based Metastore
-- Purpose: Create external tables pointing to HDFS locations
-- ============================================================

-- Create schema for raw operational data
CREATE SCHEMA IF NOT EXISTS hive.procurement_raw;

-- =====================================================
-- ORDERS EXTERNAL TABLE
-- Purpose: Daily customer purchase orders
-- Location: /raw/orders/
-- =====================================================
CREATE TABLE IF NOT EXISTS hive.procurement_raw.orders (
    order_id BIGINT,
    product_id INTEGER,
    quantity INTEGER,
    supplier_id INTEGER,
    status VARCHAR(20),
    order_date DATE
)
WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:8020/raw/orders'
);

-- =====================================================
-- INVENTORY SNAPSHOTS EXTERNAL TABLE
-- Purpose: Daily stock levels per product/warehouse
-- Location: /raw/stock/
-- =====================================================
CREATE TABLE IF NOT EXISTS hive.procurement_raw.inventory (
    product_id INTEGER,
    available_qty INTEGER,
    reserved_qty INTEGER,
    safety_stock INTEGER,
    warehouse_id INTEGER,
    snapshot_date DATE
)
WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:8020/raw/stock'
);

-- =====================================================
-- AGGREGATED ORDERS TABLE (Processed Layer)
-- Purpose: Summed daily demand per product
-- Location: /processed/aggregated_orders/
-- =====================================================
CREATE TABLE IF NOT EXISTS hive.procurement_raw.aggregated_orders (
    product_id INTEGER,
    total_quantity INTEGER,
    order_count INTEGER,
    order_date DATE
)
WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:8020/processed/aggregated_orders'
);

-- =====================================================
-- NET DEMAND TABLE (Processed Layer)
-- Purpose: Pre-export net demand calculations
-- Location: /processed/net_demand/
-- =====================================================
CREATE TABLE IF NOT EXISTS hive.procurement_raw.net_demand (
    product_id INTEGER,
    product_name VARCHAR(255),
    supplier_id INTEGER,
    supplier_name VARCHAR(255),
    net_demand INTEGER,
    unit_cost DECIMAL(10,2),
    estimated_cost DECIMAL(10,2),
    calculation_date DATE
)
WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:8020/processed/net_demand'
);
