-- ============================================================
-- External Table Definitions for HDFS (Hive Metastore)
-- Purpose: Map Parquet files in HDFS to SQL-queryable tables
-- Structure: /raw/orders/YYYY-MM-DD/ and /raw/stock/YYYY-MM-DD/
-- ============================================================

-- Create schema for raw operational data
CREATE SCHEMA IF NOT EXISTS procurement_raw;

-- =====================================================
-- ORDERS EXTERNAL TABLE (Partitioned by Date)
-- Purpose: Daily customer purchase orders
-- Location: /raw/orders/YYYY-MM-DD/data.parquet
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS procurement_raw.orders (
    order_id BIGINT,
    product_id INT,
    quantity INT,
    supplier_id INT,
    status VARCHAR(20)
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/raw/orders';

-- =====================================================
-- INVENTORY SNAPSHOTS EXTERNAL TABLE
-- Purpose: Daily stock levels per product/warehouse
-- Location: /raw/stock/YYYY-MM-DD/data.parquet
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS procurement_raw.inventory (
    product_id INT,
    available_qty INT,
    reserved_qty INT,
    safety_stock INT,
    warehouse_id INT
)
PARTITIONED BY (snapshot_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/raw/stock';

-- =====================================================
-- AGGREGATED ORDERS TABLE (Processed Layer)
-- Purpose: Summed daily demand per product
-- Location: /processed/aggregated_orders/YYYY-MM-DD/
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS procurement_raw.aggregated_orders (
    product_id INT,
    total_quantity INT,
    order_count INT
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/processed/aggregated_orders';

-- =====================================================
-- NET DEMAND TABLE (Processed Layer)
-- Purpose: Pre-export net demand calculations
-- Location: /processed/net_demand/YYYY-MM-DD/
-- =====================================================
CREATE EXTERNAL TABLE IF NOT EXISTS procurement_raw.net_demand (
    product_id INT,
    product_name VARCHAR(255),
    supplier_id INT,
    supplier_name VARCHAR(255),
    net_demand INT,
    unit_cost DECIMAL(10,2),
    estimated_cost DECIMAL(10,2)
)
PARTITIONED BY (calculation_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/processed/net_demand';

-- =====================================================
-- PARTITION MANAGEMENT COMMANDS
-- Run MSCK REPAIR after loading data to auto-discover partitions
-- =====================================================
-- MSCK REPAIR TABLE procurement_raw.orders;
-- MSCK REPAIR TABLE procurement_raw.inventory;
-- MSCK REPAIR TABLE procurement_raw.aggregated_orders;
-- MSCK REPAIR TABLE procurement_raw.net_demand;

-- =====================================================
-- MANUAL PARTITION ADDITION (Alternative method)
-- Use when MSCK REPAIR is too slow or for specific dates
-- =====================================================
-- ALTER TABLE procurement_raw.orders ADD IF NOT EXISTS PARTITION (order_date='2026-01-04');
-- ALTER TABLE procurement_raw.inventory ADD IF NOT EXISTS PARTITION (snapshot_date='2026-01-04');
-- ALTER TABLE procurement_raw.aggregated_orders ADD IF NOT EXISTS PARTITION (order_date='2026-01-04');
-- ALTER TABLE procurement_raw.net_demand ADD IF NOT EXISTS PARTITION (calculation_date='2026-01-04');
