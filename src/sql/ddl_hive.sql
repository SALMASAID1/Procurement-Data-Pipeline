-- External table definitions for HDFS (Hive Metastore)
-- These tables map to Parquet files stored in HDFS

-- Create schema
CREATE SCHEMA IF NOT EXISTS procurement_raw;

-- =====================================================
-- ORDERS EXTERNAL TABLE (Partitioned by Date)
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
LOCATION 'hdfs://namenode:9000/raw/orders';

-- =====================================================
-- INVENTORY SNAPSHOTS EXTERNAL TABLE
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
LOCATION 'hdfs://namenode:9000/raw/stock';

-- =====================================================
-- PARTITION REPAIR COMMANDS
-- Run these after loading data to register partitions
-- =====================================================
-- MSCK REPAIR TABLE procurement_raw.orders;
-- MSCK REPAIR TABLE procurement_raw.inventory;

-- =====================================================
-- ALTERNATIVE: Manual partition addition
-- =====================================================
-- ALTER TABLE procurement_raw.orders ADD PARTITION (order_date='2025-01-01');
-- ALTER TABLE procurement_raw.inventory ADD PARTITION (snapshot_date='2025-01-01');
