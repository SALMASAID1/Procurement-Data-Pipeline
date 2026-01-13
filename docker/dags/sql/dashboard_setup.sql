-- ============================================================
-- Metabase Dashboard Queries - Procurement KPIs
-- ============================================================
-- These queries are designed for Metabase automation
-- Use Metabase variables: {{date}} for dynamic date filtering
-- Default to latest available date when no parameter provided
-- ============================================================

-- Query 1: Total Daily Orders
-- Description: Sum of all order quantities for a specific date
-- Metabase Variable: {{date}} (default to latest order_date)
-- Usage: Add a Date parameter named "date" in Metabase
SELECT 
    SUM(total_quantity) as total_daily_orders
FROM hive.procurement_raw.aggregated_orders 
WHERE order_date = (
    SELECT MAX(order_date) 
    FROM hive.procurement_raw.aggregated_orders
);
-- For parameterized version in Metabase:
-- WHERE order_date = DATE [[{{date}}]]


-- Query 2: Net Demand by Product
-- Description: Products with positive net demand
-- Shows which products need to be ordered
SELECT 
    product_name, 
    net_demand
FROM hive.procurement_raw.net_demand 
WHERE net_demand > 0
    AND calculation_date = (
        SELECT MAX(calculation_date) 
        FROM hive.procurement_raw.net_demand
    )
ORDER BY net_demand DESC;
-- For parameterized version in Metabase:
-- AND calculation_date = DATE [[{{date}}]]


-- Query 3: Supplier Order Volume
-- Description: Total demand grouped by supplier
-- Shows which suppliers will receive the most orders
SELECT 
    supplier_name, 
    SUM(net_demand) as total_demand
FROM hive.procurement_raw.net_demand
WHERE calculation_date = (
    SELECT MAX(calculation_date) 
    FROM hive.procurement_raw.net_demand
)
GROUP BY supplier_name
ORDER BY total_demand DESC;
-- For parameterized version in Metabase:
-- WHERE calculation_date = DATE [[{{date}}]]


-- Query 4: Exception Count
-- Description: Count of data quality exceptions
-- Note: Exceptions are logged to JSON files in HDFS /logs/exceptions/
-- This query requires reading from HDFS or creating an external table
-- For now, use a placeholder or create an external table pointing to logs
SELECT 
    'Check HDFS /logs/exceptions/ for latest exception logs' as info,
    0 as exception_count;
-- Alternative: Create external table for exception logs if needed


-- Query 5: Total Procurement Cost
-- Description: Sum of estimated costs for all net demand
-- Shows total expected procurement spend
SELECT 
    CAST(SUM(estimated_cost) AS DECIMAL(12,2)) as total_procurement_cost
FROM hive.procurement_raw.net_demand
WHERE calculation_date = (
    SELECT MAX(calculation_date) 
    FROM hive.procurement_raw.net_demand
);
-- For parameterized version in Metabase:
-- WHERE calculation_date = DATE [[{{date}}]]


-- ============================================================
-- Additional Useful Queries for Metabase Dashboard
-- ============================================================

-- Query 6: Daily Trend - Orders Over Time
SELECT 
    order_date,
    SUM(total_quantity) as daily_orders,
    COUNT(DISTINCT product_id) as products_ordered
FROM hive.procurement_raw.aggregated_orders
WHERE order_date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY order_date
ORDER BY order_date DESC;


-- Query 7: Cost by Supplier
SELECT 
    supplier_name,
    CAST(SUM(estimated_cost) AS DECIMAL(12,2)) as supplier_cost,
    SUM(net_demand) as total_units
FROM hive.procurement_raw.net_demand
WHERE calculation_date = (
    SELECT MAX(calculation_date) 
    FROM hive.procurement_raw.net_demand
)
GROUP BY supplier_name
ORDER BY supplier_cost DESC;


-- Query 8: Inventory Status
SELECT 
    product_id,
    available_qty,
    reserved_qty,
    safety_stock,
    (available_qty - reserved_qty) as net_available,
    CASE 
        WHEN (available_qty - reserved_qty) < safety_stock THEN 'Low Stock'
        WHEN (available_qty - reserved_qty) < (safety_stock * 1.5) THEN 'Warning'
        ELSE 'Healthy'
    END as stock_status
FROM hive.procurement_raw.inventory
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) 
    FROM hive.procurement_raw.inventory
)
ORDER BY (available_qty - reserved_qty) ASC;


-- Query 9: Product Demand Summary
SELECT 
    p.product_name,
    nd.net_demand,
    nd.unit_cost,
    nd.estimated_cost,
    nd.supplier_name
FROM hive.procurement_raw.net_demand nd
WHERE nd.calculation_date = (
    SELECT MAX(calculation_date) 
    FROM hive.procurement_raw.net_demand
)
    AND nd.net_demand > 0
ORDER BY nd.estimated_cost DESC;


-- ============================================================
-- Metabase Setup Instructions
-- ============================================================
-- 1. Connect Metabase to Trino database
--    - Host: trino (container name)
--    - Port: 8080
--    - Catalog: hive
--    - Schema: procurement_raw
--
-- 2. Create Dashboard with these cards:
--    - Card 1: Total Daily Orders (Number widget)
--    - Card 2: Net Demand by Product (Bar chart)
--    - Card 3: Supplier Order Volume (Pie chart)
--    - Card 4: Exception Count (Number widget)
--    - Card 5: Total Procurement Cost (Number widget with $ formatting)
--
-- 3. Add Date Parameter (optional):
--    - Type: Date
--    - Variable name: date
--    - Default: Latest date
--    - Map to queries using [[{{date}}]] syntax
--
-- 4. Set Auto-refresh (optional):
--    - Dashboard Settings → Auto-refresh
--    - Recommended: 5-10 minutes for near real-time updates
--
-- ============================================================
