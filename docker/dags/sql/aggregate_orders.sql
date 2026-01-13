-- AGGREGATED ORDERS

-- This query aggregates orders per product for a given date
-- Results are inserted into the aggregated_orders table

INSERT INTO hive.procurement_raw.aggregated_orders
SELECT 
    product_id,
    CAST(SUM(quantity) AS INTEGER) AS total_quantity,
    CAST(COUNT(DISTINCT order_id) AS INTEGER) AS order_count,
    order_date
FROM hive.procurement_raw.orders
WHERE order_date = DATE '${EXEC_DATE}'
GROUP BY product_id, order_date
