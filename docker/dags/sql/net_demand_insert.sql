-- NET DEMAND INSERT (Persists to net_demand table)
-- Formula: max(0, Orders + Safety Stock - (Available - Reserved))

INSERT INTO hive.procurement_raw.net_demand
WITH daily_orders AS (
    SELECT 
        o.product_id,
        SUM(o.quantity) AS total_ordered
    FROM hive.procurement_raw.orders o
    WHERE o.order_date = DATE '${EXEC_DATE}'
    GROUP BY o.product_id
),

aggregated_inventory AS (
    SELECT
        i.product_id,
        SUM(i.available_qty) AS available_qty,
        SUM(i.reserved_qty)  AS reserved_qty,
        MAX(i.safety_stock)  AS safety_stock
    FROM hive.procurement_raw.inventory i
    WHERE i.snapshot_date = DATE '${EXEC_DATE}'
    GROUP BY i.product_id
),

ranked_suppliers AS (
    SELECT
        ps.product_id,
        ps.supplier_id,
        ps.unit_cost,
        ps.priority,
        ROW_NUMBER() OVER (
            PARTITION BY ps.product_id
            ORDER BY ps.priority ASC, ps.unit_cost ASC
        ) AS supplier_rank
    FROM postgres.master_data.product_suppliers ps
),

net_demand_calc AS (
    SELECT 
        CAST(p.product_id AS INTEGER) AS product_id,
        CAST(p.product_name AS VARCHAR(255)) AS product_name,
        CAST(s.supplier_id AS INTEGER) AS supplier_id,
        CAST(s.supplier_name AS VARCHAR(255)) AS supplier_name,
        CAST(GREATEST(
            0,
            COALESCE(do.total_ordered, 0)
            + COALESCE(ai.safety_stock, p.safety_stock_level)
            - (COALESCE(ai.available_qty, 0) - COALESCE(ai.reserved_qty, 0))
        ) AS INTEGER) AS net_demand,
        CAST(rs.unit_cost AS DECIMAL(10,2)) AS unit_cost
    FROM postgres.master_data.products p
    LEFT JOIN daily_orders do ON p.product_id = do.product_id
    LEFT JOIN aggregated_inventory ai ON p.product_id = ai.product_id
    INNER JOIN ranked_suppliers rs ON p.product_id = rs.product_id AND rs.supplier_rank = 1
    INNER JOIN postgres.master_data.suppliers s ON rs.supplier_id = s.supplier_id
    WHERE p.is_active = TRUE AND s.is_active = TRUE
)

SELECT
    product_id,
    product_name,
    supplier_id,
    supplier_name,
    net_demand,
    unit_cost,
    CAST(net_demand * unit_cost AS DECIMAL(10,2)) AS estimated_cost,
    DATE '${EXEC_DATE}' AS calculation_date
FROM net_demand_calc
WHERE net_demand > 0
