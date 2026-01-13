-- ============================================================
-- NET DEMAND CALCULATION (MRP – PRODUCTION SAFE VERSION)
-- Author: Tamzirt Mohamed
-- Formula: max(0, Orders + Safety Stock - (Available - Reserved))
-- ============================================================

WITH daily_orders AS (
    SELECT 
        o.product_id,
        SUM(o.quantity) AS total_ordered,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM hive.procurement_raw.orders o
    WHERE o.order_date = DATE '${EXEC_DATE}'
    GROUP BY o.product_id
),

-- Aggregate inventory at PRODUCT level to avoid warehouse duplication
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

-- Select ONE preferred supplier per product
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
        p.product_id,
        p.product_name,
        p.product_code,

        s.supplier_id,
        s.supplier_name,
        s.lead_time_days,

        COALESCE(do.total_ordered, 0) AS orders,

        GREATEST(
            0,
            COALESCE(do.total_ordered, 0)
            + COALESCE(ai.safety_stock, p.safety_stock_level)
            - (COALESCE(ai.available_qty, 0) - COALESCE(ai.reserved_qty, 0))
        ) AS net_demand,

        rs.unit_cost,
        rs.priority AS supplier_priority

    FROM postgres.master_data.products p

    LEFT JOIN daily_orders do
        ON p.product_id = do.product_id

    LEFT JOIN aggregated_inventory ai
        ON p.product_id = ai.product_id

    INNER JOIN ranked_suppliers rs
        ON p.product_id = rs.product_id
       AND rs.supplier_rank = 1   -- ONLY preferred supplier

    INNER JOIN postgres.master_data.suppliers s
        ON rs.supplier_id = s.supplier_id

    WHERE p.is_active = TRUE
      AND s.is_active = TRUE
)

SELECT
    *,
    net_demand * unit_cost AS estimated_cost,
    DATE '${EXEC_DATE}' AS calculation_date
FROM net_demand_calc
WHERE net_demand > 0
ORDER BY supplier_id, supplier_priority, net_demand DESC
