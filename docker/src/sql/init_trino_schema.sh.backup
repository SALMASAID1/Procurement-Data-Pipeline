#!/bin/bash
echo '=========================================='
echo 'Waiting for Trino to be ready...'
echo '=========================================='

TRINO_HOST="${TRINO_HOST:-trino}"
TRINO_PORT="${TRINO_PORT:-8080}"

# Wait for Trino to be available
for i in $(seq 1 60); do
  if trino --server "http://${TRINO_HOST}:${TRINO_PORT}" --execute "SELECT 1" > /dev/null 2>&1; then
    echo "Trino is ready!"
    break
  fi
  echo "Trino not ready, waiting 5s... (attempt $i/60)"
  sleep 5
done

# Check if Trino is actually ready
if ! trino --server "http://${TRINO_HOST}:${TRINO_PORT}" --execute "SELECT 1" > /dev/null 2>&1; then
  echo "ERROR: Trino did not become ready in time"
  exit 1
fi

echo '=========================================='
echo 'Creating HDFS directories...'
echo '=========================================='

# Note: HDFS directories should already exist from namenode init
# This is just for verification

echo '=========================================='
echo 'Running Trino DDL statements...'
echo '=========================================='

TRINO_CMD="trino --server http://${TRINO_HOST}:${TRINO_PORT}"

# Execute each statement separately (Trino CLI doesn't support multiple statements well)
$TRINO_CMD --execute "CREATE SCHEMA IF NOT EXISTS hive.procurement_raw"
echo "✓ Schema procurement_raw created"

$TRINO_CMD --execute "CREATE TABLE IF NOT EXISTS hive.procurement_raw.orders (order_id BIGINT, product_id INTEGER, quantity INTEGER, supplier_id INTEGER, status VARCHAR(20), order_date DATE) WITH (format = 'PARQUET', external_location = 'hdfs://namenode:8020/raw/orders')"
echo "✓ Table orders created"

$TRINO_CMD --execute "CREATE TABLE IF NOT EXISTS hive.procurement_raw.inventory (product_id INTEGER, available_qty INTEGER, reserved_qty INTEGER, safety_stock INTEGER, warehouse_id INTEGER, snapshot_date DATE) WITH (format = 'PARQUET', external_location = 'hdfs://namenode:8020/raw/stock')"
echo "✓ Table inventory created"

$TRINO_CMD --execute "CREATE TABLE IF NOT EXISTS hive.procurement_raw.aggregated_orders (product_id INTEGER, total_quantity INTEGER, order_count INTEGER, order_date DATE) WITH (format = 'PARQUET', external_location = 'hdfs://namenode:8020/processed/aggregated_orders')"
echo "✓ Table aggregated_orders created"

$TRINO_CMD --execute "CREATE TABLE IF NOT EXISTS hive.procurement_raw.net_demand (product_id INTEGER, product_name VARCHAR(255), supplier_id INTEGER, supplier_name VARCHAR(255), net_demand INTEGER, unit_cost DECIMAL(10,2), estimated_cost DECIMAL(10,2), calculation_date DATE) WITH (format = 'PARQUET', external_location = 'hdfs://namenode:8020/processed/net_demand')"
echo "✓ Table net_demand created"

echo '=========================================='
echo 'Verifying tables...'
echo '=========================================='
$TRINO_CMD --execute "SHOW TABLES FROM hive.procurement_raw"

echo '=========================================='
echo 'Trino schema initialization complete!'
echo '=========================================='
