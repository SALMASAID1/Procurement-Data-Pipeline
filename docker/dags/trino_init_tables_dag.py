"""
Trino Schema Initialization DAG - Create Hive tables via Trino
Step 5 of the Procurement Pipeline Setup (replaces trino-init container)

This DAG creates the required Hive tables in Trino's file-based metastore:
- orders (raw layer)
- inventory (raw layer)  
- aggregated_orders (processed layer)
- net_demand (processed layer)

Run this DAG AFTER hdfs_initialize_dag to ensure HDFS directories exist.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from trino.dbapi import connect
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Trino connection settings
TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "airflow"

# DDL Statements for table creation
DDL_STATEMENTS = {
    "create_schema": """
        CREATE SCHEMA IF NOT EXISTS hive.procurement_raw
    """,
    
    "orders": """
        CREATE TABLE IF NOT EXISTS hive.procurement_raw.orders (
            order_id BIGINT,
            product_id INTEGER,
            quantity INTEGER,
            status VARCHAR(20),
            order_date DATE
        )
        WITH (
            format = 'PARQUET',
            external_location = 'hdfs://namenode:8020/raw/orders',
            partitioned_by = ARRAY['order_date']
        )
    """,
    
    "inventory": """
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
            external_location = 'hdfs://namenode:8020/raw/stock',
            partitioned_by = ARRAY['snapshot_date']
        )
    """,
    
    "aggregated_orders": """
        CREATE TABLE IF NOT EXISTS hive.procurement_raw.aggregated_orders (
            product_id INTEGER,
            total_quantity INTEGER,
            order_count INTEGER,
            order_date DATE
        )
        WITH (
            format = 'PARQUET'
        )
    """,
    
    "net_demand": """
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
            format = 'PARQUET'
        )
    """
}


def get_trino_connection():
    """Create a Trino connection."""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="hive",
        schema="procurement_raw"
    )


def execute_ddl(statement_name: str, **context):
    """Execute a single DDL statement."""
    ddl = DDL_STATEMENTS.get(statement_name)
    if not ddl:
        raise ValueError(f"Unknown DDL statement: {statement_name}")
    
    logger.info(f" Executing DDL: {statement_name}")
    
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(ddl.strip())
        # Fetch results to complete the query
        try:
            cursor.fetchall()
        except:
            pass  # Some DDL statements don't return results
        
        logger.info(f" Successfully executed: {statement_name}")
        return f"Created: {statement_name}"
    except Exception as e:
        logger.error(f"Failed to execute {statement_name}: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def verify_tables(**context):
    """Verify all tables were created successfully."""
    logger.info("Verifying table creation...")
    
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SHOW TABLES FROM hive.procurement_raw")
        tables = [row[0] for row in cursor.fetchall()]
        
        expected_tables = ['orders', 'inventory', 'aggregated_orders', 'net_demand']
        missing = [t for t in expected_tables if t not in tables]
        
        if missing:
            raise Exception(f"Missing tables: {missing}")
        
        logger.info(f"All tables verified: {tables}")
        return f"Verified tables: {tables}"
    finally:
        cursor.close()
        conn.close()



# DAG Definition
with DAG(
    'trino_init_tables',
    default_args=default_args,
    description='Initialize Trino/Hive tables for Procurement Pipeline',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['setup', 'trino', 'ddl']
) as dag:
    
    # Task 1: Create schema
    task_create_schema = PythonOperator(
        task_id='create_schema',
        python_callable=execute_ddl,
        op_kwargs={'statement_name': 'create_schema'}
    )
    
    # Task 2: Create orders table
    task_create_orders = PythonOperator(
        task_id='create_orders_table',
        python_callable=execute_ddl,
        op_kwargs={'statement_name': 'orders'}
    )
    
    # Task 3: Create inventory table
    task_create_inventory = PythonOperator(
        task_id='create_inventory_table',
        python_callable=execute_ddl,
        op_kwargs={'statement_name': 'inventory'}
    )
    
    # Task 4: Create aggregated_orders table
    task_create_aggregated = PythonOperator(
        task_id='create_aggregated_orders_table',
        python_callable=execute_ddl,
        op_kwargs={'statement_name': 'aggregated_orders'}
    )
    
    # Task 5: Create net_demand table
    task_create_net_demand = PythonOperator(
        task_id='create_net_demand_table',
        python_callable=execute_ddl,
        op_kwargs={'statement_name': 'net_demand'}
    )
    
    # Task 6: Verify all tables
    task_verify = PythonOperator(
        task_id='verify_tables',
        python_callable=verify_tables
    )
    
    # Dependencies: schema first, then tables in parallel, then verify
    task_create_schema >> [task_create_orders, task_create_inventory, 
                          task_create_aggregated, task_create_net_demand] >> task_verify
