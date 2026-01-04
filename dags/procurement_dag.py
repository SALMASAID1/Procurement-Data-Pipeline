# Main Airflow orchestration
"""
Procurement Data Pipeline DAG

This DAG orchestrates the daily data pipeline:
1. Generate synthetic procurement data (orders & inventory)
2. Upload partitioned parquet files to HDFS
3. Sync Hive metastore partitions via Trino
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'procurement',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'procurement_data_pipeline',
    default_args=default_args,
    description='Daily procurement data ingestion pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 31),
    catchup=False,
    tags=['procurement', 'data-pipeline'],
)

# Task 1: Generate synthetic data
generate_data = BashOperator(
    task_id='generate_data',
    bash_command='''
        cd /opt/airflow/dags/../src/generator && \
        python main.py --date {{ ds }}
    ''',
    dag=dag,
)

# Task 2: Upload orders to HDFS
upload_orders = BashOperator(
    task_id='upload_orders_to_hdfs',
    bash_command='''
        hdfs dfs -mkdir -p /raw/orders/order_date={{ ds }} && \
        hdfs dfs -put -f /opt/airflow/data/raw/orders/order_date={{ ds }}/* \
            /raw/orders/order_date={{ ds }}/
    ''',
    dag=dag,
)

# Task 3: Upload inventory to HDFS
upload_inventory = BashOperator(
    task_id='upload_inventory_to_hdfs',
    bash_command='''
        hdfs dfs -mkdir -p /raw/stock/snapshot_date={{ ds }} && \
        hdfs dfs -put -f /opt/airflow/data/raw/stock/snapshot_date={{ ds }}/* \
            /raw/stock/snapshot_date={{ ds }}/
    ''',
    dag=dag,
)

# Task 4: Sync partitions in Hive metastore via Trino
sync_partitions = BashOperator(
    task_id='sync_hive_partitions',
    bash_command='''
        trino --server trino:8080 --execute \
            "CALL hive.system.sync_partition_metadata('procurement_raw', 'orders', 'ADD')" && \
        trino --server trino:8080 --execute \
            "CALL hive.system.sync_partition_metadata('procurement_raw', 'inventory', 'ADD')"
    ''',
    dag=dag,
)

# Define task dependencies
generate_data >> [upload_orders, upload_inventory] >> sync_partitions
