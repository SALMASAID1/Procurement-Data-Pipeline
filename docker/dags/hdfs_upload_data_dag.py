"""
HDFS Upload Sample Data DAG - Generate and upload sample procurement data
Step 4 of the Procurement Pipeline Setup

This DAG:
1. Generates sample Orders and Inventory data using Faker
2. Converts to Parquet format
3. Uploads to HDFS via WebHDFS REST API
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date, timedelta
import requests
import logging
import random
import io
import os
from dataclasses import dataclass
from typing import List

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# WebHDFS endpoint
WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"

# Trino connection settings (for partition sync)
TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "airflow"

# Configuration - Must match PostgreSQL master data
VALID_PRODUCT_IDS = [1, 2, 3, 4, 5]
ORDER_STATUS = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"]

TODAY = datetime.now().strftime("%Y-%m-%d")
SAMPLE_DATES = [TODAY]

# Local data path for CSV files
LOCAL_DATA_PATH = "/opt/airflow/data"



# Data Models (inline to avoid import issues in Airflow container)
@dataclass
class Order:
    """Represents a purchase order in the procurement system."""
    order_id: int
    product_id: int
    quantity: int
    order_date: date
    status: str = "PENDING"
    
    def to_dict(self) -> dict:
        return {
            'order_id': self.order_id,
            'product_id': self.product_id,
            'quantity': self.quantity,
            'order_date': self.order_date.isoformat(),
            'status': self.status
        }


@dataclass
class InventorySnapshot:
    """Represents a daily snapshot of inventory levels."""
    product_id: int
    available_qty: int
    reserved_qty: int
    safety_stock: int
    snapshot_date: date
    warehouse_id: int = 1
    
    def to_dict(self) -> dict:
        return {
            'product_id': self.product_id,
            'available_qty': self.available_qty,
            'reserved_qty': self.reserved_qty,
            'safety_stock': self.safety_stock,
            'snapshot_date': self.snapshot_date.isoformat(),
            'warehouse_id': self.warehouse_id
        }



# Data Generation Functions
def generate_orders(exec_date: date, num_orders: int = 100) -> List[Order]:
    """Generate daily purchase orders with valid foreign keys."""
    orders = []
    for i in range(num_orders):
        order_id = int(f"{exec_date.strftime('%Y%m%d')}{i:05d}")
        order = Order(
            order_id=order_id,
            product_id=random.choice(VALID_PRODUCT_IDS),
            quantity=random.randint(1, 100),
            order_date=exec_date,
            status=random.choice(ORDER_STATUS)
        )
        orders.append(order)
    return orders

def generate_inventory(exec_date: date) -> List[InventorySnapshot]:
    """Generate daily inventory snapshots for all products."""
    snapshots = []
    for product_id in VALID_PRODUCT_IDS:
        snapshot = InventorySnapshot(
            product_id=product_id,
            available_qty=random.randint(50, 500),
            reserved_qty=random.randint(0, 50),
            safety_stock=random.randint(20, 100),
            snapshot_date=exec_date,
            warehouse_id=1
        )
        snapshots.append(snapshot)
    return snapshots



# CSV Save Functions
def save_to_csv(data: List, data_type: str, exec_date: str) -> str:
    """
    Save data to local CSV file before Parquet conversion.
    
    Args:
        data: List of data objects (Order or InventorySnapshot)
        data_type: Type of data ('orders' or 'stock')
        exec_date: Execution date in YYYY-MM-DD format
    
    Returns:
        Path to the saved CSV file
    """
    import pandas as pd
    
    # Create directory structure: /raw/orders/YYYY-MM-DD/ or /raw/stock/YYYY-MM-DD/
    csv_dir = os.path.join(LOCAL_DATA_PATH, "raw", data_type, exec_date)
    os.makedirs(csv_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%H%M%S")
    csv_filename = f"{data_type}_{exec_date}_{timestamp}.csv"
    csv_path = os.path.join(csv_dir, csv_filename)
    
    # Convert data objects to DataFrame and save as CSV
    records = [item.to_dict() for item in data]
    df = pd.DataFrame(records)
    df.to_csv(csv_path, index=False)
    
    logger.info(f"Saved CSV: {csv_path} ({len(data)} records)")
    return csv_path



# Parquet Creation Functions
def create_parquet_buffer(data: List, data_type: str) -> bytes:
    """Convert data objects to Parquet bytes using PyArrow."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    records = [item.to_dict() for item in data]
    df = pd.DataFrame(records)
    
    # Convert to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Write to bytes buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    logger.info(f" Created {data_type} Parquet: {len(data)} records, {len(buffer.getvalue())} bytes")
    return buffer.getvalue()



# WebHDFS Upload Functions
def upload_to_hdfs(hdfs_path: str, data: bytes) -> bool:
    """
    Upload data to HDFS using WebHDFS REST API (two-step process).
    
    Step 1: Create file (get redirect URL to DataNode)
    Step 2: Upload data to DataNode
    """
    # Step 1: Initiate upload - get DataNode URL
    create_url = f"{WEBHDFS_URL}{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
    
    try:
        # First request: Get redirect location (don't follow redirect automatically)
        response = requests.put(create_url, allow_redirects=False, timeout=30)
        
        if response.status_code == 307:
            # Get the DataNode URL from Location header
            datanode_url = response.headers.get('Location')
            if not datanode_url:
                logger.error(f" No redirect location for {hdfs_path}")
                return False
            
            # Step 2: Upload data to DataNode
            upload_response = requests.put(
                datanode_url,
                data=data,
                headers={'Content-Type': 'application/octet-stream'},
                timeout=60
            )
            
            if upload_response.status_code == 201:
                logger.info(f" Uploaded to HDFS: {hdfs_path}")
                return True
            else:
                logger.error(f" Upload failed for {hdfs_path}: HTTP {upload_response.status_code}")
                logger.error(f"   Response: {upload_response.text}")
                return False
        else:
            logger.error(f" CREATE failed for {hdfs_path}: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f" Error uploading {hdfs_path}: {str(e)}")
        return False

def verify_hdfs_file(hdfs_path: str) -> bool:
    """Verify file exists in HDFS."""
    url = f"{WEBHDFS_URL}{hdfs_path}?op=GETFILESTATUS&user.name=root"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            file_status = response.json().get('FileStatus', {})
            file_size = file_status.get('length', 0)
            logger.info(f" Verified {hdfs_path}: {file_size} bytes")
            return True
        return False
    except Exception as e:
        logger.error(f" Verification failed for {hdfs_path}: {str(e)}")
        return False



# Main Task Functions

def generate_and_upload_orders(**context):
    """Generate orders for all sample dates, save to CSV, and upload to HDFS."""
    logger.info(" Starting Orders Generation and Upload...")
    
    success_count = 0
    fail_count = 0
    
    for date_str in SAMPLE_DATES:
        exec_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        logger.info(f"\n Processing orders for: {date_str}")
        
        # Generate orders
        orders = generate_orders(exec_date, num_orders=1000)
        logger.info(f"   Generated {len(orders)} orders")
        
        # Step 1: Save to local CSV file
        csv_path = save_to_csv(orders, "orders", date_str)
        logger.info(f" Saved to local CSV: {csv_path}")
        
        # Step 2: Convert to Parquet
        parquet_data = create_parquet_buffer(orders, "orders")
        
        # Step 3: Upload to HDFS (Hive partition style folder)
        hdfs_path = f"/raw/orders/order_date={date_str}/data.parquet"
        if upload_to_hdfs(hdfs_path, parquet_data):
            success_count += 1
        else:
            fail_count += 1
    
    logger.info(f"\n Orders upload complete: {success_count} success, {fail_count} failed")
    
    if fail_count > 0:
        raise Exception(f"Failed to upload {fail_count} order files")
    
    return f"Uploaded orders for {success_count} dates"


def generate_and_upload_inventory(**context):
    """Generate inventory snapshots for all sample dates, save to CSV, and upload to HDFS."""
    logger.info("Starting Inventory Generation and Upload...")
    
    success_count = 0
    fail_count = 0
    
    for date_str in SAMPLE_DATES:
        exec_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        logger.info(f"\n Processing inventory for: {date_str}")
        
        # Generate inventory snapshots
        inventory = generate_inventory(exec_date)
        logger.info(f"   Generated {len(inventory)} inventory snapshots")
        
        # Step 1: Save to local CSV file
        csv_path = save_to_csv(inventory, "stock", date_str)
        logger.info(f"Saved to local CSV: {csv_path}")
        
        # Step 2: Convert to Parquet
        parquet_data = create_parquet_buffer(inventory, "inventory")
        
        # Step 3: Upload to HDFS (Hive partition style folder)
        hdfs_path = f"/raw/stock/snapshot_date={date_str}/data.parquet"
        if upload_to_hdfs(hdfs_path, parquet_data):
            success_count += 1
        else:
            fail_count += 1
    
    logger.info(f"\n Inventory upload complete: {success_count} success, {fail_count} failed")
    
    if fail_count > 0:
        raise Exception(f"Failed to upload {fail_count} inventory files")
    
    return f"Uploaded inventory for {success_count} dates"


def verify_all_uploads(**context):
    """Verify all files were uploaded successfully."""
    logger.info("Verifying all uploads...")
    
    all_files = []
    for date_str in SAMPLE_DATES:
        all_files.append(f"/raw/orders/order_date={date_str}/data.parquet")
        all_files.append(f"/raw/stock/snapshot_date={date_str}/data.parquet")
    
    missing = []
    for file_path in all_files:
        if not verify_hdfs_file(file_path):
            missing.append(file_path)
    
    if missing:
        raise Exception(f"Missing files: {missing}")
    
    logger.info(f" All {len(all_files)} files verified successfully!")
    return f"Verified {len(all_files)} files"


def print_summary(**context):
    """Print summary of uploaded data."""
    logger.info("\n" + "="*60)
    logger.info("UPLOAD SUMMARY")
    logger.info("="*60)
    
    for date_str in SAMPLE_DATES:
        logger.info(f"\n Date: {date_str}")
        
        # Check orders
        orders_path = f"/raw/orders/{date_str}/data.parquet"
        url = f"{WEBHDFS_URL}{orders_path}?op=GETFILESTATUS&user.name=root"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                size = response.json().get('FileStatus', {}).get('length', 0)
                logger.info(f"    Orders: {orders_path} ({size:,} bytes)")
        except:
            pass
        
        # Check inventory
        stock_path = f"/raw/stock/{date_str}/data.parquet"
        url = f"{WEBHDFS_URL}{stock_path}?op=GETFILESTATUS&user.name=root"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                size = response.json().get('FileStatus', {}).get('length', 0)
                logger.info(f"   Stock:  {stock_path} ({size:,} bytes)")
        except:
            pass
    
    logger.info("\n" + "="*60)
    logger.info(" Step 4 Complete! Data uploaded and partitions synced.")
    logger.info("   Trino tables are ready for queries.")
    logger.info("="*60)
    
    return "Summary complete"


def sync_hive_partitions(**context):
    """
    Sync Hive metastore with HDFS partitions using Trino.
    This automatically discovers new partition folders and registers them.
    """
    from trino.dbapi import connect
    
    logger.info("Syncing Hive partitions with HDFS...")
    
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="hive",
        schema="procurement_raw"
    )
    cursor = conn.cursor()
    
    tables_to_sync = [
        ('procurement_raw', 'orders'),
        ('procurement_raw', 'inventory')
    ]
    
    synced = []
    failed = []
    
    for schema, table in tables_to_sync:
        try:
            sync_query = f"CALL hive.system.sync_partition_metadata('{schema}', '{table}', 'ADD')"
            logger.info(f"   Syncing {schema}.{table}...")
            cursor.execute(sync_query)
            cursor.fetchall()  
            synced.append(f"{schema}.{table}")
            logger.info(f"    Synced {schema}.{table}")
        except Exception as e:
            logger.error(f"    Failed to sync {schema}.{table}: {str(e)}")
            failed.append(f"{schema}.{table}")
    
    cursor.close()
    conn.close()
    
    if failed:
        raise Exception(f"Failed to sync partitions for: {failed}")
    
    logger.info(f"Successfully synced partitions for: {synced}")
    return f"Synced partitions for {len(synced)} tables"



# DAG Definition
with DAG(
    'hdfs_upload_sample_data',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 12, 1),
    catchup=False,
    description='Step 4: Generate and upload sample procurement data to HDFS',
    tags=['setup', 'hdfs', 'data-generation', 'step-4']
) as dag:
    
    dag.doc_md = """
    ## HDFS Upload Sample Data DAG (Step 4)
    
    **Purpose:** Generate sample procurement data, save locally as CSV, upload to HDFS, and sync partitions.
    
    **When to run:** After Step 3 (HDFS directory initialization) is complete.
    
    **What it does:**
    1. Generates 1000 purchase orders per date
    2. Generates 5 inventory snapshots per date (one per product)
    3. **Saves data to local CSV files** 
    4. Converts data to Parquet format (Snappy compression)
    5. Uploads to HDFS via WebHDFS API
    6. **Automatically syncs Hive partitions with Trino**
    
    **Local CSV Files Created:**
    - `/opt/airflow/data/raw/orders/YYYY-MM-DD/orders_YYYY-MM-DD_HHMMSS.csv`
    - `/opt/airflow/data/raw/stock/YYYY-MM-DD/stock_YYYY-MM-DD_HHMMSS.csv`
    
    **HDFS Files Created:**
    - `/raw/orders/order_date=YYYY-MM-DD/data.parquet`
    - `/raw/stock/snapshot_date=YYYY-MM-DD/data.parquet`
    
    **Trigger:** Manual only (click "Trigger DAG" button)
    """
    
    task_upload_orders = PythonOperator(
        task_id='generate_upload_orders',
        python_callable=generate_and_upload_orders,
    )
    
    task_upload_inventory = PythonOperator(
        task_id='generate_upload_inventory',
        python_callable=generate_and_upload_inventory,
    )
    
    task_verify = PythonOperator(
        task_id='verify_uploads',
        python_callable=verify_all_uploads,
    )
    
    task_sync_partitions = PythonOperator(
        task_id='sync_hive_partitions',
        python_callable=sync_hive_partitions,
    )
    
    task_summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_summary,
    )
    
    # Orders and Inventory in parallel -> verify -> sync partitions -> summary
    [task_upload_orders, task_upload_inventory] >> task_verify >> task_sync_partitions >> task_summary
