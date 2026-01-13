"""
HDFS Setup DAG - Initialize HDFS directory structure via WebHDFS REST API
Run this DAG manually once after starting the infrastructure.

This uses WebHDFS HTTP API which is available on namenode:9870
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# WebHDFS endpoint (namenode container)
WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"

# Directories 
HDFS_DIRECTORIES = [
    "/raw/orders",
    "/raw/stock",
    "/processed/aggregated_orders",
    "/processed/net_demand",
    "/output/supplier_orders",
    "/logs/exceptions"
]


def create_hdfs_directory(path: str) -> bool:
    """Create a single HDFS directory using WebHDFS REST API."""
    url = f"{WEBHDFS_URL}{path}?op=MKDIRS&user.name=root"
    try:
        response = requests.put(url, allow_redirects=True, timeout=30)
        if response.status_code == 200:
            result = response.json()
            if result.get('boolean', False):
                logger.info(f" Created directory: {path}")
                return True
            else:
                logger.warning(f" Directory may already exist: {path}")
                return True 
        else:
            logger.error(f" Failed to create {path}: HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f" Error creating {path}: {str(e)}")
        return False


def set_hdfs_permissions(path: str, permission: str = "777") -> bool:
    """Set permissions on HDFS directory using WebHDFS REST API."""
    url = f"{WEBHDFS_URL}{path}?op=SETPERMISSION&permission={permission}&user.name=root"
    try:
        response = requests.put(url, allow_redirects=True, timeout=30)
        if response.status_code == 200:
            logger.info(f" Set permissions {permission} on: {path}")
            return True
        else:
            logger.warning(f" Could not set permissions on {path}: HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f" Error setting permissions on {path}: {str(e)}")
        return False


def initialize_hdfs_structure(**context):
    """Create all required HDFS directories."""
    logger.info("Starting HDFS directory initialization...")
    
    success_count = 0
    fail_count = 0
    
    for directory in HDFS_DIRECTORIES:
        if create_hdfs_directory(directory):
            success_count += 1
        else:
            fail_count += 1
    
    logger.info(f" Directory creation complete: {success_count} success, {fail_count} failed")
    
    if fail_count > 0:
        raise Exception(f"Failed to create {fail_count} directories")
    
    return f"Created {success_count} directories successfully"


def set_permissions(**context):
    """Set 777 permissions on root directories."""
    logger.info(" Setting HDFS permissions...")
    
    root_dirs = ["/raw", "/processed", "/output", "/logs"]
    
    for directory in root_dirs:
        set_hdfs_permissions(directory, "777")
    
    return "Permissions set successfully"


def verify_hdfs_structure(**context):
    """Verify all directories exist using WebHDFS."""
    logger.info("Verifying HDFS structure...")
    
    missing = []
    for directory in HDFS_DIRECTORIES:
        url = f"{WEBHDFS_URL}{directory}?op=GETFILESTATUS&user.name=root"
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                logger.info(f" Verified: {directory}")
            else:
                logger.error(f" Missing: {directory}")
                missing.append(directory)
        except Exception as e:
            logger.error(f" Error checking {directory}: {str(e)}")
            missing.append(directory)
    
    if missing:
        raise Exception(f"Missing directories: {missing}")
    
    logger.info(" All HDFS directories verified successfully!")
    return "All directories verified"


with DAG(
    'hdfs_initialize',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 12, 1),
    catchup=False,
    description='One-time HDFS directory initialization via WebHDFS API',
    tags=['setup', 'hdfs', 'initialization', 'one-time']
) as dag:
    
    dag.doc_md = """
    ## HDFS Initialize DAG
    
    **Purpose:** Create all required HDFS directories for the Procurement Pipeline.
    
    **When to run:** Once after starting the Docker infrastructure.
    
    **Directories Created:**
    - `/raw/orders/` - Daily orders (Parquet)
    - `/raw/stock/` - Inventory snapshots
    - `/processed/aggregated_orders/` - Summed daily demand
    - `/processed/net_demand/` - MRP calculations
    - `/output/supplier_orders/` - Final JSON files
    - `/logs/exceptions/` - Data quality alerts
    
    **Trigger:** Manual only (click "Trigger DAG" button)
    """
    
    task_create = PythonOperator(
        task_id='create_directories',
        python_callable=initialize_hdfs_structure,
    )
    
    task_permissions = PythonOperator(
        task_id='set_permissions',
        python_callable=set_permissions,
    )
    
    task_verify = PythonOperator(
        task_id='verify_structure',
        python_callable=verify_hdfs_structure,
    )
    
    task_create >> task_permissions >> task_verify
