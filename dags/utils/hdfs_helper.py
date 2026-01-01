# HDFS CLI wrappers
"""
HDFS Helper utilities for the Procurement Data Pipeline.
Provides functions to upload partitioned data to HDFS.
"""

import subprocess
import os
from datetime import date


def upload_partition_to_hdfs(local_dir: str, hdfs_base_path: str, partition_col: str, partition_value: str) -> bool:
    """
    Upload a local partition directory to HDFS with proper Hive partition structure.
    
    Args:
        local_dir: Local directory containing parquet files
        hdfs_base_path: HDFS base path (e.g., /raw/orders)
        partition_col: Partition column name (e.g., order_date)
        partition_value: Partition value (e.g., 2025-12-31)
    
    Returns:
        True if successful, False otherwise
    """
    hdfs_partition_path = f"{hdfs_base_path}/{partition_col}={partition_value}"
    
    try:
        # Create HDFS directory if not exists
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", hdfs_partition_path],
            check=True,
            capture_output=True
        )
        
        # Upload files from local partition directory
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", f"{local_dir}/*", hdfs_partition_path],
            check=True,
            capture_output=True
        )
        
        print(f"✅ Uploaded {local_dir} to {hdfs_partition_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ HDFS upload failed: {e.stderr.decode()}")
        return False


def upload_orders(local_base_dir: str, exec_date: date) -> bool:
    """Upload orders partition for a specific date."""
    local_partition = os.path.join(local_base_dir, "orders", f"order_date={exec_date.isoformat()}")
    return upload_partition_to_hdfs(
        local_dir=local_partition,
        hdfs_base_path="/raw/orders",
        partition_col="order_date",
        partition_value=exec_date.isoformat()
    )


def upload_inventory(local_base_dir: str, exec_date: date) -> bool:
    """Upload inventory/stock partition for a specific date."""
    local_partition = os.path.join(local_base_dir, "stock", f"snapshot_date={exec_date.isoformat()}")
    return upload_partition_to_hdfs(
        local_dir=local_partition,
        hdfs_base_path="/raw/stock",
        partition_col="snapshot_date",
        partition_value=exec_date.isoformat()
    )
