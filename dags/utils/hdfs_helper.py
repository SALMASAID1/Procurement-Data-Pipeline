# HDFS CLI Wrappers for Airflow Integration
"""
HDFS Helper utilities for the Procurement Data Pipeline.
Provides functions to interact with HDFS from Airflow tasks.
"""

import subprocess
import logging
from datetime import date
from typing import Optional, Tuple, List

logger = logging.getLogger(__name__)


class HDFSHelper:
    """HDFS operations wrapper for Airflow tasks"""

    def __init__(self, base_path: str = "/raw", namenode: str = "namenode:8020"):
        """
        Initialize HDFS Helper.
        
        Args:
            base_path: Base HDFS path for raw data (default: /raw)
            namenode: Namenode address (default: namenode:8020 for Hadoop 3.x)
        """
        self.base_path = base_path
        self.namenode = namenode
        self.hdfs_cmd = "hdfs dfs"

    def _execute(self, cmd: str) -> Tuple[bool, str]:
        """Execute HDFS command and return status."""
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True
            )
            if result.returncode == 0:
                logger.info(f"Success: {cmd}")
                return True, result.stdout
            else:
                logger.error(f"Failed: {cmd}\n{result.stderr}")
                return False, result.stderr
        except Exception as e:
            logger.exception(f"Exception executing: {cmd}")
            return False, str(e)

    def upload_file(self, local_path: str, hdfs_path: str) -> bool:
        """Upload local file to HDFS with overwrite."""
        cmd = f"{self.hdfs_cmd} -put -f {local_path} {hdfs_path}"
        success, _ = self._execute(cmd)
        return success

    def purge_directory(self, exec_date: date, data_type: str) -> bool:
        """
        Idempotency: Remove existing data for a specific date.
        
        Args:
            exec_date: Execution date
            data_type: Type of data (orders or stock)
        """
        path = f"{self.base_path}/{data_type}/{exec_date.isoformat()}"
        cmd = f"{self.hdfs_cmd} -rm -r -f {path}"
        success, _ = self._execute(cmd)
        return success

    def create_directory(self, path: str) -> bool:
        """Create HDFS directory if not exists."""
        cmd = f"{self.hdfs_cmd} -mkdir -p {path}"
        success, _ = self._execute(cmd)
        return success

    def list_directory(self, path: str) -> Optional[List[str]]:
        """List contents of HDFS directory."""
        cmd = f"{self.hdfs_cmd} -ls {path}"
        success, output = self._execute(cmd)
        if success:
            return output.strip().split('\n')
        return None

    def file_exists(self, path: str) -> bool:
        """Check if file exists in HDFS."""
        cmd = f"{self.hdfs_cmd} -test -e {path}"
        success, _ = self._execute(cmd)
        return success

    def get_file_size(self, path: str) -> Optional[int]:
        """Get file size in bytes."""
        cmd = f"{self.hdfs_cmd} -du -s {path}"
        success, output = self._execute(cmd)
        if success and output:
            return int(output.split()[0])
        return None

    def upload_partitioned_data(
        self, 
        local_file: str, 
        table: str, 
        partition_col: str, 
        partition_val: str
    ) -> bool:
        """
        Upload file to partitioned HDFS location (Hive-compatible).
        
        Args:
            local_file: Path to local parquet file
            table: Table name (orders or stock)
            partition_col: Partition column name (order_date or snapshot_date)
            partition_val: Partition value (e.g., 2026-01-04)
        
        Returns:
            True if successful, False otherwise
        """
        hdfs_path = f"{self.base_path}/{table}/{partition_col}={partition_val}/"
        
        # Create partition directory
        if not self.create_directory(hdfs_path):
            logger.error(f"Failed to create directory: {hdfs_path}")
            return False
        
        # Upload file
        return self.upload_file(local_file, f"{hdfs_path}data.parquet")

    def upload_orders(self, local_file: str, exec_date: date) -> bool:
        """Upload orders parquet file for a specific date."""
        return self.upload_partitioned_data(
            local_file=local_file,
            table="orders",
            partition_col="order_date",
            partition_val=exec_date.isoformat()
        )

    def upload_inventory(self, local_file: str, exec_date: date) -> bool:
        """Upload inventory/stock parquet file for a specific date."""
        return self.upload_partitioned_data(
            local_file=local_file,
            table="stock",
            partition_col="snapshot_date",
            partition_val=exec_date.isoformat()
        )

    def purge_and_upload_orders(self, local_file: str, exec_date: date) -> bool:
        """Idempotent upload: purge existing data then upload new orders."""
        self.purge_directory(exec_date, "orders")
        return self.upload_orders(local_file, exec_date)

    def purge_and_upload_inventory(self, local_file: str, exec_date: date) -> bool:
        """Idempotent upload: purge existing data then upload new inventory."""
        self.purge_directory(exec_date, "stock")
        return self.upload_inventory(local_file, exec_date)

    def validate_partition_exists(self, table: str, partition_col: str, partition_val: str) -> bool:
        """
        Validate that a partition directory exists and contains data.
        
        Args:
            table: Table name (orders or stock)
            partition_col: Partition column name
            partition_val: Partition value
        
        Returns:
            True if partition exists and has files
        """
        path = f"{self.base_path}/{table}/{partition_col}={partition_val}"
        return self.file_exists(path)

    def get_partition_file_count(self, table: str, partition_col: str, partition_val: str) -> int:
        """Count files in a partition directory."""
        path = f"{self.base_path}/{table}/{partition_col}={partition_val}"
        files = self.list_directory(path)
        if files:
            # Filter out the header line from ls output
            return len([f for f in files if '.parquet' in f])
        return 0

    def validate_upload_success(self, table: str, exec_date: date) -> bool:
        """
        Validate that data was successfully uploaded for a date.
        
        Args:
            table: Table name (orders or stock)
            exec_date: Execution date
        
        Returns:
            True if partition exists with parquet files
        """
        partition_col = "order_date" if table == "orders" else "snapshot_date"
        return self.get_partition_file_count(table, partition_col, exec_date.isoformat()) > 0 