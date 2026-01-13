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
        partition_val: str,
        base_dir: str = None
    ) -> bool:
        """
        Upload file to date-partitioned HDFS location.
        Uses simple YYYY-MM-DD folder structure (not Hive-style partition=value).
        
        Args:
            local_file: Path to local parquet file
            table: Table name (orders or stock)
            partition_val: Date value (e.g., 2026-01-04)
            base_dir: Base directory (default: self.base_path which is /raw)
        
        Returns:
            True if successful, False otherwise
        """
        base = base_dir or self.base_path
        hdfs_path = f"{base}/{table}/{partition_val}/"
        
        # Create partition directory
        if not self.create_directory(hdfs_path):
            logger.error(f"Failed to create directory: {hdfs_path}")
            return False
        
        # Upload file
        return self.upload_file(local_file, f"{hdfs_path}data.parquet")

    def upload_orders(self, local_file: str, exec_date: date) -> bool:
        """Upload orders parquet file to /raw/orders/YYYY-MM-DD/"""
        return self.upload_partitioned_data(
            local_file=local_file,
            table="orders",
            partition_val=exec_date.isoformat()
        )

    def upload_inventory(self, local_file: str, exec_date: date) -> bool:
        """Upload inventory/stock parquet file to /raw/stock/YYYY-MM-DD/"""
        return self.upload_partitioned_data(
            local_file=local_file,
            table="stock",
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

    def validate_partition_exists(self, table: str, partition_val: str, base_dir: str = None) -> bool:
        """
        Validate that a partition directory exists and contains data.
        
        Args:
            table: Table name (orders, stock, aggregated_orders, net_demand)
            partition_val: Date value (YYYY-MM-DD)
            base_dir: Base directory (default: /raw)
        
        Returns:
            True if partition exists and has files
        """
        base = base_dir or self.base_path
        path = f"{base}/{table}/{partition_val}"
        return self.file_exists(path)

    def get_partition_file_count(self, table: str, partition_val: str, base_dir: str = None) -> int:
        """Count files in a partition directory."""
        base = base_dir or self.base_path
        path = f"{base}/{table}/{partition_val}"
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
        return self.get_partition_file_count(table, exec_date.isoformat()) > 0

    # =========================================================
    # NEW METHODS FOR PROCESSED & OUTPUT DIRECTORIES
    # =========================================================
    
    def upload_aggregated_orders(self, local_file: str, exec_date: date) -> bool:
        """
        Upload aggregated orders to /processed/aggregated_orders/YYYY-MM-DD/
        Purpose: Store summed daily demand per product.
        """
        return self.upload_partitioned_data(
            local_file=local_file,
            table="aggregated_orders",
            partition_val=exec_date.isoformat(),
            base_dir="/processed"
        )

    def upload_net_demand(self, local_file: str, exec_date: date) -> bool:
        """
        Upload net demand results to /processed/net_demand/YYYY-MM-DD/
        Purpose: Store pre-export MRP calculations.
        """
        return self.upload_partitioned_data(
            local_file=local_file,
            table="net_demand",
            partition_val=exec_date.isoformat(),
            base_dir="/processed"
        )

    def upload_supplier_orders(self, local_file: str, exec_date: date) -> bool:
        """
        Upload final JSON to /output/supplier_orders/YYYY-MM-DD/
        Purpose: Final supplier delivery files.
        """
        hdfs_path = f"/output/supplier_orders/{exec_date.isoformat()}/"
        self.create_directory(hdfs_path)
        return self.upload_file(local_file, f"{hdfs_path}supplier_orders.json")

    def log_exception(self, local_file: str, exec_date: date) -> bool:
        """
        Upload exception log to /logs/exceptions/YYYY-MM-DD/
        Purpose: Data quality alerts and missing mappings.
        """
        hdfs_path = f"/logs/exceptions/{exec_date.isoformat()}/"
        self.create_directory(hdfs_path)
        return self.upload_file(local_file, f"{hdfs_path}exceptions.json")

    def initialize_hdfs_structure(self) -> bool:
        """
        Create all required HDFS directories on startup.
        Called once during pipeline initialization.
        
        Directory Structure:
        /raw/orders/           - Landing zone for daily orders
        /raw/stock/            - Landing zone for inventory snapshots
        /processed/aggregated_orders/  - Summed daily demand
        /processed/net_demand/         - Pre-export calculations
        /output/supplier_orders/       - Final JSON deliverables
        /logs/exceptions/              - Data quality alerts
        """
        directories = [
            "/raw/orders",
            "/raw/stock",
            "/processed/aggregated_orders",
            "/processed/net_demand",
            "/output/supplier_orders",
            "/logs/exceptions"
        ]
        
        success = True
        for dir_path in directories:
            if not self.create_directory(dir_path):
                logger.error(f"Failed to create: {dir_path}")
                success = False
            else:
                logger.info(f"Created directory: {dir_path}")
        
        return success