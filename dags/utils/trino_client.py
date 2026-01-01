# Trino query execution logic
"""
Trino Client utilities for the Procurement Data Pipeline.
Provides functions to execute queries and sync partitions.
"""

import subprocess
from typing import Optional, List


TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_CATALOG = "hive"
TRINO_SCHEMA = "procurement_raw"


def execute_trino_query(query: str, catalog: str = TRINO_CATALOG, schema: str = TRINO_SCHEMA) -> Optional[str]:
    """
    Execute a Trino query and return results.
    
    Args:
        query: SQL query to execute
        catalog: Trino catalog (default: hive)
        schema: Trino schema (default: procurement_raw)
    
    Returns:
        Query output as string, or None if failed
    """
    try:
        result = subprocess.run(
            [
                "trino",
                f"--server={TRINO_HOST}:{TRINO_PORT}",
                f"--catalog={catalog}",
                f"--schema={schema}",
                "--execute", query
            ],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ Trino query failed: {e.stderr}")
        return None


def sync_partition_metadata(table_name: str, mode: str = "FULL") -> bool:
    """
    Sync partition metadata for a Hive table.
    This must be called after uploading new partitions to HDFS.
    
    Args:
        table_name: Name of the table (e.g., 'orders' or 'inventory')
        mode: Sync mode - 'FULL' (add & drop), 'ADD' (only add new), 'DROP' (only remove missing)
    
    Returns:
        True if successful, False otherwise
    """
    query = f"CALL hive.system.sync_partition_metadata('{TRINO_SCHEMA}', '{table_name}', '{mode}')"
    
    try:
        result = subprocess.run(
            [
                "trino",
                f"--server={TRINO_HOST}:{TRINO_PORT}",
                "--execute", query
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✅ Synced partitions for {table_name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Partition sync failed for {table_name}: {e.stderr}")
        return False


def sync_all_partitions() -> bool:
    """Sync partitions for all tables in the procurement_raw schema."""
    success = True
    for table in ["orders", "inventory"]:
        if not sync_partition_metadata(table):
            success = False
    return success
