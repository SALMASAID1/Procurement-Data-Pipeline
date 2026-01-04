<<<<<<< HEAD
"""
    Trino Query Execution Client
    
    Provides federated query capabilities across:
    - hive.procurement_raw (HDFS/Parquet data)
    - postgres.master_data (Product & Supplier master data)
"""

from trino.dbapi import connect
import json
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class TrinoClient:
    """Trino query execution client for Airflow integration"""
    
    def __init__(self, host: str = 'trino', port: int = 8080, user: str = 'admin', 
                 catalog: str = 'hive', schema: str = 'procurement_raw'):
        self.host = host
        self.port = port
        self.user = user
        self.default_catalog = catalog
        self.default_schema = schema
        self._conn = None
        
    @property
    def connection(self):
        """Lazy connection initialization"""
        if self._conn is None:
            self._conn = connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.default_catalog,
                schema=self.default_schema
            )
        return self._conn
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as list of dictionaries"""
        cursor = self.connection.cursor()
        try:
            logger.info(f"Executing query: {query[:100]}...")
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.info(f"Query returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def execute(self, query: str) -> List[Dict[str, Any]]:
        """Alias for execute_query - for compatibility"""
        return self.execute_query(query)
            
    def execute_net_demand(self, exec_date: str) -> List[Dict]:
        """
        Execute the Net Demand (Golden Query) for a specific date.
        
        Args:
            exec_date: Execution date in YYYY-MM-DD format
            
        Returns:
            List of dictionaries with net demand calculations per product/supplier
        """
        sql_path = '/opt/airflow/sql/net_demand.sql'
        try:
            with open(sql_path, 'r') as f:
                query = f.read().replace('${EXEC_DATE}', exec_date)
            logger.info(f"Executing net demand calculation for date: {exec_date}")
            return self.execute_query(query)
        except FileNotFoundError:
            logger.error(f"SQL file not found: {sql_path}")
            raise
    
    def export_to_json(self, data: List[Dict], output_path: str, 
                       group_by_supplier: bool = True) -> str:
        """
        Export query results to JSON file.
        
        Args:
            data: Query results
            output_path: Path for output JSON file
            group_by_supplier: If True, groups orders by supplier
            
        Returns:
            Path to the created JSON file
        """
        if group_by_supplier:
            grouped = {}
            for row in data:
                supplier = row.get('supplier_name', 'Unknown')
                if supplier not in grouped:
                    grouped[supplier] = {
                        'supplier_id': row.get('supplier_id'),
                        'supplier_name': supplier,
                        'orders': []
                    }
                grouped[supplier]['orders'].append({
                    'product_id': row.get('product_id'),
                    'product_name': row.get('product_name'),
                    'net_demand': row.get('net_demand'),
                    'calculation_date': str(row.get('calculation_date'))
                })
            output_data = list(grouped.values())
        else:
            output_data = data
            
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
            
        logger.info(f"Exported {len(output_data)} records to {output_path}")
        return output_path
    
    def close(self):
        """Close the database connection"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("Trino connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection is closed"""
        self.close()
        return False
=======
>>>>>>> origin/salma
