"""
Exception Logger
Task 3.4: Detect and log data quality issues

Identifies and logs data quality exceptions including:
- Products without supplier mapping
- Abnormal demand spikes
- Missing inventory data
- Invalid product references
"""

import json
import os
import logging
from datetime import date
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class ExceptionLogger:
    """Data quality exception detection and logging."""
    
    def __init__(self, trino_client, output_base_path: str = "/opt/airflow/data/logs/exceptions"):
        """
        Initialize exception logger.
        
        Args:
            trino_client: TrinoClient instance for executing queries
            output_base_path: Base path for exception log files
        """
        self.trino = trino_client
        self.output_base_path = output_base_path
        self.exceptions: List[Dict[str, Any]] = []
    
    def check_missing_supplier_mapping(self, exec_date: str) -> List[Dict]:
        """Check for products in orders without supplier mapping."""
        query = f"""
            SELECT 
                o.product_id, 
                COUNT(*) as order_count,
                SUM(o.quantity) as total_quantity
            FROM hive.procurement_raw.orders o
            LEFT JOIN postgres.master_data.product_suppliers ps 
                ON o.product_id = ps.product_id
            WHERE o.order_date = DATE '{exec_date}'
                AND ps.product_id IS NULL
            GROUP BY o.product_id
        """
        
        try:
            results = self.trino.execute_query(query)
            for row in results:
                self.exceptions.append({
                    'type': 'MISSING_SUPPLIER_MAPPING',
                    'severity': 'HIGH',
                    'product_id': row.get('product_id'),
                    'order_count': row.get('order_count'),
                    'total_quantity': row.get('total_quantity'),
                    'message': f"Product {row.get('product_id')} has {row.get('order_count')} orders but no supplier mapping"
                })
            return results
        except Exception as e:
            logger.error(f"Failed to check supplier mapping: {e}")
            return []
    
    def check_demand_spikes(self, exec_date: str, threshold_multiplier: float = 3.0) -> List[Dict]:
        """Check for abnormal demand spikes (quantity > N times average)."""
        query = f"""
            WITH avg_demand AS (
                SELECT AVG(quantity) as avg_qty
                FROM hive.procurement_raw.orders
                WHERE order_date >= DATE '{exec_date}' - INTERVAL '7' DAY
            )
            SELECT 
                o.order_id,
                o.product_id, 
                o.quantity,
                ad.avg_qty,
                ROUND(CAST(o.quantity AS DOUBLE) / NULLIF(ad.avg_qty, 0), 2) as spike_ratio
            FROM hive.procurement_raw.orders o
            CROSS JOIN avg_demand ad
            WHERE o.order_date = DATE '{exec_date}'
                AND o.quantity > ad.avg_qty * {threshold_multiplier}
        """
        
        try:
            results = self.trino.execute_query(query)
            for row in results:
                self.exceptions.append({
                    'type': 'DEMAND_SPIKE',
                    'severity': 'MEDIUM',
                    'order_id': row.get('order_id'),
                    'product_id': row.get('product_id'),
                    'quantity': row.get('quantity'),
                    'average_quantity': row.get('avg_qty'),
                    'spike_ratio': row.get('spike_ratio'),
                    'message': f"Order {row.get('order_id')} has {row.get('spike_ratio')}x average demand"
                })
            return results
        except Exception as e:
            logger.error(f"Failed to check demand spikes: {e}")
            return []
    
    def check_missing_inventory(self, exec_date: str) -> List[Dict]:
        """Check for products with orders but no inventory snapshot."""
        query = f"""
            SELECT DISTINCT 
                o.product_id,
                COUNT(*) as order_count
            FROM hive.procurement_raw.orders o
            LEFT JOIN hive.procurement_raw.inventory i 
                ON o.product_id = i.product_id 
                AND i.snapshot_date = DATE '{exec_date}'
            WHERE o.order_date = DATE '{exec_date}'
                AND i.product_id IS NULL
            GROUP BY o.product_id
        """
        
        try:
            results = self.trino.execute_query(query)
            for row in results:
                self.exceptions.append({
                    'type': 'MISSING_INVENTORY',
                    'severity': 'HIGH',
                    'product_id': row.get('product_id'),
                    'order_count': row.get('order_count'),
                    'message': f"Product {row.get('product_id')} has orders but no inventory snapshot for {exec_date}"
                })
            return results
        except Exception as e:
            logger.error(f"Failed to check missing inventory: {e}")
            return []
    
    def check_invalid_products(self, exec_date: str) -> List[Dict]:
        """Check for orders referencing non-existent products."""
        query = f"""
            SELECT 
                o.product_id,
                COUNT(*) as order_count
            FROM hive.procurement_raw.orders o
            LEFT JOIN postgres.master_data.products p 
                ON o.product_id = p.product_id
            WHERE o.order_date = DATE '{exec_date}'
                AND p.product_id IS NULL
            GROUP BY o.product_id
        """
        
        try:
            results = self.trino.execute_query(query)
            for row in results:
                self.exceptions.append({
                    'type': 'INVALID_PRODUCT',
                    'severity': 'CRITICAL',
                    'product_id': row.get('product_id'),
                    'order_count': row.get('order_count'),
                    'message': f"Product {row.get('product_id')} in orders does not exist in master data"
                })
            return results
        except Exception as e:
            logger.error(f"Failed to check invalid products: {e}")
            return []
    
    def run_all_checks(self, exec_date: str) -> List[Dict]:
        """Run all data quality checks for a given date."""
        logger.info(f"🔍 Running data quality checks for {exec_date}...")
        
        self.exceptions = []  # Reset exceptions
        
        # Run all checks
        self.check_missing_supplier_mapping(exec_date)
        self.check_demand_spikes(exec_date)
        self.check_missing_inventory(exec_date)
        self.check_invalid_products(exec_date)
        
        logger.info(f"   Found {len(self.exceptions)} exceptions")
        return self.exceptions
    
    def save_exceptions(self, exec_date: str) -> Optional[str]:
        """Save exceptions to JSON file."""
        if not self.exceptions:
            logger.info("   ✅ No exceptions to log")
            return None
        
        # Create output directory
        output_dir = f"{self.output_base_path}/{exec_date}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Categorize by severity
        by_severity = {'CRITICAL': [], 'HIGH': [], 'MEDIUM': [], 'LOW': []}
        for exc in self.exceptions:
            severity = exc.get('severity', 'MEDIUM')
            by_severity[severity].append(exc)
        
        output = {
            'date': exec_date,
            'generated_at': date.today().isoformat(),
            'total_exceptions': len(self.exceptions),
            'by_severity': {k: len(v) for k, v in by_severity.items() if v},
            'exceptions': self.exceptions
        }
        
        filename = f"{output_dir}/exceptions.json"
        with open(filename, 'w') as f:
            json.dump(output, f, indent=2, default=str)
        
        logger.info(f"   📝 Saved exceptions to {filename}")
        
        # Log summary
        for severity, items in by_severity.items():
            if items:
                logger.warning(f"   ⚠️  {severity}: {len(items)} exceptions")
        
        return filename


def log_exceptions(exec_date: str, trino_client, output_base_path: str = "/opt/airflow/data/logs/exceptions") -> Dict:
    """
    Convenience function to detect and log all data quality exceptions.
    
    Args:
        exec_date: Execution date in YYYY-MM-DD format
        trino_client: TrinoClient instance
        output_base_path: Base path for exception logs
        
    Returns:
        Dictionary with exception summary
    """
    exception_logger = ExceptionLogger(trino_client, output_base_path)
    exceptions = exception_logger.run_all_checks(exec_date)
    output_file = exception_logger.save_exceptions(exec_date)
    
    return {
        'date': exec_date,
        'exception_count': len(exceptions),
        'output_file': output_file,
        'exceptions': exceptions
    }
