"""
Supplier Order JSON Export
Task 3.3: Generate JSON files for each supplier

Exports net demand results as supplier-specific JSON files
for procurement order generation.
"""

import json
import os
import logging
from datetime import date
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def export_supplier_orders(net_demand_results: List[Dict[str, Any]], 
                           exec_date: str,
                           output_base_path: str = "/opt/airflow/data/output/supplier_orders") -> List[str]:
    """
    Group net demand by supplier and export as individual JSON files.
    
    Args:
        net_demand_results: List of dictionaries from net demand query
        exec_date: Execution date in YYYY-MM-DD format
        output_base_path: Base path for output files
        
    Returns:
        List of created file paths
    """
    logger.info(f"📦 Exporting supplier orders for {exec_date}...")
    
    # Create output directory for this date
    output_dir = f"{output_base_path}/{exec_date}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Group by supplier_id
    supplier_orders: Dict[int, Dict] = {}
    
    for row in net_demand_results:
        supplier_id = row.get('supplier_id')
        
        if supplier_id not in supplier_orders:
            supplier_orders[supplier_id] = {
                'supplier_id': supplier_id,
                'supplier_name': row.get('supplier_name'),
                'order_date': exec_date,
                'generated_at': date.today().isoformat(),
                'items': [],
                'total_items': 0,
                'total_estimated_cost': 0.0
            }
        
        net_demand = row.get('net_demand', 0)
        if net_demand > 0:
            unit_cost = float(row.get('unit_cost', 0))
            total_cost = net_demand * unit_cost
            
            supplier_orders[supplier_id]['items'].append({
                'product_id': row.get('product_id'),
                'product_name': row.get('product_name'),
                'product_code': row.get('product_code'),
                'quantity': net_demand,
                'unit_cost': unit_cost,
                'total_cost': round(total_cost, 2),
                'lead_time_days': row.get('lead_time_days')
            })
            
            supplier_orders[supplier_id]['total_items'] += 1
            supplier_orders[supplier_id]['total_estimated_cost'] += total_cost
    
    # Write JSON files for each supplier
    created_files = []
    
    for supplier_id, order in supplier_orders.items():
        # Round total cost
        order['total_estimated_cost'] = round(order['total_estimated_cost'], 2)
        
        # Only create file if there are items to order
        if order['items']:
            filename = f"{output_dir}/supplier_{supplier_id}.json"
            
            with open(filename, 'w') as f:
                json.dump(order, f, indent=2, default=str)
            
            created_files.append(filename)
            logger.info(f"   ✅ Created {filename} ({order['total_items']} items, ${order['total_estimated_cost']:.2f})")
    
    # Create summary file
    summary = {
        'order_date': exec_date,
        'generated_at': date.today().isoformat(),
        'supplier_count': len(created_files),
        'suppliers': [
            {
                'supplier_id': order['supplier_id'],
                'supplier_name': order['supplier_name'],
                'item_count': order['total_items'],
                'estimated_cost': order['total_estimated_cost']
            }
            for order in supplier_orders.values() if order['items']
        ]
    }
    
    summary_file = f"{output_dir}/summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    created_files.append(summary_file)
    
    logger.info(f"📋 Created {len(created_files)} files in {output_dir}")
    return created_files


def export_single_json(net_demand_results: List[Dict[str, Any]], 
                       exec_date: str,
                       output_path: str) -> str:
    """
    Export all supplier orders to a single JSON file.
    
    Args:
        net_demand_results: List of dictionaries from net demand query
        exec_date: Execution date
        output_path: Full path for output file
        
    Returns:
        Path to created file
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    output = {
        'order_date': exec_date,
        'generated_at': date.today().isoformat(),
        'orders': net_demand_results
    }
    
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    
    logger.info(f"✅ Exported {len(net_demand_results)} records to {output_path}")
    return output_path
