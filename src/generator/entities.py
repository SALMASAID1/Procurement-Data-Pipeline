# =============================================================================
# entities.py - Data Models for Procurement Pipeline
# Purpose: Define the structure of transactional data (Orders & Inventory)
# =============================================================================

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class Order:
    """
    Represents a purchase order in the procurement system.
    
    This entity will be:
    - Generated daily by Faker
    - Stored as Parquet in HDFS
    - Queried via Hive/Trino
    """
    order_id: int
    product_id: int
    quantity: int
    order_date: date
    supplier_id: int
    status: str = "PENDING"
    
    def to_dict(self, include_partition_col: bool = False) -> dict:
        """Convert to dictionary for DataFrame creation.
        
        Args:
            include_partition_col: If False, excludes order_date (partition column)
                                   since it's encoded in the Hive directory path.
        """
        result = {
            'order_id': self.order_id,
            'product_id': self.product_id,
            'quantity': self.quantity,
            'supplier_id': self.supplier_id,
            'status': self.status
        }
        if include_partition_col:
            result['order_date'] = self.order_date.isoformat()
        return result


@dataclass
class InventorySnapshot:
    """
    Represents a daily snapshot of inventory levels.
    """
    product_id: int
    available_qty: int
    reserved_qty: int
    safety_stock: int
    snapshot_date: date
    warehouse_id: int = 1
    
    def to_dict(self, include_partition_col: bool = False) -> dict:
        """Convert to dictionary for DataFrame creation.
        
        Args:
            include_partition_col: If False, excludes snapshot_date (partition column)
                                   since it's encoded in the Hive directory path.
        """
        result = {
            'product_id': self.product_id,
            'available_qty': self.available_qty,
            'reserved_qty': self.reserved_qty,
            'safety_stock': self.safety_stock,
            'warehouse_id': self.warehouse_id
        }
        if include_partition_col:
            result['snapshot_date'] = self.snapshot_date.isoformat()
        return result
    
    @property
    def net_available(self) -> int:
        """Calculate actual available stock"""
        return self.available_qty - self.reserved_qty
    
    @property
    def needs_reorder(self) -> bool:
        """Check if stock is below safety threshold"""
        return self.net_available < self.safety_stock