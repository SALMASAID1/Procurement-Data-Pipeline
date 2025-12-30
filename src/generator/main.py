from faker import Faker
from datetime import date , timedelta
from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import os

from entities import Order , InventorySnapshot

fake = Faker()

# CONFIGURATION - Must match PostgreSQL master data

VALID_PRODUCT_IDS = [1 , 2 , 3 , 4 , 5]
VALID_SUPPLIER_IDS = [1 , 2 , 3]
ORDER_STATUS = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED"]
OUTPUT_DIR = "../../data/raw"

# DATA GENERATION FUNCTIONS

def generate_orders(exec_date : date , num_orders : int = 100 ) -> List[Order]:
    """Generate daily purchase orders with valid foreign keys."""
    orders = []

    for i in range(num_orders):
        order_id = int(f"{exec_date.strftime('%Y%m%d')}{i:05d}")

        order = Order(
            order_id= order_id,
            product_id=random.choice(VALID_PRODUCT_IDS),
            quantity=random.randint(1,100),
            order_date=exec_date,
            supplier_id= random.choice(VALID_SUPPLIER_IDS),
            status=random.choice(ORDER_STATUS)
        )
        orders.append(order)
    return orders

def generate_inventory (exec_date : date) -> List[InventorySnapshot]:
    """Generate daily inventory snapshots for all products."""
    snapshots = []

    for product_id in VALID_PRODUCT_IDS:
        snapshot = InventorySnapshot(
            product_id=product_id,
            available_qty=random.randint(50 , 500),
            reserved_qty=random.randint(0,50),
            safety_stock=random.randint(20,100),
            snapshot_date=exec_date,
            warehouse_id=1
        )
        snapshots.append(snapshot)
    return snapshots

# PARQUET EXPORT FUNCTIONS

def save_to_parquet (data: List , output_path : str , data_type: str) -> None:
    """Convert data objects to Parquet format for HDFS storage."""
    records = [item.to_dict() for item in data ]
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table,output_path,compression="snappy")
    print(f"✅ Saved {len(data)} {data_type} records to: {output_path}")

# MAIN EXECUTION

def main (exec_date :date = None , num_orders : int = 100):
    """Main entry point for data generation."""
    if exec_date is None :
        exec_date = date.today()
    
    print(f"\n{'='*60}")
    print(f"🚀 Procurement Data Generator")
    print(f"📅 Generating data for: {exec_date.isoformat()}")
    print(f"{'='*60}\n")

    os.makedirs(OUTPUT_DIR , exist_ok=True)

    # Generate Orders
    print("📦 Generating orders...")
    orders = generate_orders(exec_date , num_orders)
    orders_file = os.path.join(OUTPUT_DIR ,f"orders_{exec_date.isoformat()}.parquet" )
    save_to_parquet(orders , orders_file , "orders")

    # Generate Inventory Snapshots
    print("🛒 Generating inventory snapshots...")
    inventory = generate_inventory(exec_date)
    inventory_file = os.path.join(OUTPUT_DIR ,f"inventory_{exec_date.isoformat()}.parquet")
    save_to_parquet(inventory , inventory_file , "inventory")

    print(f"✅ Generation Complete!")
    print(f"   - Orders: {len(orders)} records")
    print(f"   - Inventory: {len(inventory)} records")

if __name__ == "__main__":
    main(num_orders=10000)

