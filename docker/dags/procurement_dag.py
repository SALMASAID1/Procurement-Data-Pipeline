"""
Procurement Pipeline DAG - Full Implementation
Runs daily at 22:00 

This DAG implements the complete procurement data pipeline:
1. Sync Hive partitions with HDFS
2. Aggregate orders per product
3. Calculate net demand (MRP formula)
4. Export supplier orders as JSON
5. Run data quality checks and log exceptions
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import logging
from airflow.utils.dates import days_ago

from utils.trino_client import TrinoClient

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}



# Task Functions
def task_sync_partitions(**context):
    """Sync Hive partitions with HDFS before processing."""
    logger.info("Syncing Hive partitions...")
    trino = TrinoClient()
    try:
        trino.sync_partitions('procurement_raw', ['orders', 'inventory'])
        logger.info("Partitions synced successfully")
    finally:
        trino.close()
    return "Partitions synced"


def task_aggregate_orders(exec_date: str, **context):
    """Task 3.1: Aggregate orders per product."""
    logger.info(f"Aggregating orders for {exec_date}...")
    trino = TrinoClient()
    try:
        trino.execute_aggregation(exec_date)
        verify_query = f"""
            SELECT COUNT(*) as product_count, SUM(total_quantity) as total_orders
            FROM hive.procurement_raw.aggregated_orders
            WHERE order_date = DATE '{exec_date}'
        """
        result = trino.execute_query(verify_query)
        if result:
            stats = result[0]
            logger.info(f"Aggregated {stats.get('product_count')} products")
            return stats
    finally:
        trino.close()
    return "Aggregation complete"


def task_calculate_net_demand(exec_date: str, **context):
    """Task 3.2: Calculate net demand using MRP formula."""
    logger.info(f"Calculating net demand for {exec_date}...")
    trino = TrinoClient()
    try:
        results = trino.execute_net_demand(exec_date)
        # Convert Decimal to float for XCom serialization
        serializable_results = []
        for r in results:
            row = dict(r)
            for key, value in row.items():
                if hasattr(value, '__float__'):
                    row[key] = float(value)
            serializable_results.append(row)
        
        context['ti'].xcom_push(key='net_demand_results', value=serializable_results)
        total_demand = sum(r.get('net_demand', 0) for r in serializable_results)
        total_cost = sum(float(r.get('estimated_cost', 0) or 0) for r in serializable_results)
        logger.info(f"Net demand: {len(serializable_results)} products, {total_demand} units, ${total_cost:,.2f}")
        return {'product_count': len(serializable_results), 'total_demand': total_demand, 'estimated_cost': total_cost}
    finally:
        trino.close()


def task_export_supplier_json(data_date: str, order_date: str, **context):
    """Task 3.3: Export supplier orders as JSON files.
    
    Args:
        data_date: Date of the source data (for calculation reference)
        order_date: Date for supplier orders (next business day)
    """
    logger.info(f"Exporting supplier orders for order date {order_date} (based on data from {data_date})...")
    results = context['ti'].xcom_pull(key='net_demand_results', task_ids='calculate_net_demand')
    
    if not results:
        logger.warning("No net demand results found")
        return "No results to export"
    
    # Group by supplier
    supplier_orders = {}
    for row in results:
        supplier_id = row.get('supplier_id')
        # Convert float to int for IDs
        if isinstance(supplier_id, float):
            supplier_id = int(supplier_id)
        if supplier_id not in supplier_orders:
            supplier_orders[supplier_id] = {
                'supplier_id': supplier_id,
                'supplier_name': row.get('supplier_name'),
                'order_date': order_date,  # Next business day
                'data_date': data_date,     # Source data date
                'generated_at': datetime.now().isoformat(),
                'items': [],
                'total_estimated_cost': 0.0
            }
        
        net_demand = row.get('net_demand', 0)
        # Convert float to int for quantity
        if isinstance(net_demand, float):
            net_demand = int(net_demand)
        if net_demand > 0:
            product_id = row.get('product_id')
            if isinstance(product_id, float):
                product_id = int(product_id)
            supplier_orders[supplier_id]['items'].append({
                'product_id': product_id,
                'product_name': row.get('product_name'),
                'quantity': net_demand,
                'unit_cost': round(float(row.get('unit_cost', 0)), 2),
                'total_cost': round(float(row.get('estimated_cost', 0)), 2)
            })
            supplier_orders[supplier_id]['total_estimated_cost'] += float(row.get('estimated_cost', 0))
    
    # Write JSON files - folder named by ORDER date (next business day)
    output_dir = f"/opt/airflow/data/output/supplier_orders/{order_date}"
    os.makedirs(output_dir, exist_ok=True)
    
    created_files = []
    for supplier_id, order in supplier_orders.items():
        if order['items']:
            order['total_estimated_cost'] = round(order['total_estimated_cost'], 2)
            filename = f"{output_dir}/supplier_{supplier_id}.json"
            with open(filename, 'w') as f:
                json.dump(order, f, indent=2, default=str)
            created_files.append(filename)
            logger.info(f" Created {filename}")
    
    logger.info(f" Exported {len(created_files)} supplier order files")
    return f"Created {len(created_files)} files"


def task_quality_checks(exec_date: str, **context):
    """Task 3.4: Run data quality checks and log exceptions."""
    logger.info(f"🔍 Running quality checks for {exec_date}...")
    trino = TrinoClient()
    exceptions = []
    
    try:
        # Check: Products without supplier mapping
        orphan_query = f"""
            SELECT o.product_id, COUNT(*) as order_count
            FROM hive.procurement_raw.orders o
            LEFT JOIN postgres.master_data.product_suppliers ps ON o.product_id = ps.product_id
            WHERE o.order_date = DATE '{exec_date}' AND ps.product_id IS NULL
            GROUP BY o.product_id
        """
        for row in trino.execute_query(orphan_query):
            exceptions.append({'type': 'MISSING_SUPPLIER_MAPPING', 'severity': 'HIGH', 'product_id': row.get('product_id')})
        
        # Check: Missing inventory
        missing_inv_query = f"""
            SELECT DISTINCT o.product_id
            FROM hive.procurement_raw.orders o
            LEFT JOIN hive.procurement_raw.inventory i ON o.product_id = i.product_id AND i.snapshot_date = DATE '{exec_date}'
            WHERE o.order_date = DATE '{exec_date}' AND i.product_id IS NULL
        """
        for row in trino.execute_query(missing_inv_query):
            exceptions.append({'type': 'MISSING_INVENTORY', 'severity': 'MEDIUM', 'product_id': row.get('product_id')})
    finally:
        trino.close()
    
    # Log exceptions
    if exceptions:
        output_dir = f"/opt/airflow/data/logs/exceptions/{exec_date}"
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{output_dir}/exceptions.json"
        with open(filename, 'w') as f:
            json.dump({'date': exec_date, 'exception_count': len(exceptions), 'exceptions': exceptions}, f, indent=2)
        logger.warning(f"Found {len(exceptions)} exceptions")
    else:
        logger.info(" No data quality exceptions")
    
    return f"Quality check: {len(exceptions)} exceptions"


def task_copy_to_processed(exec_date: str, **context):
    """Task: Copy processed data from Hive warehouse to /processed/ and upload JSON to /output/ in HDFS via WebHDFS."""
    import requests
    logger.info(f" Copying processed data to HDFS for {exec_date}...")
    
    webhdfs_base = "http://namenode:9870/webhdfs/v1"
    
    def webhdfs_list(path: str) -> list:
        """List files in HDFS path via WebHDFS."""
        url = f"{webhdfs_base}{path}?op=LISTSTATUS"
        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json().get('FileStatuses', {}).get('FileStatus', [])
        return []
    
    def webhdfs_mkdir(path: str) -> bool:
        """Create directory in HDFS via WebHDFS."""
        url = f"{webhdfs_base}{path}?op=MKDIRS&user.name=root"
        resp = requests.put(url)
        return resp.status_code == 200
    
    def webhdfs_copy(src: str, dst: str) -> bool:
        """Copy file within HDFS - read then write via WebHDFS."""
        read_url = f"{webhdfs_base}{src}?op=OPEN"
        read_resp = requests.get(read_url, allow_redirects=True)
        if read_resp.status_code != 200:
            return False
        create_url = f"{webhdfs_base}{dst}?op=CREATE&overwrite=true&user.name=root"
        create_resp = requests.put(create_url, allow_redirects=False)
        if create_resp.status_code == 307:
            location = create_resp.headers.get('Location')
            write_resp = requests.put(location, data=read_resp.content)
            return write_resp.status_code == 201
        return False
    
    def webhdfs_upload(local_path: str, hdfs_path: str) -> bool:
        """Upload local file to HDFS via WebHDFS."""
        try:
            with open(local_path, 'rb') as f:
                content = f.read()
            create_url = f"{webhdfs_base}{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
            create_resp = requests.put(create_url, allow_redirects=False)
            if create_resp.status_code == 307:
                location = create_resp.headers.get('Location')
                write_resp = requests.put(location, data=content)
                return write_resp.status_code == 201
        except Exception as e:
            logger.warning(f"Upload failed: {e}")
        return False
    
    # 1. Copy Parquet files from Hive warehouse to /processed/
    copies = [
        {
            'source': '/user/hive/warehouse/procurement_raw/aggregated_orders',
            'dest': f'/processed/aggregated_orders/{exec_date}'
        },
        {
            'source': '/user/hive/warehouse/procurement_raw/net_demand', 
            'dest': f'/processed/net_demand/{exec_date}'
        }
    ]
    
    copied_files = 0
    for copy_spec in copies:
        try:
            webhdfs_mkdir(copy_spec['dest'])
            files = webhdfs_list(copy_spec['source'])
            for f in files:
                filename = f.get('pathSuffix', '')
                if filename and not filename.startswith('.') and f.get('type') == 'FILE':
                    src_path = f"{copy_spec['source']}/{filename}"
                    dst_path = f"{copy_spec['dest']}/{filename}"
                    if webhdfs_copy(src_path, dst_path):
                        copied_files += 1
                        logger.info(f"    Copied {filename}")
        except Exception as e:
            logger.warning(f"   Could not copy to {copy_spec['dest']}: {e}")
    
    # 2. Upload supplier JSON files to HDFS /output/supplier_orders/
    json_dir = f"/opt/airflow/data/output/supplier_orders/{exec_date}"
    hdfs_json_dir = f"/output/supplier_orders/{exec_date}"
    
    uploaded_json = 0
    if os.path.exists(json_dir):
        webhdfs_mkdir(hdfs_json_dir)
        for filename in os.listdir(json_dir):
            if filename.endswith('.json'):
                local_path = f"{json_dir}/{filename}"
                hdfs_path = f"{hdfs_json_dir}/{filename}"
                if webhdfs_upload(local_path, hdfs_path):
                    uploaded_json += 1
                    logger.info(f"    Uploaded {filename} to HDFS")
                else:
                    logger.warning(f"   Failed to upload {filename}")
    
    # 3. Upload exception logs to HDFS /logs/exceptions/
    logs_dir = f"/opt/airflow/data/logs/exceptions/{exec_date}"
    hdfs_logs_dir = f"/logs/exceptions/{exec_date}"
    
    uploaded_logs = 0
    if os.path.exists(logs_dir):
        webhdfs_mkdir(hdfs_logs_dir)
        for filename in os.listdir(logs_dir):
            if filename.endswith('.json'):
                local_path = f"{logs_dir}/{filename}"
                hdfs_path = f"{hdfs_logs_dir}/{filename}"
                if webhdfs_upload(local_path, hdfs_path):
                    uploaded_logs += 1
                    logger.info(f"    Uploaded {filename} to HDFS logs")
    
    logger.info(f" Summary: {copied_files} Parquet files, {uploaded_json} JSON files, {uploaded_logs} log files")
    return f"Copied {copied_files} Parquet, uploaded {uploaded_json} JSON, {uploaded_logs} logs"



# DAG Definition
with DAG(
    'procurement_pipeline',
    default_args=default_args,
    schedule_interval='0 22 * * *',  # 22:00 daily 
    start_date=days_ago(0),  # Start from today
    catchup=False,
    description='Daily procurement pipeline - runs at 22:00 ',
    tags=['procurement', 'etl', 'daily']
) as dag:
    
    dag.doc_md = """
    ## Procurement Pipeline DAG
    
    **Schedule:** Daily at 22:00 
    
    ### Pipeline Steps:
    1. **sync_partitions** - Sync Hive metastore with HDFS
    2. **aggregate_orders** - Sum orders per product  
    3. **calculate_net_demand** - MRP formula calculation
    4. **export_supplier_json** - Generate supplier order files
    5. **quality_checks** - Data quality validation
    6. **copy_to_processed** - Copy data to /processed/ HDFS directory
    
    ### Net Demand Formula:
    `MAX(0, Orders + Safety Stock - (Available - Reserved))`
    """
    
    t1_sync = PythonOperator(
        task_id='sync_partitions',
        python_callable=task_sync_partitions,
    )
    
    t2_aggregate = PythonOperator(
        task_id='aggregate_orders',
        python_callable=task_aggregate_orders,
        op_kwargs={'exec_date': '{{ macros.ds_format(macros.ds_add(data_interval_end | ds, 1), "%Y-%m-%d", "%Y-%m-%d") }}'}
    )
    
    t3_net_demand = PythonOperator(
        task_id='calculate_net_demand',
        python_callable=task_calculate_net_demand,
        op_kwargs={'exec_date': '{{ macros.ds_format(macros.ds_add(data_interval_end | ds, 1), "%Y-%m-%d", "%Y-%m-%d") }}'}
    )
    
    t4_export = PythonOperator(
        task_id='export_supplier_json',
        python_callable=task_export_supplier_json,
        op_kwargs={
            'data_date': '{{ macros.ds_format(macros.ds_add(data_interval_end | ds, 1), "%Y-%m-%d", "%Y-%m-%d") }}',
            'order_date': '{{ macros.ds_format(macros.ds_add(data_interval_end | ds, 2), "%Y-%m-%d", "%Y-%m-%d") }}'
        }
    )
    
    t5_quality = PythonOperator(
        task_id='quality_checks',
        python_callable=task_quality_checks,
        op_kwargs={'exec_date': '{{ macros.ds_format(macros.ds_add(data_interval_end | ds, 1), "%Y-%m-%d", "%Y-%m-%d") }}'}
    )
    
    t6_copy = PythonOperator(
        task_id='copy_to_processed',
        python_callable=task_copy_to_processed,
        op_kwargs={'exec_date': '{{ macros.ds_format(macros.ds_add(data_interval_end | ds, 1), "%Y-%m-%d", "%Y-%m-%d") }}'}
    )
    
    # Task dependencies: Sync -> Aggregate -> Net Demand -> [Export, Quality] -> Copy
    t1_sync >> t2_aggregate >> t3_net_demand >> [t4_export, t5_quality] >> t6_copy
