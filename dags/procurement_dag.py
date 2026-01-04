"""
    Procurement Pipeline DAG
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.hdfs_helper import HDFSHelper 
from utils.trino_client import TrinoClient 

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def batch_ingest_to_hdfs(exec_date, **context):
    hdfs = HDFSHelper()
    hdfs.purge_directory(exec_date, 'orders')
    hdfs.upload_file(f'/opt/airflow/data/raw/orders_{exec_date}.parquet', f'/raw/orders/{exec_date}/')
    return f"Ingested data for {exec_date}"

def calculate_net_demand(exec_date, **context):
    trino = TrinoClient()
    results = trino.execute_net_demand(exec_date)
    context['ti'].xcom_push(key='net_demand_results', value=results)
    trino.close()

def extract_supplier_orders(exec_date, **context):
    trino = TrinoClient()
    results = context['ti'].xcom_pull(key='net_demand_results', task_ids='calculate_net_demand')
    trino.export_to_json(results, f'/opt/airflow/data/output/supplier_orders_{exec_date}.json')

with DAG('procurement_pipeline', default_args=default_args, schedule_interval='0 6 * * *', 
         start_date=datetime(2025, 12, 1), catchup=False) as dag:
    
    task_ingest = PythonOperator(task_id='batch_ingest_hdfs', python_callable=batch_ingest_to_hdfs, op_kwargs={'exec_date': '{{ds}}'})
    task_net_demand = PythonOperator(task_id='calculate_net_demand', python_callable=calculate_net_demand, op_kwargs={'exec_date': '{{ds}}'})
    task_extract = PythonOperator(task_id='extract_json_orders', python_callable=extract_supplier_orders, op_kwargs={'exec_date': '{{ds}}'})

    task_ingest >> task_net_demand >> task_extract
