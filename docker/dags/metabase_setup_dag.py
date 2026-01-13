"""
Metabase Dashboard Setup DAG
Automatically configures Metabase with Trino connection and creates procurement dashboard

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time
import logging
import json

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Metabase Configuration
METABASE_URL = "http://metabase:3000"
METABASE_EMAIL = "admin@procurement.local"
METABASE_PASSWORD = "Procurement2026!"
METABASE_FIRST_NAME = "Admin"
METABASE_LAST_NAME = "User"

# Trino Connection Details
TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_CATALOG = "hive"
TRINO_SCHEMA = "procurement_raw"


def wait_for_metabase(**context):
    """Wait for Metabase to be ready."""
    logger.info("Waiting for Metabase to be ready...")
    max_attempts = 30
    
    for i in range(max_attempts):
        try:
            response = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if response.status_code == 200:
                logger.info(" Metabase is ready!")
                return True
        except Exception as e:
            logger.info(f"   Attempt {i+1}/{max_attempts}: Metabase not ready yet...")
            time.sleep(10)
    
    raise Exception(" Metabase did not become ready in time")


def setup_metabase_initial(**context):
    """Complete initial Metabase setup if not already done."""
    logger.info("🔧 Checking Metabase setup status...")
    
    try:
        # Check if setup is already complete
        response = requests.get(f"{METABASE_URL}/api/session/properties", timeout=10)
        data = response.json()
        
        if data.get('setup-token'):
            logger.info("Completing initial Metabase setup...")
            
            setup_data = {
                "token": data['setup-token'],
                "user": {
                    "first_name": METABASE_FIRST_NAME,
                    "last_name": METABASE_LAST_NAME,
                    "email": METABASE_EMAIL,
                    "password": METABASE_PASSWORD,
                    "site_name": "Procurement Dashboard"
                },
                "prefs": {
                    "site_name": "Procurement Dashboard",
                    "site_locale": "en",
                    "allow_tracking": False
                }
            }
            
            setup_response = requests.post(
                f"{METABASE_URL}/api/setup",
                json=setup_data,
                timeout=30
            )
            
            if setup_response.status_code in [200, 201, 204]:
                logger.info(" Metabase initial setup complete!")
                return True
            else:
                logger.warning(f"Setup response: {setup_response.status_code}")
        else:
            logger.info(" Metabase already configured")
            return True
            
    except Exception as e:
        logger.error(f" Setup error: {str(e)}")
        raise


def get_session_token(**context):
    """Login to Metabase and get session token."""
    logger.info("Logging in to Metabase...")
    
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            response = requests.post(
                f"{METABASE_URL}/api/session",
                json={
                    "username": METABASE_EMAIL,
                    "password": METABASE_PASSWORD
                },
                timeout=10
            )
            
            if response.status_code == 200:
                token = response.json().get('id')
                logger.info(" Login successful!")
                context['ti'].xcom_push(key='session_token', value=token)
                return token
            else:
                logger.warning(f"Attempt {attempt+1}: Login failed with status {response.status_code}")
                time.sleep(5)
                
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}: {str(e)}")
            time.sleep(5)
    
    raise Exception(" Could not login to Metabase")


def create_trino_database(**context):
    """Create Trino database connection in Metabase."""
    logger.info("🔗 Creating Trino database connection...")
    
    token = context['ti'].xcom_pull(key='session_token', task_ids='login_metabase')
    headers = {"X-Metabase-Session": token}
    
    # Check if database already exists
    try:
        db_response = requests.get(f"{METABASE_URL}/api/database", headers=headers, timeout=10)
        databases = db_response.json().get('data', [])
        
        for db in databases:
            if db.get('name') == 'Procurement Trino':
                logger.info(f" Trino database already exists (ID: {db['id']})")
                context['ti'].xcom_push(key='database_id', value=db['id'])
                return db['id']
    except Exception as e:
        logger.warning(f"Could not check existing databases: {e}")
    
    # Create new database connection
    database_config = {
        "name": "Procurement Trino",
        "engine": "starburst",
        "details": {
            "host": TRINO_HOST,
            "port": TRINO_PORT,
            "catalog": TRINO_CATALOG,
            "schema": TRINO_SCHEMA,
            "user": "trino",
            "ssl": False,
            "additional-options": ""
        }
    }
    
    try:
        response = requests.post(
            f"{METABASE_URL}/api/database",
            headers=headers,
            json=database_config,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            db_id = response.json().get('id')
            logger.info(f" Trino database created (ID: {db_id})")
            
            # Sync database schema
            sync_response = requests.post(
                f"{METABASE_URL}/api/database/{db_id}/sync_schema",
                headers=headers,
                timeout=60
            )
            logger.info("Database schema sync initiated")
            
            context['ti'].xcom_push(key='database_id', value=db_id)
            return db_id
        else:
            logger.error(f" Failed to create database: {response.text}")
            raise Exception("Database creation failed")
            
    except Exception as e:
        logger.error(f" Error creating database: {str(e)}")
        raise


def create_dashboard_cards(**context):
    """Create dashboard with KPI cards."""
    logger.info("Creating dashboard and cards...")
    
    token = context['ti'].xcom_pull(key='session_token', task_ids='login_metabase')
    database_id = context['ti'].xcom_pull(key='database_id', task_ids='create_database')
    headers = {"X-Metabase-Session": token}
    
    # Create dashboard
    dashboard_data = {
        "name": "Procurement KPI Dashboard",
        "description": "Real-time procurement metrics and net demand analysis"
    }
    
    try:
        dash_response = requests.post(
            f"{METABASE_URL}/api/dashboard",
            headers=headers,
            json=dashboard_data,
            timeout=10
        )
        
        if dash_response.status_code not in [200, 201]:
            # Dashboard might already exist
            dashboards = requests.get(f"{METABASE_URL}/api/dashboard", headers=headers).json()
            existing = [d for d in dashboards if d.get('name') == 'Procurement KPI Dashboard']
            if existing:
                dashboard_id = existing[0]['id']
                logger.info(f" Using existing dashboard (ID: {dashboard_id})")
            else:
                raise Exception("Could not create or find dashboard")
        else:
            dashboard_id = dash_response.json().get('id')
            logger.info(f" Dashboard created (ID: {dashboard_id})")
        
        # Define KPI queries
        queries = [
            {
                "name": "Total Daily Orders",
                "description": "Sum of all order quantities for latest date",
                "display": "scalar",
                "query": {
                    "database": database_id,
                    "type": "native",
                    "native": {
                        "query": """SELECT SUM(total_quantity) as total_daily_orders
                    FROM hive.procurement_raw.aggregated_orders 
                    WHERE order_date = (SELECT MAX(order_date) 
                    FROM hive.procurement_raw.aggregated_orders)"""
                    }
                }
            },
            {
                "name": "Net Demand by Product",
                "description": "Products with positive net demand",
                "display": "bar",
                "query": {
                    "database": database_id,
                    "type": "native",
                    "native": {
                        "query": """SELECT product_name, net_demand
                    FROM hive.procurement_raw.net_demand 
                    WHERE net_demand > 0 AND calculation_date = (SELECT MAX(calculation_date) 
                    FROM hive.procurement_raw.net_demand)
                    ORDER BY net_demand DESC"""
                    }
                }
            },
            {
                "name": "Supplier Order Volume",
                "description": "Total demand grouped by supplier",
                "display": "pie",
                "query": {
                    "database": database_id,
                    "type": "native",
                    "native": {
                        "query": """SELECT supplier_name, SUM(net_demand) as total_demand
                    FROM hive.procurement_raw.net_demand
                    WHERE calculation_date = (SELECT MAX(calculation_date) 
                    FROM hive.procurement_raw.net_demand)
                    GROUP BY supplier_name
                    ORDER BY total_demand DESC"""
                    }
                }
            },
            {
                "name": "Exception Count",
                "description": "Data quality exceptions",
                "display": "scalar",
                "query": {
                    "database": database_id,
                    "type": "native",
                    "native": {
                        "query": """SELECT 0 as exception_count"""
                    }
                }
            },
            {
                "name": "Total Procurement Cost",
                "description": "Sum of estimated costs",
                "display": "scalar",
                "query": {
                    "database": database_id,
                    "type": "native",
                    "native": {
                        "query": """SELECT CAST(SUM(estimated_cost) AS DECIMAL(12,2)) as total_procurement_cost
                    FROM hive.procurement_raw.net_demand
                    WHERE calculation_date = (SELECT MAX(calculation_date) 
                    FROM hive.procurement_raw.net_demand)"""
                    }
                }
            }
        ]
        
        # Create cards
        card_ids = []
        for idx, query_config in enumerate(queries):
            try:
                card_data = {
                    "name": query_config["name"],
                    "description": query_config["description"],
                    "display": query_config["display"],
                    "dataset_query": query_config["query"],
                    "visualization_settings": {}
                }
                
                card_response = requests.post(
                    f"{METABASE_URL}/api/card",
                    headers=headers,
                    json=card_data,
                    timeout=30
                )
                
                if card_response.status_code in [200, 201]:
                    card_id = card_response.json().get('id')
                    card_ids.append(card_id)
                    logger.info(f"Created card: {query_config['name']} (ID: {card_id})")
                    
                    # Add card to dashboard
                    dashcard_data = {
                        "cardId": card_id,
                        "row": idx * 4,
                        "col": 0,
                        "sizeX": 6,
                        "sizeY": 4
                    }
                    
                    requests.post(
                        f"{METABASE_URL}/api/dashboard/{dashboard_id}/cards",
                        headers=headers,
                        json=dashcard_data,
                        timeout=10
                    )
                else:
                    logger.warning(f"Could not create card: {query_config['name']}")
                    
            except Exception as e:
                logger.warning(f"Error creating card {query_config['name']}: {str(e)}")
        
        logger.info(f" Created {len(card_ids)} dashboard cards")
        logger.info(f"Dashboard URL: http://localhost:3000/dashboard/{dashboard_id}")
        
        return dashboard_id
        
    except Exception as e:
        logger.error(f" Error creating dashboard: {str(e)}")
        raise


# DAG Definition
with DAG(
    'metabase_setup_dashboard',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 12, 1),
    catchup=False,
    description='Automatically configure Metabase dashboard with Trino connection',
    tags=['setup', 'metabase', 'dashboard', 'step-5']
) as dag:
    
    dag.doc_md = """
    ## Metabase Dashboard Setup DAG (Step 5)
    
    **Purpose:** Automatically configure Metabase with Trino connection and create procurement dashboard.
    
    **When to run:** After Step 4 (data upload) and Metabase is running.
    
    **What it does:**
    1. Wait for Metabase to be ready
    2. Complete initial Metabase setup (if needed)
    3. Login and get session token
    4. Create Trino database connection
    5. Create dashboard with 5 KPI cards:
       - Total Daily Orders (Number)
       - Net Demand by Product (Bar Chart)
       - Supplier Order Volume (Pie Chart)
       - Exception Count (Number)
       - Total Procurement Cost (Number with currency)
    
    **Access Dashboard:**
    - URL: http://localhost:3000
    - Email: admin@procurement.local
    - Password: Procurement2026!
    
    **Trigger:** Manual only (click "Trigger DAG" button)
    """
    
    t1_wait = PythonOperator(
        task_id='wait_for_metabase',
        python_callable=wait_for_metabase,
    )
    
    t2_setup = PythonOperator(
        task_id='initial_setup',
        python_callable=setup_metabase_initial,
    )
    
    t3_login = PythonOperator(
        task_id='login_metabase',
        python_callable=get_session_token,
    )
    
    t4_database = PythonOperator(
        task_id='create_database',
        python_callable=create_trino_database,
    )
    
    t5_dashboard = PythonOperator(
        task_id='create_dashboard',
        python_callable=create_dashboard_cards,
    )
    
    # Task dependencies
    t1_wait >> t2_setup >> t3_login >> t4_database >> t5_dashboard
