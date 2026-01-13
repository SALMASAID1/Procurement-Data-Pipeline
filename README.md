# Procurement Data Pipeline

## Overview

A simplified Big Data pipeline for retail procurement that implements an end-to-end batch processing system. The pipeline:

- Collects daily customer orders from POS systems
- Aggregates demand per product (SKU)
- Calculates net demand using MRP formula
- Generates supplier purchase orders as JSON
- Provides real-time dashboards for procurement managers

## Architecture

```
                            ┌─────────────────────────────────────────┐
                            │           APACHE AIRFLOW                │
                            │         (Orchestration)                 │
                            │  DAGs: hdfs_init → trino_init →         │
                            │  upload_data → pipeline → metabase      │
                            └──────────────────┬──────────────────────┘
                                               │ orchestrates
                 ┌─────────────────────────────┼─────────────────────────────┐
                 │                             │                             │
                 ▼                             ▼                             ▼
┌─────────────────────────┐   ┌─────────────────────────┐   ┌─────────────────────────┐
│      DATA INGESTION     │   │       PROCESSING        │   │        OUTPUT           │
│                         │   │                         │   │                         │
│  ┌───────────────────┐  │   │  ┌───────────────────┐  │   │  ┌───────────────────┐  │
│  │   Faker Generator │  │   │  │       TRINO       │  │   │  │   JSON Files      │  │
│  │   (Orders/Stock)  │  │   │  │  (Federated SQL)  │  │   │  │ (Supplier Orders) │  │
│  └─────────┬─────────┘  │   │  └─────────┬─────────┘  │   │  └───────────────────┘  │
│            │            │   │            │            │   │                         │
│            ▼            │   │     ┌──────┴──────┐     │   │  ┌───────────────────┐  │
│  ┌───────────────────┐  │   │     │             │     │   │  │     METABASE      │  │
│  │       HDFS        │◀─┼───┼─────┤   JOIN      │     │   │  │   (Dashboard)     │  │
│  │  (Parquet Files)  │  │   │     │             │     │   │  └─────────┬─────────┘  │
│  │  - /raw/orders    │──┼───┼────▶│             │     │   │            │            │
│  │  - /raw/stock     │  │   │     └──────┬──────┘     │   │            │            │
│  └───────────────────┘  │   │            │            │   │            │            │
│                         │   │            ▼            │   │            │            │
└─────────────────────────┘   │  ┌───────────────────┐  │   │            │            │
                              │  │    PostgreSQL     │  │   │            │            │
                              │  │   (Master Data)   │◀─┼───┼────────────┘            │
                              │  │  - Products       │  │   │   queries via Trino     │
                              │  │  - Suppliers      │  │   │                         │
                              │  └───────────────────┘  │   └─────────────────────────┘
                              │                         │
                              └─────────────────────────┘
```

**Data Flow:**
1. **Ingestion**: Faker generates Orders & Inventory → stored as Parquet in HDFS
2. **Processing**: Trino performs federated queries (HDFS + PostgreSQL master data)
3. **Output**: JSON supplier orders + Metabase dashboards (querying Trino)

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Storage | HDFS (2 DataNodes) | Distributed storage with replication factor 2 |
| Compute | Trino (Starburst) | Federated SQL queries across HDFS + PostgreSQL |
| Orchestration | Apache Airflow | Daily batch processing at 22:00 UTC |
| Master Data | PostgreSQL | Products, suppliers, MOQ, lead times |
| Visualization | Metabase | Procurement manager dashboards |
| Data Format | Parquet (input), JSON (output) | Columnar storage & API-ready exports |

## Quick Start

### Prerequisites

- Docker & Docker Compose v2+
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

### Installation & Startup

```bash
# Clone repository
git clone <repo-url>
cd Procurement-Data-Pipeline

# Start all infrastructure services
cd docker
docker-compose up -d

# Wait for all services to initialize (~2-3 minutes)
docker-compose ps
```

### Verify Services Are Running

```bash
docker-compose ps
```

All services should show as "Up" with healthy status:
- `namenode` - HDFS NameNode
- `datanode1`, `datanode2` - HDFS DataNodes
- `trino` - Query engine
- `postgres-main` - Master data database
- `airflow-scheduler`, `airflow-webserver` - Orchestration
- `metabase` - Visualization

---

##  DAG Execution Order (Step-by-Step)

After starting the infrastructure with `docker-compose up -d`, trigger the Airflow DAGs **in the following order** from the Airflow UI (http://localhost:8081):

### Step 1: Initialize HDFS Structure
**DAG:** `hdfs_initialize`

```bash
# Or via CLI:
docker exec airflow-scheduler airflow dags trigger hdfs_initialize
```

**What it does:**
- Creates required HDFS directories via WebHDFS REST API:
  - `/raw/orders` - Raw order data
  - `/raw/stock` - Inventory snapshots
  - `/processed/aggregated_orders` - Aggregated results
  - `/processed/net_demand` - MRP calculation results
  - `/output/supplier_orders` - JSON exports
  - `/logs/exceptions` - Data quality alerts
- Sets permissions (777) on root directories

---

### Step 2: Initialize Trino Tables
**DAG:** `trino_init_tables`

```bash
# Or via CLI:
docker exec airflow-scheduler airflow dags trigger trino_init_tables
```

**What it does:**
- Creates the Hive schema: `hive.procurement_raw`
- Creates external tables pointing to HDFS:
  - `orders` - Partitioned by `order_date`
  - `inventory` - Partitioned by `snapshot_date`
  - `aggregated_orders` - Processed aggregations
  - `net_demand` - MRP calculations

---

### Step 3: Upload Sample Data to HDFS
**DAG:** `hdfs_upload_sample_data`

```bash
# Or via CLI:
docker exec airflow-scheduler airflow dags trigger hdfs_upload_sample_data
```

**What it does:**
- Generates sample Orders data (100 orders) using Faker
- Generates sample Inventory snapshots for 5 products
- Saves data as CSV locally
- Converts to Parquet format
- Uploads to HDFS via WebHDFS REST API
- Syncs Hive partitions with Trino

---

### Step 4: Run Procurement Pipeline
**DAG:** `procurement_pipeline`

```bash
# Or via CLI:
docker exec airflow-scheduler airflow dags trigger procurement_pipeline
```

**What it does:**
1. **sync_partitions** - Register new HDFS partitions with Hive
2. **aggregate_orders** - Sum daily orders per product (SKU)
3. **calculate_net_demand** - Execute MRP formula
4. **export_supplier_json** - Generate JSON files per supplier
5. **quality_checks** - Log data anomalies and exceptions
6. **copy_to_processed** - Archive data to `/processed/` in HDFS

**Schedule:** Runs automatically daily at 22:00 UTC

---

### Step 5: Setup Metabase Dashboard
**DAG:** `metabase_setup_dashboard`

```bash
# Or via CLI:
docker exec airflow-scheduler airflow dags trigger metabase_setup_dashboard
```

**What it does:**
1. Waits for Metabase to be ready
2. Completes initial Metabase setup (creates admin user)
3. Creates Trino database connection (using Starburst engine)
4. Creates "Procurement KPI Dashboard" with 5 cards:
   - Total Daily Orders (Number)
   - Net Demand by Product (Bar Chart)
   - Supplier Order Volume (Pie Chart)
   - Exception Count (Number)
   - Total Procurement Cost (Number with currency)

---

## Quick Reference: DAG Trigger Order

| Order | DAG Name | Purpose | Trigger Type |
|-------|----------|---------|--------------|
| 1 | `hdfs_initialize` | Create HDFS directories | Manual (once) |
| 2 | `trino_init_tables` | Create Hive tables | Manual (once) |
| 3 | `hdfs_upload_sample_data` | Generate & upload test data | Manual |
| 4 | `procurement_pipeline` | Process data & generate outputs | Manual / Scheduled (22:00 UTC) |
| 5 | `metabase_setup_dashboard` | Configure visualization | Manual (once) |

---

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| HDFS NameNode UI | http://localhost:9870 | - |
| Trino UI | http://localhost:8080 | - |
| Airflow UI | http://localhost:8081 | admin / admin |
| Metabase | http://localhost:3000 | admin@procurement.local / Procurement2026! |
| PostgreSQL | localhost:5432 | admin / admin / procurement |

---

## Project Structure

```
Procurement-Data-Pipeline/
├── docker/
│   ├── dags/                           # Airflow DAGs
│   │   ├── hdfs_initialize_dag.py      # Step 1: HDFS setup
│   │   ├── trino_init_tables_dag.py    # Step 2: Table creation
│   │   ├── hdfs_upload_data_dag.py     # Step 3: Data upload
│   │   ├── procurement_dag.py          # Step 4: Main pipeline
│   │   ├── metabase_setup_dag.py       # Step 5: Dashboard setup
│   │   ├── sql/                        # SQL queries
│   │   │   ├── ddl_postgres.sql        # PostgreSQL schema
│   │   │   ├── aggregate_orders.sql    # Aggregation query
│   │   │   ├── net_demand.sql          # MRP calculation
│   │   │   └── net_demand_insert.sql   # Insert results
│   │   └── utils/                      # Helper modules
│   │       └── trino_client.py         # Trino connection
│   ├── data/                           # Runtime data (mounted)
│   │   ├── raw/                        # Generated CSV files
│   │   │   ├── orders/                 # Order data by date
│   │   │   └── stock/                  # Inventory by date
│   │   └── output/                     # Generated outputs
│   │       └── supplier_orders/        # JSON supplier orders
│   ├── hadoop-conf/                    # HDFS configuration
│   │   ├── core-site.xml
│   │   └── hdfs-site.xml
│   ├── trino-conf/                     # Trino configuration
│   │   ├── config.properties
│   │   ├── jvm.config
│   │   ├── node.properties
│   │   └── catalog/
│   │       ├── hive.properties         # HDFS catalog
│   │       └── postgres.properties     # PostgreSQL catalog
│   ├── metabase-data/                  # Metabase persistence
│   └── docker-compose.yml              # Service definitions
├── requirements.txt
└── README.md
```

---

## HDFS Directory Structure

```
/
├── raw/
│   ├── orders/order_date=YYYY-MM-DD/       # Daily orders (Parquet)
│   └── stock/snapshot_date=YYYY-MM-DD/     # Inventory snapshots
├── processed/
│   ├── aggregated_orders/                   # Summed demand per product
│   └── net_demand/                          # MRP calculation results
├── output/
│   └── supplier_orders/YYYY-MM-DD/          # JSON purchase orders
└── logs/
    └── exceptions/YYYY-MM-DD/               # Data quality alerts
```

---

## Net Demand Formula (MRP)

```
Net Demand = MAX(0, Total Orders + Safety Stock - (Available Qty - Reserved Qty))
```

Where:
- **Total Orders** - Sum of all orders for a product on a given day
- **Safety Stock** - Minimum inventory level to maintain
- **Available Qty** - Current inventory on hand
- **Reserved Qty** - Inventory already committed to other orders

---

## Master Data Schema (PostgreSQL)

### Products Table
| Column | Type | Description |
|--------|------|-------------|
| product_id | SERIAL | Primary key |
| product_name | VARCHAR(255) | Product display name |
| product_code | VARCHAR(50) | Unique SKU code |
| category | VARCHAR(100) | Product category |
| unit_price | DECIMAL(10,2) | Selling price |
| safety_stock_level | INT | Safety stock threshold |
| min_order_quantity | INT | Minimum order quantity |

### Suppliers Table
| Column | Type | Description |
|--------|------|-------------|
| supplier_id | SERIAL | Primary key |
| supplier_name | VARCHAR(255) | Supplier display name |
| supplier_code | VARCHAR(50) | Unique supplier code |
| lead_time_days | INT | Default delivery lead time |
| reliability_score | DECIMAL(3,2) | Supplier reliability rating |

### Product-Supplier Mapping
| Column | Type | Description |
|--------|------|-------------|
| product_id | INT | Foreign key to products |
| supplier_id | INT | Foreign key to suppliers |
| unit_cost | DECIMAL(10,2) | Cost from this supplier |
| priority | INT | Supplier priority ranking |
| is_preferred | BOOLEAN | Preferred supplier flag |

---

## Output Files

### Supplier Order JSON Format

```json
{
  "supplier_id": 1,
  "supplier_name": "TechSupply Co.",
  "order_date": "2026-01-14",
  "data_date": "2026-01-13",
  "generated_at": "2026-01-13T21:06:36.483836",
  "items": [
    {
      "product_id": 1,
      "product_name": "Laptop",
      "quantity": 45,
      "unit_cost": 500.00,
      "total_cost": 22500.00
    }
  ],
  "total_estimated_cost": 22500.00
}
```

---

## Metabase Dashboard

The "Procurement KPI Dashboard" includes:

| Card | Type | Description |
|------|------|-------------|
| Total Daily Orders | Number | Sum of all order quantities for latest date |
| Net Demand by Product | Bar Chart | Products with positive net demand |
| Supplier Order Volume | Pie Chart | Total demand grouped by supplier |
| Exception Count | Number | Data quality exceptions count |
| Total Procurement Cost | Number | Sum of estimated costs |

---

## Troubleshooting

### Services not starting
```bash
# Check container logs
docker-compose logs <service-name>

# Restart a specific service
docker-compose restart <service-name>
```

### HDFS issues
```bash
# Check HDFS health
docker exec namenode hdfs dfsadmin -report

# List HDFS directories
docker exec namenode hdfs dfs -ls /
```

### Trino connection issues
```bash
# Test Trino CLI
docker exec -it trino trino --catalog hive --schema procurement_raw

# Show tables
SHOW TABLES;
```

### Airflow DAG not appearing
```bash
# Check DAG parsing errors
docker exec airflow-scheduler airflow dags list

# Check logs
docker-compose logs airflow-scheduler
```

---

## Team

- **SAID Salma** - Data Engineering
- **TAMZIRT Mohamed** - Data Engineering

---

## License

All Rights Reserved