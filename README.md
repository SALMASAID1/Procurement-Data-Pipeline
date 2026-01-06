# Procurement Data Pipeline

## Overview

Simplified data pipeline for retail procurement using Big Data technologies. This project implements an end-to-end batch processing system that:

- Collects daily customer orders from POS systems
- Aggregates demand per product (SKU)
- Calculates net demand using MRP formula
- Generates supplier purchase orders as JSON
- Provides real-time dashboards for procurement managers

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Source   │     │   Processing    │     │    Output       │
│   (Faker Gen)   │────▶│   (Trino SQL)   │────▶│  (JSON/HDFS)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│      HDFS       │     │   PostgreSQL    │     │    Metabase     │
│  (Parquet Data) │     │  (Master Data)  │     │   (Dashboard)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Technology Stack:**

| Component | Technology | Purpose |
|-----------|------------|---------|
| Storage | HDFS (2 DataNodes) | Distributed storage with replication factor 2 |
| Compute | Trino | Federated SQL queries across HDFS + PostgreSQL |
| Orchestration | Apache Airflow | Daily batch processing at 22:00 |
| Master Data | PostgreSQL | Products, suppliers, MOQ, lead times |
| Visualization | Metabase | Procurement manager dashboards |
| Data Format | Parquet (input), JSON (output) | Columnar storage & API-ready exports |

## Quick Start

### Prerequisites

- Docker & Docker Compose v2+
- Python 3.9+
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

### Installation

```bash
# Clone repository
git clone <repo-url>
cd Procurement-Data-Pipeline

# Install Python dependencies (for data generator)
pip install -r requirements.txt

# Start infrastructure
cd docker
docker-compose up -d

# Wait for services to initialize (~2-3 minutes)
docker-compose ps
```

### Generate Test Data

```bash
cd src/generator
python main.py --date 2026-01-04
python main.py --date 2026-01-05
python main.py --date 2026-01-06
```

### Upload Data to HDFS

```bash
# Enter namenode container
docker exec -it namenode bash

# Upload orders and inventory
hdfs dfs -put /data/raw/orders/2026-01-06 /raw/orders/order_date=2026-01-06/
hdfs dfs -put /data/raw/stock/2026-01-06 /raw/stock/snapshot_date=2026-01-06/
```

### Run Pipeline

```bash
# Trigger DAG manually
docker exec airflow-scheduler airflow dags trigger procurement_pipeline

# Or wait for scheduled run at 22:00
```

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| HDFS NameNode | http://localhost:9870 | - |
| Trino UI | http://localhost:8080 | - |
| Airflow | http://localhost:8081 | admin / admin |
| Metabase | http://localhost:3000 | (setup required) |
| PostgreSQL | localhost:5432 | admin / admin123 / procurement |

## Pipeline Tasks

The Airflow DAG (`procurement_pipeline`) executes 6 tasks daily:

1. **sync_partitions** - Register new HDFS partitions with Hive
2. **aggregate_orders** - Sum daily orders per product
3. **calculate_net_demand** - Execute MRP formula
4. **export_supplier_json** - Generate JSON files per supplier
5. **quality_checks** - Log data anomalies and exceptions
6. **copy_to_processed** - Archive data to `/processed/` in HDFS

### Net Demand Formula (MRP)

```
Net Demand = MAX(0, Total Orders + Safety Stock - (Available Qty - Reserved Qty))
```

## Project Structure

```
Procurement-Data-Pipeline/
├── docker/
│   ├── dags/                    # Airflow DAGs
│   │   ├── procurement_dag.py   # Main pipeline
│   │   ├── sql/                 # SQL queries
│   │   └── utils/               # Helper modules
│   ├── data/                    # Runtime data (mounted)
│   ├── hadoop-conf/             # HDFS configuration
│   ├── trino-conf/              # Trino catalogs
│   └── docker-compose.yml
├── src/
│   ├── generator/               # Faker data generator
│   ├── export/                  # JSON export module
│   ├── quality/                 # Exception logging
│   └── analytics/               # Dashboard queries
├── docs/                        # LaTeX documentation
├── requirements.txt
└── README.md
```

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

## Metabase Dashboard

The "Procurement Manager View" dashboard includes:

- **Total Daily Orders** - Big number card
- **Net Demand by Product** - Bar chart
- **Supplier Order Volume** - Pie chart
- **Exception Count** - Alert indicator
- **Stock Levels** - Heatmap by warehouse
- **Cost Estimation** - Line chart trend

### Connect Metabase to Trino

1. Open http://localhost:3000
2. Create admin account
3. Add Database → Starburst (Trino)
   - Host: `trino`
   - Port: `8080`
   - Catalog: `hive`
   - Schema: `procurement_raw`

## Team

- **SAID Salma** - Data Engineering
- **TAMZIRT Mohamed** - Data Engineering

## License

All Rights Reserved