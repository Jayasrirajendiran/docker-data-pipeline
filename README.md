# Olist End-to-End Data Engineering Pipeline

An end-to-end Data Engineering project built using Python, Pandas, Apache Airflow, Docker and PostgreSQL.

The project processes the Olist Brazilian E-Commerce dataset using Medallion Architecture and produces analytics-ready datasets for reporting.

## Project Objective

The objective is to build an automated pipeline that:

- Ingests multiple Olist CSV datasets.
- Supports full and incremental order ingestion.
- Stores raw data in the Bronze layer.
- Validates and separates rejected records.
- Cleans and standardises data in the Silver layer.
- Implements SCD Type 1 and SCD Type 2.
- Performs joins, aggregations, lookup mapping and window functions.
- Creates business-ready Gold datasets.
- Loads final datasets into PostgreSQL.
- Records final metadata and audit information.
- Runs on both Windows and Docker/Airflow.

## Technology Stack

| Component | Technology |
|---|---|
| Programming | Python |
| Data processing | Pandas |
| File storage | CSV and Parquet |
| Orchestration | Apache Airflow 2.9.2 |
| Containerisation | Docker and Docker Compose |
| Database | PostgreSQL 15 |
| Database connectivity | SQLAlchemy and Psycopg2 |
| Database administration | pgAdmin |

## Pipeline Workflow

```text
Raw Olist CSV Files
        ↓
Ingestion
Full load + incremental order processing
        ↓
Bronze Layer
Raw Parquet files with technical metadata
        ↓
Data Validation
Valid and rejected records
        ↓
Silver Layer
Cleaned, deduplicated and standardised data
        ↓
 ┌────────────────────┬────────────────────┐
 │ SCD Type 1         │ SCD Type 2         │
 │ Customer dimension │ Product history    │
 └────────────────────┴────────────────────┘
        ↓
Business Transformation
        ↓
Gold Layer
        ↓
PostgreSQL
        ↓
Metadata
        ↓
Audit
```

SCD Type 1 and SCD Type 2 run in parallel because they process independent datasets. Business Transformation begins only after both tasks complete.

## Olist Source Datasets

Place these files inside `data/raw`:

```text
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_orders_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

The dataset is available from the Olist Brazilian E-Commerce dataset on Kaggle.

## Project Structure

```text
docker-data-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── olist_pipeline_dag.py
│   ├── logs/
│   ├── plugins/
│   ├── Dockerfile
│   ├── database.sql
│   └── docker-compose.yml
│
├── data/
│   ├── raw/
│   ├── ingestion/
│   ├── bronze/
│   ├── validation/
│   ├── rejected/
│   ├── silver/
│   ├── scd_type1/
│   ├── scd_type2/
│   └── gold/
│
├── scripts/
│   ├── ingestion.py
│   ├── bronze.py
│   ├── validation.py
│   ├── silver.py
│   ├── scd_type1.py
│   ├── scd_type2.py
│   ├── business_transformation.py
│   ├── gold.py
│   ├── load_postgres.py
│   ├── metadata.py
│   ├── audit.py
│   ├── config.py
│   └── utils.py
│
├── audit/
├── metadata/
├── documentation/
├── main.py
├── requirements.txt
└── README.md
```

## Pipeline Stages

### 1. Ingestion

The ingestion stage copies source datasets into the ingestion layer.

Orders support incremental ingestion using `order_purchase_timestamp`.

- First execution: full order load.
- Later executions: only orders newer than the saved watermark.
- Other datasets: full snapshot copies.

Incremental processing is implemented inside `ingestion.py`. There is no separate `incremental.py` file.

### 2. Bronze Layer

The Bronze layer converts ingested CSV files into Parquet format and adds:

- Load timestamp
- Source filename
- Technical tracking information

### 3. Validation

The validation stage checks:

- Missing required keys
- Duplicate primary keys
- Required columns

Valid records continue through the pipeline. Invalid records are stored in `data/rejected`.

### 4. Silver Layer

The Silver layer:

- Standardises column names
- Removes duplicate rows
- Converts date columns
- Converts numeric columns
- Handles missing text and numeric values

### 5. SCD Type 1

SCD Type 1 is applied to the customer dimension.

It keeps only the latest customer record and overwrites previous values when history is unnecessary.

### 6. SCD Type 2

SCD Type 2 is applied to the product dimension.

It maintains product history using:

- `effective_from`
- `effective_to`
- `is_current`

### 7. Business Transformation

This stage performs:

- Customer, order, item, product and seller joins
- Product category lookup mapping
- Payment aggregation
- Derived-column creation
- Delivery-day calculation
- Customer order ranking using a window function

### 8. Gold Layer

The Gold layer creates:

```text
business_dataset.parquet
customer_summary.parquet
product_summary.parquet
monthly_summary.parquet
seller_summary.parquet
```

### 9. PostgreSQL Load

The pipeline loads five Gold datasets and two SCD dimensions into PostgreSQL:

```text
business_dataset
customer_summary
product_summary
monthly_summary
seller_summary
customer_dimension
product_dimension_history
```

### 10. Metadata

One final Metadata task creates `pipeline_metadata.csv` containing:

- Dataset name
- Layer
- Filename
- File format
- Record count
- Update timestamp

### 11. Audit

Audit is the final pipeline task.

It records:

- Run ID
- Pipeline name
- Start and end time
- Execution duration
- Final status
- Failed-task or error information

## Running on Windows

Create a virtual environment:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

Ensure PostgreSQL is running, then execute:

```powershell
python main.py
```

This runs the complete pipeline from ingestion through Audit.

## Running with Docker and Airflow

Open the Airflow directory:

```powershell
cd airflow
```

Build and start the containers:

```powershell
docker compose up --build -d
```

Check the containers:

```powershell
docker compose ps
```

Open Airflow:

```text
http://localhost:8081
```

Login credentials:

```text
Username: airflow
Password: airflow
```

Enable and trigger:

```text
olist_data_pipeline
```

PostgreSQL is exposed on:

```text
Host: localhost
Port: 5434
Database: olist
Username: airflow
Password: airflow
```

## Verify PostgreSQL Tables

```powershell
docker exec olist_postgres psql -U airflow -d olist -c "\dt"
```

Example query:

```sql
SELECT *
FROM customer_summary
ORDER BY customer_rank
LIMIT 10;
```

## Final Results

The completed pipeline produced:

| Dataset | Rows |
|---|---:|
| business_dataset | 113,425 |
| customer_summary | 96,219 |
| product_summary | 32,328 |
| monthly_summary | 25 |
| seller_summary | 3,095 |
| customer_dimension | 99,441 |
| product_dimension_history | 32,951 |

All eleven Airflow tasks completed successfully.

## Project Report

[Download the complete project report](documentation/Jayasri_R_Data_Engineering_Report.pdf)

## Author

**Jayasri R**

Data Engineering Project using Python, Apache Airflow, Docker and PostgreSQL.
