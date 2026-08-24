# Olist End-to-End Data Engineering Pipeline

An end-to-end Data Engineering project built using **Python, Pandas, Apache Airflow, Docker and PostgreSQL**.

The project processes the **Olist Brazilian E-Commerce dataset** and demonstrates how raw data from multiple sources and formats can be transformed into clean, validated and analytics-ready data using a complete automated Data Engineering pipeline.

The project follows a **Medallion Architecture** with Ingestion, Staging, Bronze, Validation, Silver, SCD, Business Transformation and Gold layers.

---

## Project Objective

The objective of this project is to build an automated and reliable Data Engineering pipeline that:

- Processes multiple Olist datasets.
- Handles multiple data formats such as CSV, JSON, XML and TXT.
- Performs full and incremental data ingestion.
- Uses watermark-based incremental processing for order data.
- Converts different source formats into Parquet during staging.
- Organises structured, semi-structured and unstructured data.
- Stores raw data in the Bronze layer.
- Performs data quality validation.
- Separates valid and rejected records.
- Cleans and standardises data in the Silver layer.
- Implements SCD Type 0, Type 1 and Type 2.
- Performs joins and business transformations.
- Performs aggregations and lookup mapping.
- Uses window functions for analytical calculations.
- Creates business-ready Gold datasets.
- Loads final datasets into PostgreSQL.
- Maintains pipeline metadata.
- Maintains audit logs.
- Sends email notifications for pipeline success or failure.
- Orchestrates the complete workflow using Apache Airflow.
- Runs the pipeline locally using Python or through Docker and Airflow.

---

# Architecture

The project follows a Medallion-style Data Engineering architecture.

```text
                         OLIST SOURCE DATA
                                |
                 +--------------+--------------+
                 |              |              |
                CSV            JSON         XML / TXT
                 |              |              |
                 +--------------+--------------+
                                |
                            INGESTION
                       Full + Incremental
                                |
                             STAGING
                                |
                  CSV / JSON / XML / TXT
                                |
                             PARQUET
                                |
                             BRONZE
                                |
                           VALIDATION
                           /          \
                        VALID       REJECTED
                          |
                        SILVER
                          |
                 +--------+--------+
                 |        |        |
                SCD 0    SCD 1    SCD 2
                 |        |        |
                 +--------+--------+
                          |
                 BUSINESS TRANSFORMATION
                          |
                         GOLD
                          |
                     POSTGRESQL
                          |
                       METADATA
                          |
                         AUDIT
                          |
                  EMAIL NOTIFICATION
Technology Stack
Component	Technology
Programming	Python
Data Processing	Pandas
File Formats	CSV, JSON, XML, TXT, Parquet
Orchestration	Apache Airflow 2.9.2
Containerisation	Docker
Container Management	Docker Compose
Database	PostgreSQL 15
Database Connectivity	SQLAlchemy, Psycopg2
Database Administration	pgAdmin
Architecture	Medallion Architecture
Version Control	Git and GitHub
Multiple Data Type Processing

Real-world Data Engineering pipelines do not always receive data in a single format.

In this project, the Olist data is processed using multiple formats:

Format	Example	Data Type
CSV	Customers, Orders, Products	Structured
JSON	Order Events	Semi-Structured
XML	Product Categories	Semi-Structured
TXT	Customer Reviews	Unstructured
Parquet	Processed Data	Analytical Format

The additional source files are:

data/raw/json/order_events.json
data/raw/xml/product_categories.xml
data/raw/text/customer_reviews.txt

The pipeline converts these different formats into a common Parquet format during the Staging stage.

CSV  ───────────────┐
JSON ───────────────┤
XML  ───────────────┼──> STAGING ──> PARQUET
TXT  ───────────────┘

This demonstrates how a Data Engineer can handle structured, semi-structured and unstructured data in the same pipeline.

Implementation:

scripts/multiple_datatypes.py
Olist Source Datasets

The original Olist dataset contains the following CSV files:

olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_orders_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv

Additional files created for demonstrating multiple data types:

data/raw/json/order_events.json
data/raw/xml/product_categories.xml
data/raw/text/customer_reviews.txt
Project Structure
docker-data-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── olist_pipeline_dag.py
│   ├── Dockerfile
│   ├── database.sql
│   └── docker-compose.yml
│
├── data/
│   ├── raw/
│   │   ├── json/
│   │   │   └── order_events.json
│   │   ├── xml/
│   │   │   └── product_categories.xml
│   │   ├── text/
│   │   │   └── customer_reviews.txt
│   │   └── Olist CSV files
│   │
│   ├── ingestion/
│   │
│   ├── staging/
│   │   ├── structured/
│   │   ├── semi_structured/
│   │   └── unstructured/
│   │
│   ├── bronze/
│   ├── validation/
│   ├── rejected/
│   ├── silver/
│   ├── scd_type0/
│   ├── scd_type1/
│   ├── scd_type2/
│   ├── business_transformation/
│   └── gold/
│
├── scripts/
│   ├── __init__.py
│   ├── ingestion.py
│   ├── multiple_datatypes.py
│   ├── staging.py
│   ├── bronze.py
│   ├── validation.py
│   ├── silver.py
│   ├── scd_type0.py
│   ├── scd_type1.py
│   ├── scd_type2.py
│   ├── business_transformation.py
│   ├── gold.py
│   ├── load_postgres.py
│   ├── metadata.py
│   ├── audit.py
│   ├── email_notification.py
│   ├── config.py
│   └── utils.py
│
├── audit/
├── metadata/
├── main.py
├── requirements.txt
├── .gitignore
└── README.md
Python Files and Responsibilities
main.py

main.py is the main entry point for running the pipeline locally.

It coordinates the different pipeline stages.

The overall flow is:

Ingestion
    ↓
Multiple Data Type Processing
    ↓
Staging
    ↓
Bronze
    ↓
Validation
    ↓
Silver
    ↓
SCD Type 0 / Type 1 / Type 2
    ↓
Business Transformation
    ↓
Gold
    ↓
PostgreSQL
    ↓
Metadata
    ↓
Audit
    ↓
Email Notification
scripts/config.py

This file contains configuration values used throughout the pipeline.

Examples include:

Input directories.
Output directories.
Database configuration.
Pipeline configuration.
Email configuration.
File locations.

Keeping configuration separately makes the project easier to maintain.

scripts/utils.py

Contains reusable helper functions used by different Python modules.

This avoids writing the same logic repeatedly in multiple files.

scripts/ingestion.py

This file handles data ingestion.

It supports:

Full ingestion.
Incremental order ingestion.
Watermark-based processing.
Copying source datasets into the ingestion layer.
Full Load

For most datasets, the complete source file is processed.

Raw Dataset
    ↓
Ingestion
    ↓
Complete Dataset
Incremental Load

Orders use:

order_purchase_timestamp

to identify newly arrived records.

First execution:

First Run
   ↓
Read All Orders
   ↓
Process Orders
   ↓
Save Latest Timestamp

Later execution:

New Run
   ↓
Read Previous Watermark
   ↓
Compare order_purchase_timestamp
   ↓
Select New Orders
   ↓
Process Only New Records
   ↓
Update Watermark

This prevents already processed orders from being processed again.

The incremental logic is implemented inside:

scripts/ingestion.py

There is no separate incremental.py file.

Staging Layer
scripts/staging.py

The Staging layer acts as a temporary preparation area between Ingestion and Bronze.

The purpose is to standardise different source formats.

CSV  → Parquet
JSON → Parquet
XML  → Parquet
TXT  → Parquet

The Staging layer is organised into:

data/staging/
│
├── structured/
├── semi_structured/
└── unstructured/
Structured Data

CSV datasets are structured data.

CSV
 ↓
Structured Staging
 ↓
Parquet
Semi-Structured Data

JSON and XML are treated as semi-structured data.

JSON / XML
     ↓
Semi-Structured Staging
     ↓
Parquet
Unstructured Data

TXT customer review comments are treated as unstructured data.

TXT
 ↓
Unstructured Staging
 ↓
Parquet

Parquet provides an efficient and common format for downstream analytical processing.

Implementation:

scripts/staging.py
scripts/multiple_datatypes.py
Bronze Layer
scripts/bronze.py

The Bronze layer stores data in raw or near-raw form.

The purpose is to preserve the source data before major transformations.

Bronze processing includes:

Reading ingested data.
Converting data to Parquet.
Preserving source information.
Adding technical metadata.
Maintaining a reusable raw processing layer.

Simple explanation:

Raw Data
   ↓
Bronze
   ↓
Raw / Preserved Data
Validation
scripts/validation.py

The Validation stage checks whether the incoming data is valid before further processing.

Validation checks include:

Required columns.
Missing primary keys.
Duplicate primary keys.
Required fields.
Basic data validity.

The flow is:

                 VALIDATION
                     |
             +-------+-------+
             |               |
           VALID          REJECTED
             |               |
             ↓               ↓
          SILVER         Rejected Folder

Invalid records are not permanently deleted.

They are stored in:

data/rejected/

This allows the records to be investigated later.

Silver Layer
scripts/silver.py

The Silver layer contains cleaned and trusted data.

The pipeline performs:

Column name standardisation.
Duplicate removal.
Date conversion.
Numeric conversion.
Missing-value handling.
Data-type correction.
Basic data cleaning.

The simple concept is:

Bronze = Raw / Preserved Data
Silver = Clean / Trusted Data
Slowly Changing Dimensions

SCD stands for Slowly Changing Dimension.

SCD determines how changes in dimension information should be handled.

This project demonstrates:

SCD Type 0
SCD Type 1
SCD Type 2
SCD Type 0
scripts/scd_type0.py

SCD Type 0 preserves the original value.

The value is not changed even if the source data changes.

Example:

Original City = Chennai

Later:
Customer City = Bangalore

Stored Value = Chennai

The original value is permanently preserved.

This is useful when the initial/original value is important for analysis.

Output:

data/scd_type0/
SCD Type 1
scripts/scd_type1.py

SCD Type 1 overwrites the previous value.

Example:

Old City = Chennai
New City = Bangalore

Final City = Bangalore

The old value is not retained.

SCD Type 1 is useful when only the current value is required.

SCD Type 2
scripts/scd_type2.py

SCD Type 2 maintains complete history.

Instead of overwriting the old record, a new version is created.

Example:

Version 1
City = Chennai
is_current = False

Version 2
City = Bangalore
is_current = True

Important fields include:

effective_from
effective_to
is_current

This allows us to answer:

What was the previous value?
What is the current value?
When did the change happen?
SCD Comparison
SCD Type	Behaviour	History
Type 0	Keep original value	Original value preserved
Type 1	Overwrite old value	No history
Type 2	Create new record/version	Full history
Business Transformation
scripts/business_transformation.py

After the data has been cleaned and SCD processing is completed, business transformations are performed.

The pipeline combines information from:

Customers
Orders
Order Items
Products
Sellers
Payments
Reviews

The transformation stage performs:

Customer joins.
Order joins.
Product joins.
Seller joins.
Payment aggregation.
Product category lookup.
Derived column creation.
Delivery-day calculation.
Customer order ranking.
Aggregations.
Window functions.

Example derived fields include:

Order Year
Order Month
Delivery Days
Item Total
Total Payment
Customer Order Number
Gold Layer
scripts/gold.py

The Gold layer contains business-ready analytical datasets.

The project creates:

business_dataset.parquet
customer_summary.parquet
product_summary.parquet
monthly_summary.parquet
seller_summary.parquet

These datasets are ready for:

Reporting.
Business analysis.
SQL queries.
Power BI.
Dashboard development.

Simple explanation:

Bronze = Raw Data
Silver = Clean Data
Gold   = Business-Ready Data
Star Schema

The Gold layer can be used for analytical modelling using a Star Schema.

The central fact table contains measurable business information.

                  Customer Dimension
                          |
                          |
Product Dimension --- Fact Sales --- Seller Dimension
                          |
                          |
                    Date Dimension

The fact table can contain measures such as:

Price
Freight Value
Payment
Item Total
Delivery Days

Dimension tables contain descriptive information such as:

Customer
Product
Seller
Date

The Star Schema structure makes analytical queries and BI reporting easier and more efficient.

PostgreSQL
scripts/load_postgres.py

The final Gold datasets and SCD dimensions are loaded into PostgreSQL.

Tables include:

business_dataset
customer_summary
product_summary
monthly_summary
seller_summary
customer_dimension
product_dimension_history

PostgreSQL provides:

SQL querying.
Centralised analytical storage.
BI connectivity.
Structured access to final datasets.
Easier reporting and analysis.
Metadata
scripts/metadata.py

Metadata means:

Data about Data

The pipeline maintains information about processed datasets.

Metadata contains:

Dataset Name
Layer
Filename
File Format
Record Count
Update Timestamp

Example:

Dataset: customers
Layer: Silver
Filename: customers.parquet
Format: Parquet
Rows: 99,441

The metadata output is stored under:

metadata/
Audit Logging
scripts/audit.py

Audit logging tracks pipeline execution.

It records:

Run ID
Pipeline Name
Start Time
End Time
Execution Duration
Final Status
Failed Task
Error Information

Audit allows us to answer:

When did the pipeline run?
How long did it take?
Did the pipeline succeed?
Which task failed?
What was the error?

The audit information helps with pipeline monitoring and troubleshooting.

Email Notification
scripts/email_notification.py

The pipeline includes automatic email notification.

After the pipeline completes, the notification can report either:

SUCCESS

or:

FAILED
Success Email

A successful email can contain:

Pipeline Name
Run ID
SUCCESS Status
Execution Time
Processed Assets
PostgreSQL Tables
Record Counts
Failure Email

A failed email can contain:

Pipeline Name
Run ID
FAILED Status
Failed Task
Error Message
Execution Details

This provides operational monitoring without requiring manual inspection of Airflow logs every time.

Apache Airflow
airflow/dags/olist_pipeline_dag.py

Apache Airflow is used to orchestrate the complete pipeline.

Airflow manages:

Task execution.
Task dependencies.
Scheduling.
Retries.
Task monitoring.
Execution logs.
Failure tracking.

The pipeline dependency is:

Multiple Data Types
        ↓
    Ingestion
        ↓
     Staging
        ↓
      Bronze
        ↓
    Validation
        ↓
      Silver
        ↓
 +------+------+------+
 |      |      |      |
SCD0   SCD1   SCD2
 |      |      |
 +------+------+------+
        ↓
Business Transformation
        ↓
       Gold
        ↓
   PostgreSQL
        ↓
    Metadata
        ↓
      Audit
        ↓
Email Notification

Independent SCD processing tasks can run in parallel, and the business transformation stage starts after the required SCD processing is completed.

Docker

Docker provides a consistent execution environment for the pipeline.

The project uses Docker containers for services such as:

Apache Airflow
PostgreSQL

Docker Compose manages the services.

Important files:

airflow/docker-compose.yml
airflow/Dockerfile

Benefits of Docker:

Consistent environment.
Service isolation.
Reproducible execution.
Easier deployment.
Easier local development.
Running on Windows

Create a virtual environment:

python -m venv .venv

Activate the environment:

.venv\Scripts\Activate.ps1

Install dependencies:

pip install -r requirements.txt

Ensure PostgreSQL is running.

Run the pipeline:

python main.py

This runs the complete pipeline from ingestion through email notification.

Running with Docker and Airflow

Open the Airflow directory:

cd airflow

Build and start the containers:

docker compose up --build -d

Check the containers:

docker compose ps

Open Airflow:

http://localhost:8081

Default credentials:

Username: airflow
Password: airflow

Enable and trigger the DAG:

olist_data_pipeline
PostgreSQL Configuration

PostgreSQL can be accessed using:

Host: localhost
Port: 5434
Database: olist
Username: airflow
Password: airflow

The port can be changed through the Docker Compose configuration if required.

Verify PostgreSQL Tables

Run:

docker exec olist_postgres psql -U airflow -d olist -c "\dt"

Example query:

SELECT *
FROM customer_summary
ORDER BY customer_rank
LIMIT 10;
Final Data Outputs

The pipeline produces business-ready analytical datasets and dimension tables.

Dataset	Purpose
business_dataset	Complete business-level dataset
customer_summary	Customer-level analysis
product_summary	Product-level analysis
monthly_summary	Monthly sales analysis
seller_summary	Seller-level analysis
customer_dimension	Customer dimension
product_dimension_history	Historical product dimension

Example pipeline results:

Dataset	Rows
business_dataset	113,425
customer_summary	96,219
product_summary	32,328
monthly_summary	25
seller_summary	3,095
customer_dimension	99,441
product_dimension_history	32,951
Key Data Engineering Concepts Demonstrated

This project demonstrates:

End-to-end Data Engineering.
ETL pipeline development.
Medallion Architecture.
Structured data processing.
Semi-structured data processing.
Unstructured data processing.
CSV processing.
JSON processing.
XML processing.
TXT processing.
Parquet conversion.
Staging layer.
Full data ingestion.
Incremental data ingestion.
Watermark-based processing.
Bronze layer.
Data validation.
Rejected-record handling.
Silver layer.
Data cleaning.
Data standardisation.
SCD Type 0.
SCD Type 1.
SCD Type 2.
Business transformations.
Joins.
Aggregations.
Lookup mapping.
Window functions.
Gold layer.
Star Schema.
PostgreSQL.
Metadata management.
Audit logging.
Email notification.
Apache Airflow.
Docker.
Docker Compose.
Pipeline monitoring.
Complete Pipeline Summary

The complete project flow is:

                         OLIST DATA
                             |
             +---------------+---------------+
             |               |               |
            CSV             JSON          XML / TXT
             |               |               |
             +---------------+---------------+
                             |
                         INGESTION
                    Full / Incremental
                             |
                          STAGING
                             |
                 Format Standardisation
                             |
                 CSV / JSON / XML / TXT
                             |
                          PARQUET
                             |
                          BRONZE
                             |
                        VALIDATION
                        /         \
                     VALID      REJECTED
                       |
                     SILVER
                       |
              +--------+--------+
              |        |        |
             SCD0     SCD1     SCD2
              |        |        |
              +--------+--------+
                       |
             BUSINESS TRANSFORMATION
                       |
                      GOLD
                       |
                  POSTGRESQL
                       |
                    METADATA
                       |
                     AUDIT
                       |
              EMAIL NOTIFICATION
Why This Project Is Useful

This project demonstrates how raw e-commerce data can be transformed into trusted analytical data through a complete Data Engineering workflow.

Instead of directly using raw data for analysis, the pipeline:

Multiple Source Formats
        ↓
Data Ingestion
        ↓
Staging
        ↓
Parquet Conversion
        ↓
Bronze
        ↓
Validation
        ↓
Silver
        ↓
SCD Type 0 / Type 1 / Type 2
        ↓
Business Transformation
        ↓
Gold
        ↓
PostgreSQL
        ↓
Metadata
        ↓
Audit
        ↓
Email Notification

This project demonstrates a practical Data Engineering architecture that can be extended for larger e-commerce, banking, retail and other enterprise data processing use cases.

Project Report

The complete project report can be maintained inside the documentation folder.

Author

Jayasri R

Data Engineering Project using:

Python | Pandas | Apache Airflow | Docker | PostgreSQL | Git | GitHub