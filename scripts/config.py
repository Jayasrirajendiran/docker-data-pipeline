import os
from pathlib import Path


# =========================================================
# PROJECT ROOT
# =========================================================

PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[1]
    )
)


# =========================================================
# DATA PATHS
# =========================================================

DATA_PATH = PROJECT_ROOT / "data"

RAW_PATH = DATA_PATH / "raw"
INGESTION_PATH = DATA_PATH / "ingestion"
STAGING_PATH = DATA_PATH / "staging"
BRONZE_PATH = DATA_PATH / "bronze"
VALIDATION_PATH = DATA_PATH / "validation"
REJECTED_PATH = DATA_PATH / "rejected"
SILVER_PATH = DATA_PATH / "silver"

BUSINESS_TRANSFORMATION_PATH = (
    DATA_PATH /
    "business_transformation"
)

SCD_TYPE0_PATH = DATA_PATH / "scd_type0"
SCD_TYPE1_PATH = DATA_PATH / "scd_type1"
SCD_TYPE2_PATH = DATA_PATH / "scd_type2"

GOLD_PATH = DATA_PATH / "gold"


# =========================================================
# SUPPORTING PATHS
# =========================================================

AUDIT_PATH = PROJECT_ROOT / "audit"
METADATA_PATH = PROJECT_ROOT / "metadata"
LOG_PATH = PROJECT_ROOT / "logs"


# =========================================================
# OLIST SOURCE FILES
# =========================================================

DATASETS = {
    "customers":
        "olist_customers_dataset.csv",

    "geolocation":
        "olist_geolocation_dataset.csv",

    "orders":
        "olist_orders_dataset.csv",

    "order_items":
        "olist_order_items_dataset.csv",

    "payments":
        "olist_order_payments_dataset.csv",

    "reviews":
        "olist_order_reviews_dataset.csv",

    "products":
        "olist_products_dataset.csv",

    "sellers":
        "olist_sellers_dataset.csv",

    "category_translation":
        "product_category_name_translation.csv"
}


# =========================================================
# CREATE PROJECT FOLDERS
# =========================================================

def create_project_folders():
    """Create all required project folders."""

    folders = [
        RAW_PATH,
        INGESTION_PATH,
        STAGING_PATH,
        BRONZE_PATH,
        VALIDATION_PATH,
        REJECTED_PATH,
        SILVER_PATH,
        BUSINESS_TRANSFORMATION_PATH,
        SCD_TYPE0_PATH,
        SCD_TYPE1_PATH,
        SCD_TYPE2_PATH,
        GOLD_PATH,
        AUDIT_PATH,
        METADATA_PATH,
        LOG_PATH
    ]

    for folder in folders:
        folder.mkdir(
            parents=True,
            exist_ok=True
        )


# =========================================================
# POSTGRESQL CONFIGURATION
# =========================================================

# Windows uses localhost and Docker's published port 5434.
POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5434"
)

POSTGRES_DATABASE = os.getenv(
    "POSTGRES_DATABASE",
    "olist"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "airflow"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "airflow"
)


SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{POSTGRES_DATABASE}"
)


# =========================================================
# DIRECT EXECUTION
# =========================================================

if __name__ == "__main__":
    create_project_folders()

    print("Project configuration loaded successfully")
    print(f"Project path    : {PROJECT_ROOT}")
    print(f"Raw data path   : {RAW_PATH}")
    print(f"PostgreSQL host : {POSTGRES_HOST}")
    print(f"PostgreSQL port : {POSTGRES_PORT}")
    print(f"Database        : {POSTGRES_DATABASE}")