import os
from pathlib import Path


# Automatically locate the project folder
PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parent.parent
    )
)


# Main data folders
DATA_PATH = PROJECT_ROOT / "data"

RAW_PATH = DATA_PATH / "raw"
INGESTION_PATH = DATA_PATH / "ingestion"
BRONZE_PATH = DATA_PATH / "bronze"
VALIDATION_PATH = DATA_PATH / "validation"
REJECTED_PATH = DATA_PATH / "rejected"
SILVER_PATH = DATA_PATH / "silver"
SCD_TYPE1_PATH = DATA_PATH / "scd_type1"
SCD_TYPE2_PATH = DATA_PATH / "scd_type2"
GOLD_PATH = DATA_PATH / "gold"


# Supporting folders
AUDIT_PATH = PROJECT_ROOT / "audit"
METADATA_PATH = PROJECT_ROOT / "metadata"
LOG_PATH = PROJECT_ROOT / "logs"


# Olist source filenames
DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def create_project_folders():
    """
    Create missing project folders automatically.
    """

    folders = [
        RAW_PATH,
        INGESTION_PATH,
        BRONZE_PATH,
        VALIDATION_PATH,
        REJECTED_PATH,
        SILVER_PATH,
        SCD_TYPE1_PATH,
        SCD_TYPE2_PATH,
        GOLD_PATH,
        AUDIT_PATH,
        METADATA_PATH,
        LOG_PATH,
    ]

    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)


# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "olist")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")


SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DATABASE}"
)


if __name__ == "__main__":
    create_project_folders()

    print("Project configuration loaded successfully")
    print(f"Project path : {PROJECT_ROOT}")
    print(f"Raw data path: {RAW_PATH}")
