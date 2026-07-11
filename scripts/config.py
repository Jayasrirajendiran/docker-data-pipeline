from pathlib import Path

# ==========================================================
# Base Project Path
# ==========================================================

BASE_PATH = Path("/opt/airflow/data")

# ==========================================================
# Data Layer Paths
# ==========================================================

RAW_PATH = BASE_PATH / "raw"
INGESTION_PATH = BASE_PATH / "ingestion"
BRONZE_PATH = BASE_PATH / "bronze"
VALIDATION_PATH = BASE_PATH / "validation"
SILVER_PATH = BASE_PATH / "silver"
SCD_TYPE1_PATH = BASE_PATH / "scd_type1"
SCD_TYPE2_PATH = BASE_PATH / "scd_type2"
GOLD_PATH = BASE_PATH / "gold"
INCREMENTAL_PATH = BASE_PATH / "incremental"

# ==========================================================
# Project Folders
# ==========================================================

AUDIT_PATH = Path("/opt/airflow/audit")
METADATA_PATH = Path("/opt/airflow/metadata")

# ==========================================================
# PostgreSQL Connection
# ==========================================================

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{POSTGRES_CONFIG['user']}:"
    f"{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:"
    f"{POSTGRES_CONFIG['port']}/"
    f"{POSTGRES_CONFIG['database']}"
)