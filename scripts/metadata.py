import json
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
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
    METADATA_PATH,
    SQLALCHEMY_DATABASE_URI,
    create_project_folders
)


METADATA_FILE = (
    METADATA_PATH /
    "pipeline_metadata.csv"
)

OLAP_SCHEMA = "olist_olap"


LAYERS = {
    "raw": RAW_PATH,
    "ingestion": INGESTION_PATH,
    "staging": STAGING_PATH,
    "bronze": BRONZE_PATH,
    "validation": VALIDATION_PATH,
    "rejected": REJECTED_PATH,
    "silver": SILVER_PATH,
    "business_transformation":
        BUSINESS_TRANSFORMATION_PATH,
    "scd_type0": SCD_TYPE0_PATH,
    "scd_type1": SCD_TYPE1_PATH,
    "scd_type2": SCD_TYPE2_PATH,
    "gold": GOLD_PATH
}


def get_record_count(file_path):
    """Get record count based on the file format."""

    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return len(
            pd.read_csv(
                file_path,
                low_memory=False
            )
        )

    if suffix == ".parquet":
        return (
            pq.ParquetFile(file_path)
            .metadata
            .num_rows
        )

    if suffix == ".json":
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:
            data = json.load(file)

        return (
            len(data)
            if isinstance(data, list)
            else 1
        )

    if suffix == ".xml":
        tree = ET.parse(file_path)
        root = tree.getroot()

        return len(list(root))

    if suffix == ".txt":
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:
            return sum(
                1
                for line in file
                if line.strip()
            )

    return 0


def collect_file_metadata():
    """Collect metadata from every pipeline layer."""

    supported_formats = {
        ".csv",
        ".parquet",
        ".json",
        ".xml",
        ".txt"
    }

    records = []

    for layer_name, folder_path in LAYERS.items():
        if not folder_path.exists():
            continue

        for file_path in folder_path.rglob("*"):
            if (
                not file_path.is_file()
                or file_path.suffix.lower()
                not in supported_formats
            ):
                continue

            records.append({
                "asset_name": file_path.stem,
                "layer": layer_name,
                "storage_type": "file",
                "location": str(file_path),
                "file_format":
                    file_path.suffix
                    .replace(".", "")
                    .upper(),
                "record_count":
                    get_record_count(file_path),
                "updated_at":
                    datetime.now()
            })

    return records


def collect_postgres_metadata():
    """Collect metadata from PostgreSQL OLAP tables."""

    records = []

    engine = create_engine(
        SQLALCHEMY_DATABASE_URI
    )

    try:
        with engine.connect() as connection:
            tables = connection.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema_name
                    ORDER BY table_name
                    """
                ),
                {
                    "schema_name": OLAP_SCHEMA
                }
            ).fetchall()

            for table in tables:
                table_name = table[0]

                row_count = connection.execute(
                    text(
                        f"""
                        SELECT COUNT(*)
                        FROM {OLAP_SCHEMA}.{table_name}
                        """
                    )
                ).scalar()

                records.append({
                    "asset_name": table_name,
                    "layer": "postgresql",
                    "storage_type": "database",
                    "location":
                        f"{OLAP_SCHEMA}.{table_name}",
                    "file_format": "TABLE",
                    "record_count": row_count,
                    "updated_at": datetime.now()
                })

    finally:
        engine.dispose()

    return records


def run_metadata():
    """Create one final pipeline metadata report."""

    print("=" * 60)
    print("METADATA COLLECTION STARTED")
    print("=" * 60)

    create_project_folders()

    file_records = collect_file_metadata()
    database_records = collect_postgres_metadata()

    all_records = (
        file_records +
        database_records
    )

    metadata = pd.DataFrame(
        all_records
    )

    metadata.to_csv(
        METADATA_FILE,
        index=False
    )

    print(f"File assets     : {len(file_records)}")
    print(f"Database tables : {len(database_records)}")
    print(f"Total assets    : {len(metadata)}")
    print(f"Saved           : {METADATA_FILE}")

    print("=" * 60)
    print("METADATA COLLECTION COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_metadata()