import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
    BRONZE_PATH,
    VALIDATION_PATH,
    REJECTED_PATH,
    create_project_folders
)


PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": [
        "order_id",
        "order_item_id"
    ],
    "payments": [
        "order_id",
        "payment_sequential"
    ],
    "reviews": ["review_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "category_translation": [
        "product_category_name"
    ],
    "order_events": ["order_id"],
    "product_categories": [
        "product_category_name"
    ]
}


def validate_dataset(
    source_file,
    valid_folder,
    rejected_folder
):
    """Validate one Bronze Parquet dataset."""

    dataset_name = source_file.stem
    dataframe = pd.read_parquet(source_file)

    original_rows = len(dataframe)

    invalid_rows = pd.Series(
        False,
        index=dataframe.index
    )

    primary_keys = PRIMARY_KEYS.get(
        dataset_name,
        []
    )

    if primary_keys:
        missing_columns = [
            column
            for column in primary_keys
            if column not in dataframe.columns
        ]

        if missing_columns:
            raise ValueError(
                f"{dataset_name}: Missing columns "
                f"{missing_columns}"
            )

        null_keys = dataframe[
            primary_keys
        ].isna().any(axis=1)

        duplicate_keys = dataframe.duplicated(
            subset=primary_keys,
            keep="first"
        )

        invalid_rows |= (
            null_keys |
            duplicate_keys
        )

    # Validate unstructured review text.
    if dataset_name == "customer_reviews":
        if "review_text" not in dataframe.columns:
            raise ValueError(
                "customer_reviews: Missing review_text"
            )

        invalid_text = (
            dataframe["review_text"]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("")
        )

        invalid_rows |= invalid_text

    # Validate payment values.
    if (
        dataset_name == "payments"
        and "payment_value" in dataframe.columns
    ):
        payment_value = pd.to_numeric(
            dataframe["payment_value"],
            errors="coerce"
        )

        invalid_rows |= (
            payment_value.isna() |
            (payment_value < 0)
        )

    # Validate Orders timestamps.
    if (
        dataset_name == "orders"
        and "order_purchase_timestamp"
        in dataframe.columns
    ):
        purchase_date = pd.to_datetime(
            dataframe[
                "order_purchase_timestamp"
            ],
            errors="coerce"
        )

        invalid_rows |= purchase_date.isna()

    valid_data = dataframe[
        ~invalid_rows
    ].copy()

    rejected_data = dataframe[
        invalid_rows
    ].copy()

    valid_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    rejected_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    valid_data.to_parquet(
        valid_folder /
        f"{dataset_name}.parquet",
        index=False
    )

    rejected_data.to_parquet(
        rejected_folder /
        f"{dataset_name}_rejected.parquet",
        index=False
    )

    print("-" * 60)
    print(f"Dataset       : {dataset_name}")
    print(f"Original rows : {original_rows}")
    print(f"Valid rows    : {len(valid_data)}")
    print(f"Rejected rows : {len(rejected_data)}")


def validate_folder(
    source_folder,
    valid_folder,
    rejected_folder
):
    """Validate every Parquet file in a folder."""

    parquet_files = list(
        source_folder.glob("*.parquet")
    )

    for source_file in parquet_files:
        validate_dataset(
            source_file=source_file,
            valid_folder=valid_folder,
            rejected_folder=rejected_folder
        )


def run_validation():
    """Validate all Bronze datasets."""

    start_time = datetime.now()

    create_project_folders()

    print("=" * 60)
    print("DATA VALIDATION STARTED")
    print("=" * 60)

    structured_files = list(
        BRONZE_PATH.glob("*.parquet")
    )

    if not structured_files:
        raise FileNotFoundError(
            "No structured Bronze files found"
        )

    # Validate the original nine Olist datasets.
    validate_folder(
        source_folder=BRONZE_PATH,
        valid_folder=VALIDATION_PATH,
        rejected_folder=REJECTED_PATH
    )

    # Validate JSON and XML derived datasets.
    validate_folder(
        source_folder=(
            BRONZE_PATH /
            "semi_structured"
        ),
        valid_folder=(
            VALIDATION_PATH /
            "semi_structured"
        ),
        rejected_folder=(
            REJECTED_PATH /
            "semi_structured"
        )
    )

    # Validate TXT derived datasets.
    validate_folder(
        source_folder=(
            BRONZE_PATH /
            "unstructured"
        ),
        valid_folder=(
            VALIDATION_PATH /
            "unstructured"
        ),
        rejected_folder=(
            REJECTED_PATH /
            "unstructured"
        )
    )

    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print("DATA VALIDATION COMPLETED")
    print(
        f"Execution time: "
        f"{execution_time:.2f} seconds"
    )
    print("=" * 60)


if __name__ == "__main__":
    run_validation()