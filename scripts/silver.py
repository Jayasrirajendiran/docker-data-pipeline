import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
    VALIDATION_PATH,
    SILVER_PATH,
    create_project_folders
)


DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ],
    "reviews": [
        "review_creation_date",
        "review_answer_timestamp"
    ],
    "order_items": [
        "shipping_limit_date"
    ],
    "order_events": [
        "event_timestamp"
    ]
}


NUMERIC_COLUMNS = {
    "geolocation": [
        "geolocation_lat",
        "geolocation_lng"
    ],
    "order_items": [
        "order_item_id",
        "price",
        "freight_value"
    ],
    "payments": [
        "payment_sequential",
        "payment_installments",
        "payment_value"
    ],
    "products": [
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
}


def clean_dataset(dataframe, dataset_name):
    """Clean and standardize one validated dataset."""

    original_rows = len(dataframe)

    # Standardize column names.
    dataframe.columns = [
        str(column).strip().lower()
        for column in dataframe.columns
    ]

    # Convert date columns.
    for column in DATE_COLUMNS.get(
        dataset_name,
        []
    ):
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(
                dataframe[column],
                errors="coerce"
            )

    # Convert numeric columns.
    for column in NUMERIC_COLUMNS.get(
        dataset_name,
        []
    ):
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="coerce"
            )

    # Clean text columns.
    text_columns = dataframe.select_dtypes(
        include=["object", "string"]
    ).columns

    for column in text_columns:
        dataframe[column] = (
            dataframe[column]
            .fillna("Unknown")
            .astype(str)
            .str.strip()
        )

    # Fill missing numeric values.
    numeric_columns = dataframe.select_dtypes(
        include="number"
    ).columns

    dataframe[numeric_columns] = (
        dataframe[numeric_columns]
        .fillna(0)
    )

    # Remove complete duplicate rows.
    dataframe = dataframe.drop_duplicates()

    # Add Silver processing timestamp.
    dataframe["silver_timestamp"] = (
        datetime.now(timezone.utc)
    )

    duplicates_removed = (
        original_rows - len(dataframe)
    )

    return dataframe, original_rows, duplicates_removed


def process_folder(
    input_folder,
    output_folder
):
    """Clean every Parquet file in one folder."""

    parquet_files = list(
        input_folder.glob("*.parquet")
    )

    output_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    for source_file in parquet_files:
        dataset_name = source_file.stem

        dataframe = pd.read_parquet(
            source_file
        )

        (
            dataframe,
            original_rows,
            duplicates_removed
        ) = clean_dataset(
            dataframe=dataframe,
            dataset_name=dataset_name
        )

        output_file = (
            output_folder /
            f"{dataset_name}.parquet"
        )

        dataframe.to_parquet(
            output_file,
            index=False,
            engine="pyarrow"
        )

        print("-" * 60)
        print(f"Dataset            : {dataset_name}")
        print(f"Original rows      : {original_rows}")
        print(f"Duplicates removed : {duplicates_removed}")
        print(f"Final rows         : {len(dataframe)}")
        print(f"Saved              : {output_file}")


def run_silver():
    """Create structured and multi-format Silver datasets."""

    start_time = datetime.now()

    create_project_folders()

    print("=" * 60)
    print("SILVER LAYER STARTED")
    print("=" * 60)

    structured_files = list(
        VALIDATION_PATH.glob("*.parquet")
    )

    if not structured_files:
        raise FileNotFoundError(
            "No validated structured files found"
        )

    # Process the original nine Olist datasets.
    process_folder(
        input_folder=VALIDATION_PATH,
        output_folder=SILVER_PATH
    )

    # Process JSON and XML derived datasets.
    process_folder(
        input_folder=(
            VALIDATION_PATH /
            "semi_structured"
        ),
        output_folder=(
            SILVER_PATH /
            "semi_structured"
        )
    )

    # Process TXT derived datasets.
    process_folder(
        input_folder=(
            VALIDATION_PATH /
            "unstructured"
        ),
        output_folder=(
            SILVER_PATH /
            "unstructured"
        )
    )

    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print("SILVER LAYER COMPLETED")
    print(
        f"Execution time: "
        f"{execution_time:.2f} seconds"
    )
    print("=" * 60)


if __name__ == "__main__":
    run_silver()