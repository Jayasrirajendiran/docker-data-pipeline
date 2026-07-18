import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    VALIDATION_PATH,
    SILVER_PATH,
    create_project_folders,
)


DATE_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "reviews": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
    "order_items": [
        "shipping_limit_date",
    ],
}


NUMERIC_COLUMNS = {
    "geolocation": [
        "geolocation_lat",
        "geolocation_lng",
    ],
    "order_items": [
        "order_item_id",
        "price",
        "freight_value",
    ],
    "payments": [
        "payment_sequential",
        "payment_installments",
        "payment_value",
    ],
    "products": [
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ],
}


def run_silver():

    start_time = datetime.now()

    create_project_folders()

    print("=" * 60)
    print("SILVER LAYER STARTED")
    print("=" * 60)

    parquet_files = list(
        VALIDATION_PATH.glob("*.parquet")
    )

    if not parquet_files:
        raise FileNotFoundError(
            "No validated Parquet files found"
        )

    for source_file in parquet_files:

        dataset_name = source_file.stem
        dataframe = pd.read_parquet(source_file)

        original_rows = len(dataframe)

        # Standardize column names
        dataframe.columns = [
            column.strip().lower()
            for column in dataframe.columns
        ]

        # Remove duplicate rows
        dataframe = dataframe.drop_duplicates()

        # Convert date columns
        for column in DATE_COLUMNS.get(
            dataset_name,
            [],
        ):
            if column in dataframe.columns:
                dataframe[column] = pd.to_datetime(
                    dataframe[column],
                    errors="coerce",
                )

        # Convert numeric columns
        for column in NUMERIC_COLUMNS.get(
            dataset_name,
            [],
        ):
            if column in dataframe.columns:
                dataframe[column] = pd.to_numeric(
                    dataframe[column],
                    errors="coerce",
                )

        # Fill missing text values
        text_columns = dataframe.select_dtypes(
            include=["object", "string"]
        ).columns

        dataframe[text_columns] = (
            dataframe[text_columns]
            .fillna("Unknown")
        )

        # Fill missing numeric values
        numeric_columns = dataframe.select_dtypes(
            include="number"
        ).columns

        dataframe[numeric_columns] = (
            dataframe[numeric_columns]
            .fillna(0)
        )

        dataframe["silver_timestamp"] = (
            datetime.now()
        )

        output_file = (
            SILVER_PATH
            / f"{dataset_name}.parquet"
        )

        dataframe.to_parquet(
            output_file,
            index=False,
        )

        print(f"\nDataset           : {dataset_name}")
        print(f"Original rows     : {original_rows}")
        print(f"Duplicates removed: {original_rows - len(dataframe)}")
        print(f"Final rows        : {len(dataframe)}")
        print(f"Saved             : {output_file.name}")

    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print("SILVER LAYER COMPLETED")
    print(f"Execution time: {execution_time:.2f} seconds")
    print("=" * 60)


if __name__ == "__main__":
    run_silver()