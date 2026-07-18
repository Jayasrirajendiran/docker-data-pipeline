import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    BRONZE_PATH,
    VALIDATION_PATH,
    REJECTED_PATH,
    create_project_folders,
)


PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"],
}


def run_validation():

    start_time = datetime.now()

    create_project_folders()

    print("=" * 60)
    print("DATA VALIDATION STARTED")
    print("=" * 60)

    parquet_files = list(
        BRONZE_PATH.glob("*.parquet")
    )

    if not parquet_files:
        raise FileNotFoundError(
            "No Bronze Parquet files found"
        )

    for source_file in parquet_files:

        dataset_name = source_file.stem
        dataframe = pd.read_parquet(source_file)

        original_rows = len(dataframe)

        # Initially, all rows are valid
        invalid_rows = pd.Series(
            False,
            index=dataframe.index,
        )

        primary_keys = PRIMARY_KEYS.get(
            dataset_name,
            [],
        )

        if primary_keys:

            # Check whether primary-key columns exist
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

            # Find null primary keys
            null_keys = dataframe[
                primary_keys
            ].isna().any(axis=1)

            # Find duplicate primary keys
            duplicate_keys = dataframe.duplicated(
                subset=primary_keys,
                keep="first",
            )

            invalid_rows = (
                null_keys | duplicate_keys
            )

        valid_data = dataframe[
            ~invalid_rows
        ].copy()

        rejected_data = dataframe[
            invalid_rows
        ].copy()

        valid_data.to_parquet(
            VALIDATION_PATH
            / f"{dataset_name}.parquet",
            index=False,
        )

        rejected_data.to_parquet(
            REJECTED_PATH
            / f"{dataset_name}_rejected.parquet",
            index=False,
        )

        print(f"\nDataset       : {dataset_name}")
        print(f"Original rows : {original_rows}")
        print(f"Valid rows    : {len(valid_data)}")
        print(f"Rejected rows : {len(rejected_data)}")

    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print("DATA VALIDATION COMPLETED")
    print(f"Execution time: {execution_time:.2f} seconds")
    print("=" * 60)


if __name__ == "__main__":
    run_validation()