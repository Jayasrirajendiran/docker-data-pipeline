import os
from pathlib import Path

import pandas as pd


def run_silver():
    """
    Clean and standardize Bronze data into the Silver layer.
    """

    print("=" * 60)
    print("SILVER LAYER STARTED")
    print("=" * 60)

    BASE_PATH = Path("/opt/airflow")

    BRONZE_PATH = BASE_PATH / "data" / "bronze"
    SILVER_PATH = BASE_PATH / "data" / "silver"

    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    date_columns = {
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
        ]
    }

    parquet_files = list(BRONZE_PATH.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet files found in {BRONZE_PATH}"
        )

    for file in parquet_files:

        table = file.stem

        print(f"\nProcessing : {table}")

        df = pd.read_parquet(file)

        print(f"Original Rows : {len(df)}")

        # Remove duplicates
        before = len(df)
        df = df.drop_duplicates()
        print(f"Duplicates Removed : {before - len(df)}")

        # Handle missing values
        for col in df.columns:

            if df[col].dtype == "object":
                df[col] = df[col].fillna("Unknown")
            else:
                df[col] = df[col].fillna(0)

        # Standardize column names
        df.columns = (
            df.columns
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )

        # Convert date columns
        if table in date_columns:

            for col in date_columns[table]:

                if col in df.columns:

                    df[col] = pd.to_datetime(
                        df[col],
                        errors="coerce"
                    )

        output_file = SILVER_PATH / file.name

        df.to_parquet(
            output_file,
            index=False
        )

        print(f"Final Rows : {len(df)}")
        print(f"Saved : {output_file.name}")

    print("\n" + "=" * 60)
    print("SILVER LAYER COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_silver()