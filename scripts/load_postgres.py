import sys
import psycopg2
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    GOLD_PATH,
    SCD_TYPE1_PATH,
    SCD_TYPE2_PATH,
    SQLALCHEMY_DATABASE_URI,
)


def run_load_postgres():

    print("=" * 60)
    print("POSTGRESQL LOAD STARTED")
    print("=" * 60)

    engine = create_engine(
        SQLALCHEMY_DATABASE_URI
    )

    datasets = {
        "business_dataset": (
            GOLD_PATH / "business_dataset.parquet"
        ),
        "customer_summary": (
            GOLD_PATH / "customer_summary.parquet"
        ),
        "product_summary": (
            GOLD_PATH / "product_summary.parquet"
        ),
        "monthly_summary": (
            GOLD_PATH / "monthly_summary.parquet"
        ),
        "seller_summary": (
            GOLD_PATH / "seller_summary.parquet"
        ),
        "customer_dimension": (
            SCD_TYPE1_PATH
            / "customer_dimension.parquet"
        ),
        "product_dimension_history": (
            SCD_TYPE2_PATH
            / "product_dimension_history.parquet"
        ),
    }

    try:

        for table_name, file_path in datasets.items():

            if not file_path.exists():

                print(
                    f"Skipped: {file_path.name} not found"
                )

                continue

            dataframe = pd.read_parquet(file_path)

            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
                chunksize=5000,
                method="multi",
            )

            print(
                f"{table_name}: "
                f"{len(dataframe)} rows loaded"
            )

        print("=" * 60)
        print("POSTGRESQL LOAD COMPLETED")
        print("=" * 60)

    finally:

        engine.dispose()


if __name__ == "__main__":
    run_load_postgres()