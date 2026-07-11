from datetime import datetime
import logging

import pandas as pd
from sqlalchemy import create_engine

from config import GOLD_PATH, SQLALCHEMY_DATABASE_URI
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_load_postgres():

    start_time = datetime.now()

    setup_logging("load_postgres.log")

    print_header("LOAD TO POSTGRESQL")

    try:

        engine = create_engine(SQLALCHEMY_DATABASE_URI)

        datasets = {
            "business_dataset": "business_dataset.parquet",
            "customer_summary": "customer_summary.parquet",
            "product_summary": "product_summary.parquet",
            "monthly_summary": "monthly_summary.parquet",
            "customer_rank": "customer_rank.parquet"
        }

        for table_name, file_name in datasets.items():

            file_path = GOLD_PATH / file_name

            if not file_path.exists():
                print(f"Skipped : {file_name}")
                continue

            df = pd.read_parquet(file_path)

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False
            )

            print(
                f"{table_name} -> {len(df)} rows loaded"
            )

        execution_time = get_execution_time(start_time)

        logging.info(
            f"PostgreSQL loading completed in {execution_time} seconds"
        )

        print_footer("LOAD TO POSTGRESQL")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_load_postgres()