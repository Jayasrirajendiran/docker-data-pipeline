from datetime import datetime
import logging

import pandas as pd

from config import BRONZE_PATH, VALIDATION_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_validation():
    """
    Validate Bronze datasets before Silver processing.
    """

    start_time = datetime.now()

    setup_logging("validation.log")

    print_header("VALIDATION LAYER")

    VALIDATION_PATH.mkdir(parents=True, exist_ok=True)

    datasets = {
        "customers": "customer_id",
        "orders": "order_id",
        "order_items": "order_id",
        "products": "product_id",
        "payments": "order_id",
        "reviews": "review_id",
        "geolocation": None,
        "sellers": "seller_id",
        "category_translation": "product_category_name"
    }

    try:

        for table, primary_key in datasets.items():

            file_path = BRONZE_PATH / f"{table}.parquet"

            if not file_path.exists():
                logging.warning(f"{table}.parquet not found")
                continue

            print(f"\nValidating : {table}")

            df = pd.read_parquet(file_path)

            logging.info(f"{table} : {len(df)} rows")

            # Empty Dataset Check
            if df.empty:
                logging.warning(f"{table} is empty")
                continue

            # Primary Key NULL Check
            if primary_key and primary_key in df.columns:

                null_df = df[df[primary_key].isna()]

                if not null_df.empty:

                    null_df.to_csv(
                        VALIDATION_PATH / f"{table}_null_pk.csv",
                        index=False
                    )

                    logging.warning(
                        f"{table} : {len(null_df)} NULL primary keys"
                    )

            # Duplicate Check
            duplicates = df[df.duplicated()]

            if not duplicates.empty:

                duplicates.to_csv(
                    VALIDATION_PATH / f"{table}_duplicates.csv",
                    index=False
                )

                logging.warning(
                    f"{table} : {len(duplicates)} duplicate rows"
                )

        execution_time = get_execution_time(start_time)

        logging.info(
            f"Validation completed in {execution_time} seconds"
        )

        print_footer("VALIDATION LAYER")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_validation()