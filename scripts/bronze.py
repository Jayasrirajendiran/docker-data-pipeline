from datetime import datetime
import logging

import pandas as pd

from config import INGESTION_PATH, BRONZE_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_batch_id,
    get_execution_time
)


def run_bronze():
    """
    Read CSV files from Ingestion layer,
    convert them to Parquet,
    add metadata columns,
    and save them in the Bronze layer.
    """

    start_time = datetime.now()

    setup_logging("bronze.log")

    print_header("BRONZE LAYER")

    batch_id = get_batch_id()

    try:

        BRONZE_PATH.mkdir(parents=True, exist_ok=True)

        csv_files = list(INGESTION_PATH.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(
                f"No CSV files found in {INGESTION_PATH}"
            )

        # Mapping raw filenames to standardized dataset names
        file_mapping = {
            "olist_customers_dataset": "customers",
            "olist_orders_dataset": "orders",
            "olist_order_items_dataset": "order_items",
            "olist_order_payments_dataset": "payments",
            "olist_order_reviews_dataset": "reviews",
            "olist_products_dataset": "products",
            "olist_sellers_dataset": "sellers",
            "olist_geolocation_dataset": "geolocation",
            "product_category_name_translation": "category_translation"
        }

        logging.info(f"Batch ID : {batch_id}")

        for file in csv_files:

            print(f"\nProcessing : {file.name}")

            df = pd.read_csv(file)

            # Metadata Columns
            df["load_timestamp"] = datetime.now()
            df["batch_id"] = batch_id
            df["source_file"] = file.name

            dataset_name = file_mapping.get(file.stem, file.stem)

            output_file = BRONZE_PATH / f"{dataset_name}.parquet"

            df.to_parquet(
                output_file,
                index=False
            )

            print(f"Rows Loaded : {len(df)}")
            print(f"Saved : {dataset_name}.parquet")

            logging.info(
                f"{dataset_name}.parquet -> {len(df)} rows loaded"
            )

        execution_time = get_execution_time(start_time)

        logging.info(
            f"Bronze completed in {execution_time} seconds"
        )

        print_footer("BRONZE LAYER")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_bronze()