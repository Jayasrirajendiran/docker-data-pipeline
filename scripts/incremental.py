from datetime import datetime
import logging

import pandas as pd

from config import GOLD_PATH, INCREMENTAL_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_incremental():

    start_time = datetime.now()

    setup_logging("incremental.log")

    print_header("INCREMENTAL LOADING")

    try:

        INCREMENTAL_PATH.mkdir(parents=True, exist_ok=True)

        input_file = GOLD_PATH / "business_dataset.parquet"

        if not input_file.exists():
            raise FileNotFoundError(
                f"{input_file} not found"
            )

        df = pd.read_parquet(input_file)

        print(f"Total Records : {len(df)}")

        # --------------------------------------------
        # Convert Purchase Timestamp
        # --------------------------------------------

        df["order_purchase_timestamp"] = pd.to_datetime(
            df["order_purchase_timestamp"],
            errors="coerce"
        )

        # --------------------------------------------
        # Last Processed Date
        # --------------------------------------------

        last_processed_date = pd.Timestamp("2018-06-01")

        incremental_df = df[
            df["order_purchase_timestamp"] > last_processed_date
        ]

        print(f"Incremental Records : {len(incremental_df)}")

        output_file = (
            INCREMENTAL_PATH /
            "incremental_business.parquet"
        )

        incremental_df.to_parquet(
            output_file,
            index=False
        )

        print("Saved : incremental_business.parquet")

        execution_time = get_execution_time(start_time)

        logging.info(
            f"Incremental completed in {execution_time} seconds"
        )

        print_footer("INCREMENTAL LOADING")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_incremental()