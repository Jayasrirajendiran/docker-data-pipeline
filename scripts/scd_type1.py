from datetime import datetime
import logging
import pandas as pd

from config import SILVER_PATH, SCD_TYPE1_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_scd_type1():

    start_time = datetime.now()

    setup_logging("scd_type1.log")

    print_header("SCD TYPE 1")

    try:

        SCD_TYPE1_PATH.mkdir(parents=True, exist_ok=True)

        input_file = SILVER_PATH / "customers.parquet"

        if not input_file.exists():
            raise FileNotFoundError(
                f"{input_file} not found"
            )

        df = pd.read_parquet(input_file)

        print(f"Original Rows : {len(df)}")

        # Keep latest customer record
        df = df.drop_duplicates(
            subset=["customer_id"],
            keep="last"
        )

        print(f"Rows After SCD Type 1 : {len(df)}")

        output_file = (
            SCD_TYPE1_PATH /
            "customers_dimension.parquet"
        )

        df.to_parquet(
            output_file,
            index=False
        )

        print("Saved : customers_dimension.parquet")

        execution_time = get_execution_time(start_time)

        logging.info(
            f"SCD Type 1 completed in {execution_time} seconds"
        )

        print_footer("SCD TYPE 1")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_scd_type1()