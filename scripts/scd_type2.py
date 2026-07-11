from datetime import datetime
import logging

import pandas as pd

from config import SCD_TYPE1_PATH, SCD_TYPE2_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_scd_type2():
    """
    SCD Type 2

    Maintains historical records by adding
    effective dates, current flag and version.
    """

    start_time = datetime.now()

    setup_logging("scd_type2.log")

    print_header("SCD TYPE 2")

    try:

        SCD_TYPE2_PATH.mkdir(parents=True, exist_ok=True)

        input_file = SCD_TYPE1_PATH / "customers_dimension.parquet"

        if not input_file.exists():
            raise FileNotFoundError(
                f"{input_file} not found"
            )

        df = pd.read_parquet(input_file)

        print(f"Original Rows : {len(df)}")

        # -----------------------------
        # SCD Type 2 Columns
        # -----------------------------

        current_time = datetime.now()

        df["effective_from"] = current_time

        df["effective_to"] = pd.NaT

        df["is_current"] = True

        df["version"] = 1

        output_file = (
            SCD_TYPE2_PATH /
            "customers_dimension.parquet"
        )

        df.to_parquet(
            output_file,
            index=False
        )

        print(f"Rows Written : {len(df)}")
        print("Saved : customers_dimension.parquet")

        execution_time = get_execution_time(start_time)

        logging.info(
            f"SCD Type 2 completed in {execution_time} seconds"
        )

        print_footer("SCD TYPE 2")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_scd_type2()