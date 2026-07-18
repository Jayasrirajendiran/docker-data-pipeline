import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    SILVER_PATH,
    SCD_TYPE1_PATH,
    create_project_folders,
)


def run_scd_type1():

    create_project_folders()

    print("=" * 50)
    print("SCD TYPE 1 STARTED")
    print("=" * 50)

    source_file = SILVER_PATH / "customers.parquet"

    output_file = (
        SCD_TYPE1_PATH
        / "customer_dimension.parquet"
    )

    if not source_file.exists():
        raise FileNotFoundError(
            "customers.parquet not found in Silver"
        )

    new_data = pd.read_parquet(source_file)

    # If the dimension already exists, combine old and new data
    if output_file.exists():

        old_data = pd.read_parquet(output_file)

        final_data = pd.concat(
            [old_data, new_data],
            ignore_index=True,
        )

    else:

        final_data = new_data

    # Keep the latest value for each customer
    final_data = final_data.drop_duplicates(
        subset=["customer_id"],
        keep="last",
    )

    final_data.to_parquet(
        output_file,
        index=False,
    )

    print(f"Source rows : {len(new_data)}")
    print(f"Final rows  : {len(final_data)}")
    print("SCD Type 1 completed")
    print("=" * 50)


if __name__ == "__main__":
    run_scd_type1()