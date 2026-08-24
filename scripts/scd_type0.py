from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BUSINESS_PATH = (
    PROJECT_ROOT /
    "data" /
    "business_transformation"
)

SCD0_PATH = (
    PROJECT_ROOT /
    "data" /
    "scd_type0"
)

SOURCE_FILE = (
    BUSINESS_PATH /
    "customer_dimension_input.parquet"
)

OUTPUT_FILE = (
    SCD0_PATH /
    "customer_original_dimension.parquet"
)


def run_scd_type0():
    """Preserve original customer values permanently."""

    print("=" * 60)
    print("SCD TYPE 0 STARTED")
    print("=" * 60)

    SCD0_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Missing input file: {SOURCE_FILE}"
        )

    incoming = pd.read_parquet(
        SOURCE_FILE
    )

    incoming = incoming.drop_duplicates(
        subset=["customer_id"]
    )

    if not OUTPUT_FILE.exists():
        incoming.insert(
            0,
            "customer_key",
            range(1, len(incoming) + 1)
        )

        incoming["scd_type"] = 0
        incoming["created_at"] = datetime.now()

        result = incoming
        new_records = len(incoming)

    else:
        existing = pd.read_parquet(
            OUTPUT_FILE
        )

        new_customers = incoming[
            ~incoming["customer_id"].isin(
                existing["customer_id"]
            )
        ].copy()

        if new_customers.empty:
            result = existing
            new_records = 0
        else:
            next_key = (
                existing["customer_key"].max()
                + 1
            )

            new_customers.insert(
                0,
                "customer_key",
                range(
                    next_key,
                    next_key + len(new_customers)
                )
            )

            new_customers["scd_type"] = 0
            new_customers["created_at"] = (
                datetime.now()
            )

            result = pd.concat(
                [existing, new_customers],
                ignore_index=True
            )

            new_records = len(new_customers)

    result.to_parquet(
        OUTPUT_FILE,
        index=False
    )

    print(f"Total records : {len(result)}")
    print(f"New records   : {new_records}")
    print("Existing customer values were not updated")
    print(f"Saved         : {OUTPUT_FILE.name}")

    print("=" * 60)
    print("SCD TYPE 0 COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_scd_type0()