from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BUSINESS_PATH = (
    PROJECT_ROOT /
    "data" /
    "business_transformation"
)

SCD1_PATH = (
    PROJECT_ROOT /
    "data" /
    "scd_type1"
)

SOURCE_FILE = (
    BUSINESS_PATH /
    "customer_dimension_input.parquet"
)

OUTPUT_FILE = (
    SCD1_PATH /
    "customer_dimension.parquet"
)


def run_scd_type1():
    """Maintain the latest customer information."""

    print("=" * 60)
    print("SCD TYPE 1 STARTED")
    print("=" * 60)

    SCD1_PATH.mkdir(
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

    incoming = incoming.drop(
        columns=["transformation_timestamp"],
        errors="ignore"
    )

    incoming = incoming.drop_duplicates(
        subset=["customer_id"],
        keep="last"
    )

    current_time = datetime.now()

    if not OUTPUT_FILE.exists():
        incoming.insert(
            0,
            "customer_key",
            range(1, len(incoming) + 1)
        )

        incoming["scd_type"] = 1
        incoming["created_at"] = current_time
        incoming["last_updated_at"] = current_time

        final_data = incoming
        new_records = len(incoming)
        updated_records = 0

    else:
        existing = pd.read_parquet(
            OUTPUT_FILE
        )

        # Add keys to an older dimension if necessary.
        if "customer_key" not in existing.columns:
            existing.insert(
                0,
                "customer_key",
                range(1, len(existing) + 1)
            )

        if "created_at" not in existing.columns:
            existing["created_at"] = current_time

        if "last_updated_at" not in existing.columns:
            existing["last_updated_at"] = current_time

        key_mapping = existing.set_index(
            "customer_id"
        )["customer_key"]

        incoming["customer_key"] = (
            incoming["customer_id"]
            .map(key_mapping)
        )

        new_mask = incoming[
            "customer_key"
        ].isna()

        new_records = int(new_mask.sum())

        next_key = (
            int(existing["customer_key"].max())
            + 1
        )

        incoming.loc[
            new_mask,
            "customer_key"
        ] = range(
            next_key,
            next_key + new_records
        )

        incoming["customer_key"] = (
            incoming["customer_key"].astype(int)
        )

        # Identify changed customer values.
        tracked_columns = [
            column
            for column in incoming.columns
            if column not in [
                "customer_key",
                "customer_id"
            ]
        ]

        comparison = incoming.merge(
            existing[
                ["customer_id"] + tracked_columns
            ],
            on="customer_id",
            how="inner",
            suffixes=("_new", "_old")
        )

        changed = pd.Series(
            False,
            index=comparison.index
        )

        for column in tracked_columns:
            changed |= (
                comparison[f"{column}_new"]
                .fillna("")
                .astype(str)
                !=
                comparison[f"{column}_old"]
                .fillna("")
                .astype(str)
            )

        changed_ids = comparison.loc[
            changed,
            "customer_id"
        ]

        updated_records = len(changed_ids)

        created_mapping = existing.set_index(
            "customer_id"
        )["created_at"]

        updated_mapping = existing.set_index(
            "customer_id"
        )["last_updated_at"]

        incoming["created_at"] = (
            incoming["customer_id"]
            .map(created_mapping)
            .fillna(current_time)
        )

        incoming["last_updated_at"] = (
            incoming["customer_id"]
            .map(updated_mapping)
            .fillna(current_time)
        )

        incoming.loc[
            incoming["customer_id"].isin(
                changed_ids
            ),
            "last_updated_at"
        ] = current_time

        incoming["scd_type"] = 1

        # Retain customers missing from the latest input.
        old_missing = existing[
            ~existing["customer_id"].isin(
                incoming["customer_id"]
            )
        ]

        final_data = pd.concat(
            [old_missing, incoming],
            ignore_index=True
        )

    columns = [
        "customer_key"
    ] + [
        column
        for column in final_data.columns
        if column != "customer_key"
    ]

    final_data = final_data[columns]

    final_data.to_parquet(
        OUTPUT_FILE,
        index=False
    )

    print(f"Source records  : {len(incoming)}")
    print(f"New records     : {new_records}")
    print(f"Updated records : {updated_records}")
    print(f"Final records   : {len(final_data)}")
    print(f"Saved           : {OUTPUT_FILE.name}")

    print("=" * 60)
    print("SCD TYPE 1 COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_scd_type1()