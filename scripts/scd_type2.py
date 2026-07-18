import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    SILVER_PATH,
    SCD_TYPE2_PATH,
    create_project_folders,
)


TRACKED_COLUMNS = [
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]


def run_scd_type2():

    create_project_folders()

    print("=" * 50)
    print("SCD TYPE 2 STARTED")
    print("=" * 50)

    source_file = SILVER_PATH / "products.parquet"

    output_file = (
        SCD_TYPE2_PATH
        / "product_dimension_history.parquet"
    )

    if not source_file.exists():
        raise FileNotFoundError(
            "products.parquet not found in Silver"
        )

    new_data = pd.read_parquet(source_file)
    current_time = datetime.now()

    # First execution
    if not output_file.exists():

        new_data["effective_from"] = current_time
        new_data["effective_to"] = pd.NaT
        new_data["is_current"] = True

        final_data = new_data

    # Later executions
    else:

        history_data = pd.read_parquet(output_file)

        current_data = history_data[
            history_data["is_current"] == True
        ].set_index("product_id")

        new_versions = []

        for _, new_row in new_data.iterrows():

            product_id = new_row["product_id"]

            # New product
            if product_id not in current_data.index:

                new_versions.append(
                    new_row.to_dict()
                )

                continue

            old_row = current_data.loc[product_id]

            # Check whether tracked values changed
            changed = any(
                str(old_row[column])
                != str(new_row[column])
                for column in TRACKED_COLUMNS
            )

            if changed:

                # Close the old record
                history_data.loc[
                    (
                        history_data["product_id"]
                        == product_id
                    )
                    & (
                        history_data["is_current"]
                        == True
                    ),
                    "effective_to",
                ] = current_time

                history_data.loc[
                    (
                        history_data["product_id"]
                        == product_id
                    )
                    & (
                        history_data["is_current"]
                        == True
                    ),
                    "is_current",
                ] = False

                new_versions.append(
                    new_row.to_dict()
                )

        if new_versions:

            new_version_data = pd.DataFrame(
                new_versions
            )

            new_version_data["effective_from"] = (
                current_time
            )
            new_version_data["effective_to"] = pd.NaT
            new_version_data["is_current"] = True

            final_data = pd.concat(
                [history_data, new_version_data],
                ignore_index=True,
            )

        else:

            final_data = history_data

    final_data.to_parquet(
        output_file,
        index=False,
    )

    print(f"Source rows  : {len(new_data)}")
    print(f"History rows : {len(final_data)}")
    print("SCD Type 2 completed")
    print("=" * 50)


if __name__ == "__main__":
    run_scd_type2()