from datetime import datetime
from pathlib import Path

import pandas as pd


# =========================================================
# PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_FILE = (
    PROJECT_ROOT
    / "data"
    / "business_transformation"
    / "product_dimension_input.parquet"
)

SCD2_PATH = (
    PROJECT_ROOT
    / "data"
    / "scd_type2"
)

OUTPUT_FILE = (
    SCD2_PATH
    / "product_dimension_history.parquet"
)


# Columns monitored for product changes
TRACKED_COLUMNS = [
    "product_category_name",
    "product_category_name_english",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]


# Original Olist dataset uses the spelling "lenght"
NUMERIC_COLUMNS = [
    "product_name_lenght",
    "product_name_length",
    "product_description_lenght",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "product_key",
    "version",
    "scd_type"
]


TEXT_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_category_name_english"
]


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def clean_value(value):
    """Convert a value into a safe comparison value."""

    if pd.isna(value):
        return ""

    return str(value)


def standardize_datatypes(dataframe):
    """Ensure Parquet-compatible datatypes."""

    dataframe = dataframe.copy()

    for column in NUMERIC_COLUMNS:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="coerce"
            )

    for column in TEXT_COLUMNS:
        if column in dataframe.columns:
            dataframe[column] = (
                dataframe[column]
                .fillna("Unknown")
                .astype("string")
            )

    if "effective_from" in dataframe.columns:
        dataframe["effective_from"] = pd.to_datetime(
            dataframe["effective_from"],
            errors="coerce"
        )

    if "effective_to" in dataframe.columns:
        dataframe["effective_to"] = pd.to_datetime(
            dataframe["effective_to"],
            errors="coerce"
        )

    if "is_current" in dataframe.columns:
        dataframe["is_current"] = (
            dataframe["is_current"]
            .fillna(False)
            .astype(bool)
        )

    return dataframe


# =========================================================
# SCD TYPE 2
# =========================================================

def run_scd_type2():
    """Preserve current and historical product records."""

    print("=" * 60)
    print("SCD TYPE 2 STARTED")
    print("=" * 60)

    SCD2_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Missing source file: {SOURCE_FILE}"
        )

    incoming = pd.read_parquet(
        SOURCE_FILE
    )

    incoming = incoming.drop(
        columns=["transformation_timestamp"],
        errors="ignore"
    )

    incoming = incoming.drop_duplicates(
        subset=["product_id"],
        keep="last"
    )

    # Ensure tracked columns exist.
    for column in TRACKED_COLUMNS:
        if column not in incoming.columns:
            incoming[column] = pd.NA

    incoming = standardize_datatypes(
        incoming
    )

    current_time = datetime.now()

    new_products = 0
    changed_products = 0

    # -----------------------------------------------------
    # FIRST EXECUTION
    # -----------------------------------------------------

    if not OUTPUT_FILE.exists():
        incoming.insert(
            0,
            "product_key",
            range(1, len(incoming) + 1)
        )

        incoming["version"] = 1
        incoming["effective_from"] = current_time
        incoming["effective_to"] = pd.NaT
        incoming["is_current"] = True
        incoming["scd_type"] = 2

        final_data = incoming
        new_products = len(incoming)

    # -----------------------------------------------------
    # LATER EXECUTIONS
    # -----------------------------------------------------

    else:
        history = pd.read_parquet(
            OUTPUT_FILE
        )

        # Support history created by the old script.
        if "product_key" not in history.columns:
            history.insert(
                0,
                "product_key",
                range(1, len(history) + 1)
            )

        if "version" not in history.columns:
            history["version"] = 1

        if "effective_from" not in history.columns:
            history["effective_from"] = current_time

        if "effective_to" not in history.columns:
            history["effective_to"] = pd.NaT

        if "is_current" not in history.columns:
            history["is_current"] = True

        if "scd_type" not in history.columns:
            history["scd_type"] = 2

        for column in TRACKED_COLUMNS:
            if column not in history.columns:
                history[column] = pd.NA

        history = standardize_datatypes(
            history
        )

        current_records = (
            history[
                history["is_current"] == True
            ]
            .drop_duplicates(
                subset=["product_id"],
                keep="last"
            )
            .set_index("product_id")
        )

        next_key = (
            int(history["product_key"].max())
            + 1
        )

        new_versions = []

        for _, new_row in incoming.iterrows():
            product_id = new_row["product_id"]

            # Completely new product
            if product_id not in current_records.index:
                new_record = new_row.to_dict()

                new_record["product_key"] = next_key
                new_record["version"] = 1

                next_key += 1
                new_products += 1

                new_versions.append(
                    new_record
                )

                continue

            old_row = current_records.loc[
                product_id
            ]

            changed = any(
                clean_value(old_row[column])
                != clean_value(new_row[column])
                for column in TRACKED_COLUMNS
            )

            if not changed:
                continue

            # Close the existing current record
            current_mask = (
                history["product_id"].eq(
                    product_id
                )
                &
                history["is_current"].eq(True)
            )

            history.loc[
                current_mask,
                "effective_to"
            ] = current_time

            history.loc[
                current_mask,
                "is_current"
            ] = False

            # Create a new version
            new_record = new_row.to_dict()

            new_record["product_key"] = next_key
            new_record["version"] = (
                int(old_row["version"]) + 1
            )

            next_key += 1
            changed_products += 1

            new_versions.append(
                new_record
            )

        if new_versions:
            new_version_data = pd.DataFrame(
                new_versions
            )

            new_version_data[
                "effective_from"
            ] = current_time

            new_version_data[
                "effective_to"
            ] = pd.NaT

            new_version_data[
                "is_current"
            ] = True

            new_version_data[
                "scd_type"
            ] = 2

            final_data = pd.concat(
                [
                    history,
                    new_version_data
                ],
                ignore_index=True
            )

        else:
            final_data = history

    # -----------------------------------------------------
    # FINAL DATATYPE STANDARDIZATION
    # -----------------------------------------------------

    final_data = standardize_datatypes(
        final_data
    )

    final_data["product_key"] = (
        final_data["product_key"]
        .astype("int64")
    )

    final_data["version"] = (
        final_data["version"]
        .astype("int64")
    )

    final_data["scd_type"] = (
        final_data["scd_type"]
        .astype("int64")
    )

    columns = [
        "product_key"
    ] + [
        column
        for column in final_data.columns
        if column != "product_key"
    ]

    final_data = final_data[columns]

    final_data.to_parquet(
        OUTPUT_FILE,
        index=False,
        engine="pyarrow"
    )

    print(f"Source products  : {len(incoming)}")
    print(f"New products     : {new_products}")
    print(f"Changed products : {changed_products}")
    print(f"History records  : {len(final_data)}")
    print(f"Saved            : {OUTPUT_FILE.name}")

    print("=" * 60)
    print("SCD TYPE 2 COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_scd_type2()