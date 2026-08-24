import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
    DATASETS,
    BRONZE_PATH,
    create_project_folders
)


STAGING_PATH = PROJECT_ROOT / "data" / "staging"

STRUCTURED_STAGING = STAGING_PATH / "structured"
SEMI_STRUCTURED_STAGING = STAGING_PATH / "semi_structured"
UNSTRUCTURED_STAGING = STAGING_PATH / "unstructured"

SEMI_STRUCTURED_BRONZE = BRONZE_PATH / "semi_structured"
UNSTRUCTURED_BRONZE = BRONZE_PATH / "unstructured"


def save_bronze(source_file, output_file, data_type):
    """Read Staging Parquet and save it in Bronze."""

    dataframe = pd.read_parquet(
        source_file,
        engine="pyarrow"
    )

    dataframe["bronze_load_timestamp"] = (
        datetime.now(timezone.utc)
    )

    dataframe["bronze_data_type"] = data_type

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    dataframe.to_parquet(
        output_file,
        index=False,
        engine="pyarrow"
    )

    print(
        f"{output_file.stem}: "
        f"{len(dataframe)} rows saved"
    )


def process_structured_data():
    """Process the original nine Olist datasets."""

    for dataset_name, filename in DATASETS.items():
        source_name = (
            Path(filename).stem + ".parquet"
        )

        source_file = (
            STRUCTURED_STAGING /
            source_name
        )

        output_file = (
            BRONZE_PATH /
            f"{dataset_name}.parquet"
        )

        if not source_file.exists():
            raise FileNotFoundError(
                f"Staging file not found: {source_file}"
            )

        save_bronze(
            source_file=source_file,
            output_file=output_file,
            data_type="structured"
        )


def process_extra_data(
    source_folder,
    output_folder,
    data_type
):
    """Process JSON, XML, or TXT derived Parquet files."""

    parquet_files = list(
        source_folder.glob("*.parquet")
    )

    for source_file in parquet_files:
        output_file = (
            output_folder /
            source_file.name
        )

        save_bronze(
            source_file=source_file,
            output_file=output_file,
            data_type=data_type
        )


def run_bronze():
    """Create the complete Bronze layer."""

    start_time = datetime.now()

    create_project_folders()

    print("=" * 60)
    print("BRONZE LAYER STARTED")
    print("=" * 60)

    process_structured_data()

    process_extra_data(
        source_folder=SEMI_STRUCTURED_STAGING,
        output_folder=SEMI_STRUCTURED_BRONZE,
        data_type="semi_structured"
    )

    process_extra_data(
        source_folder=UNSTRUCTURED_STAGING,
        output_folder=UNSTRUCTURED_BRONZE,
        data_type="unstructured"
    )

    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print("BRONZE LAYER COMPLETED")
    print(
        f"Execution time: "
        f"{execution_time:.2f} seconds"
    )
    print("=" * 60)


if __name__ == "__main__":
    run_bronze()