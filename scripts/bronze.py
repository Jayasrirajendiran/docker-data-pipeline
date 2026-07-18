import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    DATASETS,
    INGESTION_PATH,
    BRONZE_PATH,
    create_project_folders,
)


def run_bronze():

    start_time = datetime.now()

    create_project_folders()

    print("=" * 60)
    print("BRONZE LAYER STARTED")
    print("=" * 60)

    for dataset_name, filename in DATASETS.items():

        source_file = INGESTION_PATH / filename
        output_file = BRONZE_PATH / f"{dataset_name}.parquet"

        if not source_file.exists():
            raise FileNotFoundError(
                f"Ingested file not found: {source_file}"
            )

        # Read source values as text
        dataframe = pd.read_csv(
            source_file,
            dtype="string",
            low_memory=False,
        )

        # Add technical columns
        dataframe["load_timestamp"] = datetime.now()
        dataframe["source_file"] = filename

        dataframe.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
        )

        print(
            f"{dataset_name}: "
            f"{len(dataframe)} rows saved"
        )

    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print("BRONZE LAYER COMPLETED")
    print(f"Execution time: {execution_time:.2f} seconds")
    print("=" * 60)


if __name__ == "__main__":
    run_bronze()