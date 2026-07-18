import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    RAW_PATH,
    INGESTION_PATH,
    BRONZE_PATH,
    VALIDATION_PATH,
    SILVER_PATH,
    SCD_TYPE1_PATH,
    SCD_TYPE2_PATH,
    GOLD_PATH,
    METADATA_PATH,
    create_project_folders,
)


METADATA_FILE = (
    METADATA_PATH / "pipeline_metadata.csv"
)


def initialize_metadata():
    """
    Create an empty metadata file before the pipeline starts.
    """

    create_project_folders()

    columns = [
        "dataset_name",
        "layer",
        "file_name",
        "file_format",
        "record_count",
        "updated_at",
    ]

    pd.DataFrame(columns=columns).to_csv(
        METADATA_FILE,
        index=False,
    )

    print("Metadata initialized")


def get_record_count(file_path):

    if file_path.suffix == ".csv":

        return len(
            pd.read_csv(
                file_path,
                low_memory=False,
            )
        )

    if file_path.suffix == ".parquet":

        return len(
            pd.read_parquet(file_path)
        )

    return 0


def finalize_metadata():
    """
    Collect information about pipeline output files.
    """

    layers = {
        "raw": RAW_PATH,
        "ingestion": INGESTION_PATH,
        "bronze": BRONZE_PATH,
        "validation": VALIDATION_PATH,
        "silver": SILVER_PATH,
        "scd_type1": SCD_TYPE1_PATH,
        "scd_type2": SCD_TYPE2_PATH,
        "gold": GOLD_PATH,
    }

    metadata_records = []

    for layer_name, folder_path in layers.items():

        for file_path in folder_path.iterdir():

            if file_path.suffix not in [
                ".csv",
                ".parquet",
            ]:
                continue

            metadata_records.append({
                "dataset_name": file_path.stem,
                "layer": layer_name,
                "file_name": file_path.name,
                "file_format": (
                    file_path.suffix.replace(".", "")
                ),
                "record_count": get_record_count(
                    file_path
                ),
                "updated_at": datetime.now(),
            })

    metadata = pd.DataFrame(metadata_records)

    metadata.to_csv(
        METADATA_FILE,
        index=False,
    )

    print(
        f"Metadata finalized: "
        f"{len(metadata)} files recorded"
    )


if __name__ == "__main__":

    initialize_metadata()
    finalize_metadata()