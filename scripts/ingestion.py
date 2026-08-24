import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
    DATASETS,
    RAW_PATH,
    INGESTION_PATH,
    METADATA_PATH,
    create_project_folders
)


STRUCTURED_PATH = (
    INGESTION_PATH /
    "structured"
)

SEMI_STRUCTURED_PATH = (
    INGESTION_PATH /
    "semi_structured"
)

UNSTRUCTURED_PATH = (
    INGESTION_PATH /
    "unstructured"
)

WATERMARK_FILE = (
    METADATA_PATH /
    "watermark.json"
)


def create_folders():
    """Create all ingestion folders."""

    folders = [
        STRUCTURED_PATH,
        SEMI_STRUCTURED_PATH,
        UNSTRUCTURED_PATH,
        METADATA_PATH
    ]

    for folder in folders:
        folder.mkdir(
            parents=True,
            exist_ok=True
        )


def get_watermark():
    """Read the last processed order timestamp."""

    if not WATERMARK_FILE.exists():
        return None

    with open(
        WATERMARK_FILE,
        "r",
        encoding="utf-8"
    ) as file:
        data = json.load(file)

    return data.get(
        "last_order_timestamp"
    )


def save_watermark(timestamp):
    """Save the latest processed order timestamp."""

    with open(
        WATERMARK_FILE,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            {
                "last_order_timestamp":
                    timestamp
            },
            file,
            indent=4
        )


def ingest_orders(
    source_file,
    output_file
):
    """Run full or incremental Orders ingestion."""

    orders = pd.read_csv(
        source_file,
        low_memory=False
    )

    date_column = (
        "order_purchase_timestamp"
    )

    if date_column not in orders.columns:
        raise ValueError(
            f"Missing column: {date_column}"
        )

    orders[date_column] = pd.to_datetime(
        orders[date_column],
        errors="coerce"
    )

    orders = orders.dropna(
        subset=[date_column]
    )

    watermark = get_watermark()

    # First execution or missing output file.
    if (
        watermark is None
        or not output_file.exists()
    ):
        new_orders = orders
        load_type = "FULL LOAD"
        mode = "w"
        header = True

    # Later executions.
    else:
        new_orders = orders[
            orders[date_column]
            > pd.to_datetime(watermark)
        ]

        load_type = "INCREMENTAL LOAD"
        mode = "a"
        header = False

    if new_orders.empty:
        print(
            "orders: 0 new records "
            "using INCREMENTAL LOAD"
        )

        return

    new_orders.to_csv(
        output_file,
        mode=mode,
        header=header,
        index=False
    )

    latest_timestamp = (
        new_orders[date_column]
        .max()
        .isoformat()
    )

    save_watermark(
        latest_timestamp
    )

    print(
        f"orders: {len(new_orders)} records "
        f"using {load_type}"
    )


def ingest_csv_files():
    """Ingest the nine structured CSV datasets."""

    for dataset_name, filename in DATASETS.items():
        source_file = (
            RAW_PATH /
            filename
        )

        output_file = (
            STRUCTURED_PATH /
            filename
        )

        if not source_file.exists():
            raise FileNotFoundError(
                f"Missing source: {source_file}"
            )

        if dataset_name == "orders":
            ingest_orders(
                source_file,
                output_file
            )

        else:
            # copyfile avoids Windows/Docker
            # timestamp permission errors.
            shutil.copyfile(
                source_file,
                output_file
            )

            print(
                f"{dataset_name}: BATCH LOAD"
            )


def ingest_other_formats():
    """Ingest JSON, XML and TXT datasets."""

    formats = [
        (
            RAW_PATH / "json",
            "*.json",
            SEMI_STRUCTURED_PATH,
            "JSON"
        ),
        (
            RAW_PATH / "xml",
            "*.xml",
            SEMI_STRUCTURED_PATH,
            "XML"
        ),
        (
            RAW_PATH / "text",
            "*.txt",
            UNSTRUCTURED_PATH,
            "TXT"
        )
    ]

    for (
        source_folder,
        pattern,
        destination,
        format_name
    ) in formats:

        source_files = list(
            source_folder.glob(pattern)
        )

        if not source_files:
            print(
                f"{format_name}: "
                f"No files found"
            )

            continue

        for source_file in source_files:
            output_file = (
                destination /
                source_file.name
            )

            # Copy only file contents.
            shutil.copyfile(
                source_file,
                output_file
            )

            print(
                f"{format_name}: "
                f"{source_file.name} BATCH LOAD"
            )


def run_ingestion():
    """Run batch and incremental ingestion."""

    start_time = datetime.now()

    create_project_folders()
    create_folders()

    print("=" * 60)
    print("DATA INGESTION STARTED")
    print("=" * 60)

    try:
        ingest_csv_files()
        ingest_other_formats()

        duration = (
            datetime.now() - start_time
        ).total_seconds()

        print("=" * 60)
        print("DATA INGESTION COMPLETED")
        print(
            f"Execution time: "
            f"{duration:.2f} seconds"
        )
        print("=" * 60)

    except Exception as error:
        print("=" * 60)
        print("DATA INGESTION FAILED")
        print(f"Error: {error}")
        print("=" * 60)

        raise


if __name__ == "__main__":
    run_ingestion()