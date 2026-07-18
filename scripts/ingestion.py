import sys
import json
import shutil
from pathlib import Path
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    DATASETS,
    RAW_PATH,
    INGESTION_PATH,
    METADATA_PATH,
    create_project_folders,
)


WATERMARK_FILE = METADATA_PATH / "watermark.json"


def get_watermark():

    if not WATERMARK_FILE.exists():
        return None

    with open(
        WATERMARK_FILE,
        "r",
        encoding="utf-8",
    ) as file:
        data = json.load(file)

    return data.get("last_order_timestamp")


def save_watermark(timestamp):

    with open(
        WATERMARK_FILE,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            {
                "last_order_timestamp": timestamp
            },
            file,
            indent=4,
        )


def run_ingestion():

    start_time = datetime.now()

    create_project_folders()

    print("=" * 60)
    print("DATA INGESTION STARTED")
    print("=" * 60)

    for dataset_name, filename in DATASETS.items():

        source_file = RAW_PATH / filename
        output_file = INGESTION_PATH / filename

        if not source_file.exists():
            raise FileNotFoundError(
                f"Source file not found: {source_file}"
            )

        # Incremental processing for orders
        if dataset_name == "orders":

            orders = pd.read_csv(
                source_file,
                low_memory=False,
            )

            orders["order_purchase_timestamp"] = (
                pd.to_datetime(
                    orders[
                        "order_purchase_timestamp"
                    ],
                    errors="coerce",
                )
            )

            watermark = get_watermark()

            if watermark is None:

                new_orders = orders
                load_type = "FULL LOAD"
                mode = "w"
                header = True

            else:

                new_orders = orders[
                    orders[
                        "order_purchase_timestamp"
                    ]
                    > pd.to_datetime(watermark)
                ]

                load_type = "INCREMENTAL LOAD"
                mode = "a"
                header = False

            if new_orders.empty:

                print(
                    "orders: 0 new records found"
                )

            else:

                new_orders.to_csv(
                    output_file,
                    mode=mode,
                    header=header,
                    index=False,
                )

                latest_timestamp = (
                    new_orders[
                        "order_purchase_timestamp"
                    ]
                    .max()
                    .isoformat()
                )

                save_watermark(
                    latest_timestamp
                )

                print(
                    f"orders: {len(new_orders)} "
                    f"records using {load_type}"
                )

        # Full snapshots for other datasets
        else:

            shutil.copyfile(
                source_file,
                output_file,
            )

            print(
                f"{dataset_name}: "
                f"Full snapshot copied"
            )

    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print("DATA INGESTION COMPLETED")
    print(
        f"Execution time: "
        f"{execution_time:.2f} seconds"
    )
    print("=" * 60)


if __name__ == "__main__":
    run_ingestion()