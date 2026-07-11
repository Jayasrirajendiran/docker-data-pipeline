from pathlib import Path
import shutil
from datetime import datetime
import logging

from config import RAW_PATH, INGESTION_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_batch_id,
    get_execution_time
)


def run_ingestion():

    start_time = datetime.now()

    setup_logging("ingestion.log")

    print_header("INGESTION LAYER")

    batch_id = get_batch_id()

    try:

        INGESTION_PATH.mkdir(parents=True, exist_ok=True)

        csv_files = list(RAW_PATH.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(
                f"No CSV files found in {RAW_PATH}"
            )

        logging.info(f"Batch ID : {batch_id}")

        for file in csv_files:

            destination = INGESTION_PATH / file.name

            shutil.copy(file, destination)

            print(f"Copied : {file.name}")

            logging.info(f"Copied {file.name}")

        execution_time = get_execution_time(start_time)

        logging.info(
            f"Ingestion completed in {execution_time} seconds"
        )

        print_footer("INGESTION")

    except Exception as e:

        logging.exception(e)

        raise


if __name__ == "__main__":

    run_ingestion()