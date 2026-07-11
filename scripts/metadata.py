from datetime import datetime
import logging
import pandas as pd

from config import METADATA_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_metadata():

    start_time = datetime.now()

    setup_logging("metadata.log")

    print_header("METADATA FRAMEWORK")

    try:

        METADATA_PATH.mkdir(parents=True, exist_ok=True)

        metadata = pd.DataFrame({
            "pipeline_name": ["Olist Data Engineering Pipeline"],
            "execution_date": [datetime.now()],
            "source_layer": ["Raw"],
            "target_layer": ["Gold"],
            "pipeline_status": ["SUCCESS"],
            "total_files_processed": [9],
            "created_by": ["Airflow"]
        })

        output_file = METADATA_PATH / "pipeline_metadata.csv"

        metadata.to_csv(
            output_file,
            index=False
        )

        print(f"Metadata file created : {output_file.name}")

        execution_time = get_execution_time(start_time)

        logging.info(
            f"Metadata completed in {execution_time} seconds"
        )

        print_footer("METADATA FRAMEWORK")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_metadata()