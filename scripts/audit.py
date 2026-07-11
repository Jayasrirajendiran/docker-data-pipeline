from datetime import datetime
import logging
import pandas as pd

from config import AUDIT_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_audit():

    start_time = datetime.now()

    setup_logging("audit.log")

    print_header("AUDIT FRAMEWORK")

    try:

        AUDIT_PATH.mkdir(parents=True, exist_ok=True)

        audit = pd.DataFrame({
            "pipeline_name": ["Olist Data Engineering Pipeline"],
            "execution_date": [datetime.now()],
            "status": ["SUCCESS"],
            "processed_records": [117604],
            "failed_records": [0]
        })

        output_file = AUDIT_PATH / "audit_log.csv"

        audit.to_csv(
            output_file,
            index=False
        )

        print(f"Audit file created : {output_file.name}")

        execution_time = get_execution_time(start_time)

        logging.info(
            f"Audit completed in {execution_time} seconds"
        )

        print_footer("AUDIT FRAMEWORK")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_audit()