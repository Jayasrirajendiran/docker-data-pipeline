import sys
from pathlib import Path
from datetime import datetime
from uuid import uuid4

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    AUDIT_PATH,
    create_project_folders,
)


AUDIT_FILE = AUDIT_PATH / "audit_log.csv"


def run_audit(
    status,
    start_time,
    error_message="",
    run_id=None,
):
    """
    Add one final audit record after pipeline execution.
    """

    create_project_folders()

    end_time = datetime.now()

    # Remove timezone information when required
    if hasattr(start_time, "tzinfo"):
        start_time = start_time.replace(
            tzinfo=None
        )

    execution_seconds = (
        end_time - start_time
    ).total_seconds()

    if run_id is None:
        run_id = (
            f"RUN_{datetime.now():%Y%m%d_%H%M%S}_"
            f"{uuid4().hex[:5]}"
        )

    new_record = pd.DataFrame([{
        "run_id": str(run_id),
        "pipeline_name": "Olist Data Pipeline",
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "status": status,
        "execution_seconds": round(
            execution_seconds,
            2,
        ),
        "error_message": str(error_message),
    }])

    if AUDIT_FILE.exists():

        old_audit = pd.read_csv(
            AUDIT_FILE,
            dtype=str,
        ).fillna("")

        final_audit = pd.concat(
            [old_audit, new_record],
            ignore_index=True,
        )

    else:

        final_audit = new_record

    final_audit.to_csv(
        AUDIT_FILE,
        index=False,
    )

    print(
        f"Audit recorded: "
        f"{run_id} - {status}"
    )


if __name__ == "__main__":

    print(
        "Audit runs automatically at "
        "the end of the pipeline"
    )