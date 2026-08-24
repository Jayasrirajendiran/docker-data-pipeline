import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
    AUDIT_PATH,
    METADATA_PATH,
    create_project_folders
)


AUDIT_FILE = (
    AUDIT_PATH /
    "audit_log.csv"
)

METADATA_FILE = (
    METADATA_PATH /
    "pipeline_metadata.csv"
)


AUDIT_COLUMNS = [
    "run_id",
    "pipeline_name",
    "start_time",
    "end_time",
    "status",
    "execution_seconds",
    "total_assets",
    "file_assets",
    "database_tables",
    "postgresql_rows",
    "error_message"
]


def get_airflow_context():
    """Get DAG information when executed by Airflow."""

    try:
        from airflow.operators.python import (
            get_current_context
        )

        return get_current_context()

    except Exception:
        return {}


def make_timezone_aware(value):
    """Convert a datetime value into UTC."""

    timestamp = pd.Timestamp(value)

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(
            timezone.utc
        )
    else:
        timestamp = timestamp.tz_convert(
            timezone.utc
        )

    return timestamp.to_pydatetime()


def get_metadata_statistics():
    """Read final asset statistics from Metadata."""

    if not METADATA_FILE.exists():
        return {
            "total_assets": 0,
            "file_assets": 0,
            "database_tables": 0,
            "postgresql_rows": 0
        }

    metadata = pd.read_csv(
        METADATA_FILE
    )

    database_data = metadata[
        metadata["storage_type"]
        .eq("database")
    ]

    file_data = metadata[
        metadata["storage_type"]
        .eq("file")
    ]

    postgresql_rows = pd.to_numeric(
        database_data["record_count"],
        errors="coerce"
    ).fillna(0).sum()

    return {
        "total_assets": len(metadata),
        "file_assets": len(file_data),
        "database_tables": len(database_data),
        "postgresql_rows": int(postgresql_rows)
    }


def run_audit(
    status="SUCCESS",
    start_time=None,
    error_message="",
    run_id=None
):
    """Add one final pipeline audit record."""

    create_project_folders()

    context = get_airflow_context()
    dag_run = context.get("dag_run")

    # Use the Airflow DAG start time when available.
    if start_time is None and dag_run is not None:
        start_time = dag_run.start_date

    # Manual execution does not have a DAG start time.
    if start_time is None:
        start_time = datetime.now(
            timezone.utc
        )

    if run_id is None and dag_run is not None:
        run_id = dag_run.run_id

    if run_id is None:
        run_id = (
            f"RUN_{datetime.now():%Y%m%d_%H%M%S}_"
            f"{uuid4().hex[:5]}"
        )

    start_time = make_timezone_aware(
        start_time
    )

    end_time = datetime.now(
        timezone.utc
    )

    execution_seconds = (
        end_time - start_time
    ).total_seconds()

    statistics = get_metadata_statistics()

    new_record = pd.DataFrame([
        {
            "run_id": str(run_id),
            "pipeline_name":
                "Olist Data Pipeline",
            "start_time":
                start_time.isoformat(),
            "end_time":
                end_time.isoformat(),
            "status":
                str(status).upper(),
            "execution_seconds":
                round(execution_seconds, 2),
            "total_assets":
                statistics["total_assets"],
            "file_assets":
                statistics["file_assets"],
            "database_tables":
                statistics["database_tables"],
            "postgresql_rows":
                statistics["postgresql_rows"],
            "error_message":
                str(error_message)
        }
    ])

    if AUDIT_FILE.exists():
        old_audit = pd.read_csv(
            AUDIT_FILE,
            dtype=str
        ).fillna("")

        final_audit = pd.concat(
            [old_audit, new_record],
            ignore_index=True
        )

    else:
        final_audit = new_record

    final_audit = final_audit.reindex(
        columns=AUDIT_COLUMNS,
        fill_value=""
    )

    final_audit.to_csv(
        AUDIT_FILE,
        index=False
    )

    print("=" * 60)
    print("PIPELINE AUDIT COMPLETED")
    print("=" * 60)
    print(f"Run ID          : {run_id}")
    print(f"Status          : {status}")
    print(
        f"Execution time  : "
        f"{execution_seconds:.2f} seconds"
    )
    print(
        f"Total assets    : "
        f"{statistics['total_assets']}"
    )
    print(
        f"Database tables : "
        f"{statistics['database_tables']}"
    )
    print(
        f"PostgreSQL rows : "
        f"{statistics['postgresql_rows']}"
    )
    print(f"Saved           : {AUDIT_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    run_audit()