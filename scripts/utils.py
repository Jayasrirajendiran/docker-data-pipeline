import logging
from datetime import datetime
from pathlib import Path
import uuid


def setup_logging(log_name="pipeline.log"):
    """
    Configure logging for pipeline scripts.
    """

    log_folder = Path("/opt/airflow/logs")
    log_folder.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=log_folder / log_name,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        force=True
    )


def print_header(title):
    """
    Print formatted pipeline header.
    """

    print("\n" + "=" * 70)
    print(title.center(70))
    print("=" * 70)


def print_footer(title):
    """
    Print formatted pipeline footer.
    """

    print("=" * 70)
    print(f"{title} COMPLETED".center(70))
    print("=" * 70 + "\n")


def get_batch_id():
    """
    Generate unique Batch ID.
    """

    return str(uuid.uuid4())


def get_execution_time(start_time):
    """
    Return execution time in seconds.
    """

    return round(
        (datetime.now() - start_time).total_seconds(),
        2
    )