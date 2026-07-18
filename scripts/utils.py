import logging
from datetime import datetime

from scripts.config import LOG_PATH


def setup_logging(log_filename):
    """
    Configure file and terminal logging.
    """

    LOG_PATH.mkdir(parents=True, exist_ok=True)

    log_file = LOG_PATH / log_filename

    logger = logging.getLogger(log_filename)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def print_header(stage_name):
    print("\n" + "=" * 60)
    print(f"{stage_name} STARTED")
    print("=" * 60)


def print_footer(stage_name, start_time):
    execution_time = (
        datetime.now() - start_time
    ).total_seconds()

    print("=" * 60)
    print(f"{stage_name} COMPLETED")
    print(f"Execution time: {execution_time:.2f} seconds")
    print("=" * 60)