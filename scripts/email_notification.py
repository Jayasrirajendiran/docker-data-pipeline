import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

AUDIT_FILE = (
    PROJECT_ROOT
    / "audit"
    / "audit_log.csv"
)


def read_latest_audit():
    """Read the latest pipeline audit record."""

    if not AUDIT_FILE.exists():
        raise FileNotFoundError(
            f"Audit file not found: {AUDIT_FILE}"
        )

    audit = pd.read_csv(
        AUDIT_FILE,
        keep_default_na=False
    )

    if audit.empty:
        raise ValueError(
            "Audit file contains no records"
        )

    return audit.iloc[-1]


def format_integer(value):
    """Format numbers without decimal values."""

    if value == "" or pd.isna(value):
        return 0

    return int(float(value))


def format_duration(value):
    """Format pipeline execution duration."""

    if value == "" or pd.isna(value):
        return "0.00"

    return f"{float(value):.2f}"


def run_email_notification():
    """Send the latest pipeline audit report by email."""

    smtp_host = os.getenv(
        "EMAIL_SMTP_HOST",
        "smtp.gmail.com"
    )

    smtp_port = int(
        os.getenv(
            "EMAIL_SMTP_PORT",
            "587"
        )
    )

    sender = os.getenv(
        "EMAIL_SENDER"
    )

    password = os.getenv(
        "EMAIL_PASSWORD"
    )

    recipient = os.getenv(
        "EMAIL_RECIPIENT"
    )

    if not sender:
        raise ValueError(
            "EMAIL_SENDER is not configured"
        )

    if not password:
        raise ValueError(
            "EMAIL_PASSWORD is not configured"
        )

    if not recipient:
        raise ValueError(
            "EMAIL_RECIPIENT is not configured"
        )

    latest = read_latest_audit()

    run_id = latest.get(
        "run_id",
        "Unknown"
    )

    status = str(
        latest.get(
            "status",
            "UNKNOWN"
        )
    ).upper()

    error_message = (
        latest.get(
            "error_message",
            ""
        )
        or "None"
    )

    execution_seconds = format_duration(
        latest.get(
            "execution_seconds",
            0
        )
    )

    total_assets = format_integer(
        latest.get(
            "total_assets",
            0
        )
    )

    file_assets = format_integer(
        latest.get(
            "file_assets",
            0
        )
    )

    database_tables = format_integer(
        latest.get(
            "database_tables",
            0
        )
    )

    postgresql_rows = format_integer(
        latest.get(
            "postgresql_rows",
            0
        )
    )

    subject = (
        f"Olist Pipeline {status} - {run_id}"
    )

    body = f"""
Olist Data Pipeline Execution Report

Run ID: {run_id}
Status: {status}

Start Time: {latest.get("start_time", "")}
End Time: {latest.get("end_time", "")}
Execution Time: {execution_seconds} seconds

Total Assets: {total_assets}
File Assets: {file_assets}
PostgreSQL Tables: {database_tables}
PostgreSQL Rows: {postgresql_rows}

Error Message: {error_message}

This is an automated notification from the
Olist Data Engineering Pipeline.
""".strip()

    message = EmailMessage()

    message["Subject"] = subject
    message["From"] = sender
    message["To"] = recipient

    message.set_content(
        body
    )

    print("=" * 60)
    print("EMAIL NOTIFICATION STARTED")
    print("=" * 60)

    with smtplib.SMTP(
        smtp_host,
        smtp_port,
        timeout=30
    ) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()

        smtp.login(
            sender,
            password
        )

        smtp.send_message(
            message
        )

    print(f"Run ID    : {run_id}")
    print(f"Status    : {status}")
    print(f"Recipient : {recipient}")
    print("Email sent successfully")
    print("=" * 60)


if __name__ == "__main__":
    run_email_notification()