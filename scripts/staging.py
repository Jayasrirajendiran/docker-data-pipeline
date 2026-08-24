import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PROJECT_PATH = Path(__file__).resolve().parents[1]

INGESTION_PATH = PROJECT_PATH / "data" / "ingestion"
STAGING_PATH = PROJECT_PATH / "data" / "staging"

STRUCTURED_INPUT = INGESTION_PATH / "structured"
SEMI_STRUCTURED_INPUT = INGESTION_PATH / "semi_structured"
UNSTRUCTURED_INPUT = INGESTION_PATH / "unstructured"


def add_metadata(
    df,
    source_file,
    source_format,
    data_type,
    run_id
):
    """Add ingestion metadata."""

    df = df.copy()

    df["source_file"] = source_file
    df["source_format"] = source_format
    df["data_type"] = data_type
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    df["pipeline_run_id"] = run_id

    return df


def save_to_parquet(
    df,
    file_path,
    output_folder,
    source_format,
    data_type,
    run_id
):
    """Add metadata and save a dataset as Parquet."""

    output_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    df = add_metadata(
        df=df,
        source_file=file_path.name,
        source_format=source_format,
        data_type=data_type,
        run_id=run_id
    )

    output_file = (
        output_folder /
        f"{file_path.stem}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False
    )

    print(
        f"{source_format}: "
        f"{file_path.name} → {output_file.name} "
        f"| Rows: {len(df)}"
    )


def stage_csv_files(run_id):
    """Convert structured CSV files to Parquet."""

    output_folder = STAGING_PATH / "structured"
    csv_files = list(
        STRUCTURED_INPUT.glob("*.csv")
    )

    if not csv_files:
        print("CSV: No files found")
        return

    for file_path in csv_files:
        df = pd.read_csv(
            file_path,
            low_memory=False
        )

        save_to_parquet(
            df=df,
            file_path=file_path,
            output_folder=output_folder,
            source_format="CSV",
            data_type="structured",
            run_id=run_id
        )


def stage_json_files(run_id):
    """Convert semi-structured JSON files to Parquet."""

    output_folder = (
        STAGING_PATH /
        "semi_structured"
    )

    json_files = list(
        SEMI_STRUCTURED_INPUT.glob("*.json")
    )

    if not json_files:
        print("JSON: No files found")
        return

    for file_path in json_files:
        df = pd.read_json(file_path)

        save_to_parquet(
            df=df,
            file_path=file_path,
            output_folder=output_folder,
            source_format="JSON",
            data_type="semi_structured",
            run_id=run_id
        )


def stage_xml_files(run_id):
    """Convert semi-structured XML files to Parquet."""

    output_folder = (
        STAGING_PATH /
        "semi_structured"
    )

    xml_files = list(
        SEMI_STRUCTURED_INPUT.glob("*.xml")
    )

    if not xml_files:
        print("XML: No files found")
        return

    for file_path in xml_files:
        tree = ET.parse(file_path)
        root = tree.getroot()

        records = []

        for category in root.findall(
            "product_category"
        ):
            records.append({
                "product_category_name":
                    category.findtext(
                        "product_category_name"
                    ),
                "product_category_name_english":
                    category.findtext(
                        "product_category_name_english"
                    )
            })

        df = pd.DataFrame(records)

        save_to_parquet(
            df=df,
            file_path=file_path,
            output_folder=output_folder,
            source_format="XML",
            data_type="semi_structured",
            run_id=run_id
        )


def stage_text_files(run_id):
    """Convert unstructured TXT files to Parquet."""

    output_folder = (
        STAGING_PATH /
        "unstructured"
    )

    text_files = list(
        UNSTRUCTURED_INPUT.glob("*.txt")
    )

    if not text_files:
        print("TXT: No files found")
        return

    for file_path in text_files:
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:
            comments = [
                line.strip()
                for line in file
                if line.strip()
            ]

        df = pd.DataFrame({
            "review_text": comments
        })

        save_to_parquet(
            df=df,
            file_path=file_path,
            output_folder=output_folder,
            source_format="TXT",
            data_type="unstructured",
            run_id=run_id
        )


def run_staging(run_id=None):
    """Run multi-format Staging sequentially."""

    if run_id is None:
        run_id = (
            f"STAGE_{uuid.uuid4().hex[:8]}"
        )

    print("=" * 60)
    print("MULTI-FORMAT STAGING STARTED")
    print(f"Run ID: {run_id}")
    print("=" * 60)

    stage_csv_files(run_id)
    stage_json_files(run_id)
    stage_xml_files(run_id)
    stage_text_files(run_id)

    print("=" * 60)
    print("MULTI-FORMAT STAGING COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_staging()