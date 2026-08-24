import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_PATH = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_PATH / "data" / "raw"

JSON_PATH = RAW_PATH / "json"
TEXT_PATH = RAW_PATH / "text"
XML_PATH = RAW_PATH / "xml"


# Create folders when they do not exist
JSON_PATH.mkdir(parents=True, exist_ok=True)
TEXT_PATH.mkdir(parents=True, exist_ok=True)
XML_PATH.mkdir(parents=True, exist_ok=True)


# =========================================================
# FIND SOURCE FILE
# =========================================================

def find_file(possible_names):
    """
    Find the first available file from a list of filenames.
    """

    for filename in possible_names:
        file_path = RAW_PATH / filename

        if file_path.exists():
            return file_path

    raise FileNotFoundError(
        f"None of these files were found: {possible_names}"
    )


# =========================================================
# CSV TO JSON
# =========================================================

def create_order_events_json():
    """
    Convert selected Orders CSV columns into JSON.
    """

    orders_file = find_file([
        "orders.csv",
        "olist_orders_dataset.csv"
    ])

    orders = pd.read_csv(orders_file)

    required_columns = [
        "order_id",
        "order_status",
        "order_purchase_timestamp"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in orders.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing Orders columns: {missing_columns}"
        )

    order_events = orders[required_columns].copy()

    order_events = order_events.rename(
        columns={
            "order_purchase_timestamp":
                "event_timestamp"
        }
    )

    order_events["event_type"] = "order_status"

    records = order_events.to_dict(
        orient="records"
    )

    output_file = (
        JSON_PATH /
        "order_events.json"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            records,
            file,
            indent=2,
            ensure_ascii=False
        )

    print("-" * 60)
    print("CSV TO JSON COMPLETED")
    print(f"Source  : {orders_file.name}")
    print(f"Output  : {output_file}")
    print(f"Records : {len(records)}")


# =========================================================
# CSV TO TXT
# =========================================================

def create_customer_reviews_text():
    """
    Convert review comments from CSV into a text file.
    """

    reviews_file = find_file([
        "reviews.csv",
        "order_reviews.csv",
        "olist_order_reviews_dataset.csv"
    ])

    reviews = pd.read_csv(reviews_file)

    review_column = "review_comment_message"

    if review_column not in reviews.columns:
        raise ValueError(
            f"Missing Reviews column: {review_column}"
        )

    comments = (
        reviews[review_column]
        .dropna()
        .astype(str)
        .str.strip()
    )

    comments = comments[
        comments != ""
    ]

    output_file = (
        TEXT_PATH /
        "customer_reviews.txt"
    )

    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:
        for comment in comments:
            clean_comment = (
                comment
                .replace("\n", " ")
                .replace("\r", " ")
            )

            file.write(
                clean_comment + "\n"
            )

    print("-" * 60)
    print("CSV TO TXT COMPLETED")
    print(f"Source  : {reviews_file.name}")
    print(f"Output  : {output_file}")
    print(f"Records : {len(comments)}")


# =========================================================
# CSV TO XML
# =========================================================

def create_product_categories_xml():
    """
    Convert product-category CSV into XML.
    """

    category_file = find_file([
        "product_category_name_translation.csv"
    ])

    category_df = pd.read_csv(category_file)

    required_columns = [
        "product_category_name",
        "product_category_name_english"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in category_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing Category columns: {missing_columns}"
        )

    root = ET.Element(
        "product_categories"
    )

    for _, row in category_df.iterrows():
        category_element = ET.SubElement(
            root,
            "product_category"
        )

        portuguese_element = ET.SubElement(
            category_element,
            "product_category_name"
        )

        english_element = ET.SubElement(
            category_element,
            "product_category_name_english"
        )

        portuguese_value = row[
            "product_category_name"
        ]

        english_value = row[
            "product_category_name_english"
        ]

        portuguese_element.text = (
            ""
            if pd.isna(portuguese_value)
            else str(portuguese_value)
        )

        english_element.text = (
            ""
            if pd.isna(english_value)
            else str(english_value)
        )

    tree = ET.ElementTree(root)

    ET.indent(
        tree,
        space="  "
    )

    output_file = (
        XML_PATH /
        "product_categories.xml"
    )

    tree.write(
        output_file,
        encoding="utf-8",
        xml_declaration=True
    )

    print("-" * 60)
    print("CSV TO XML COMPLETED")
    print(f"Source  : {category_file.name}")
    print(f"Output  : {output_file}")
    print(f"Records : {len(category_df)}")


# =========================================================
# MAIN FUNCTION
# =========================================================

def run_multiple_datatypes():
    """
    Create JSON, TXT and XML datasets from Olist CSV files.
    """

    print("=" * 60)
    print("MULTIPLE DATA TYPES PROCESS STARTED")
    print("=" * 60)

    try:
        create_order_events_json()
        create_customer_reviews_text()
        create_product_categories_xml()

        print("=" * 60)
        print("MULTIPLE DATA TYPES CREATED SUCCESSFULLY")
        print("=" * 60)

        print("Formats created:")
        print("1. JSON - Order events")
        print("2. TXT  - Customer reviews")
        print("3. XML  - Product categories")

    except Exception as error:
        print("=" * 60)
        print("MULTIPLE DATA TYPES PROCESS FAILED")
        print(f"Error: {error}")
        print("=" * 60)

        raise


# =========================================================
# SCRIPT EXECUTION
# =========================================================

if __name__ == "__main__":
    run_multiple_datatypes()