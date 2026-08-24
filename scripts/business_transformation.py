import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
    SILVER_PATH,
    create_project_folders
)


BUSINESS_PATH = (
    PROJECT_ROOT /
    "data" /
    "business_transformation"
)


TECHNICAL_COLUMNS = [
    "load_timestamp",
    "batch_id",
    "source_file",
    "source_format",
    "data_type",
    "ingestion_timestamp",
    "pipeline_run_id",
    "bronze_load_timestamp",
    "bronze_data_type",
    "silver_timestamp"
]


def read_silver(name):
    """Read Silver data without technical columns."""

    file_path = SILVER_PATH / f"{name}.parquet"

    if not file_path.exists():
        raise FileNotFoundError(
            f"Missing Silver file: {file_path}"
        )

    return pd.read_parquet(file_path).drop(
        columns=TECHNICAL_COLUMNS,
        errors="ignore"
    )


def run_business_transformation():
    """Create business and SCD input datasets."""

    start_time = datetime.now()

    create_project_folders()
    BUSINESS_PATH.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("BUSINESS TRANSFORMATION STARTED")
    print("=" * 60)

    customers = read_silver("customers")
    orders = read_silver("orders")
    items = read_silver("order_items")
    payments = read_silver("payments")
    reviews = read_silver("reviews")
    products = read_silver("products")
    sellers = read_silver("sellers")
    translation = read_silver(
        "category_translation"
    )

    # Customer input for SCD 0 and SCD 1.
    customer_input = customers.drop_duplicates(
        subset=["customer_id"]
    )

    customer_input.to_parquet(
        BUSINESS_PATH /
        "customer_dimension_input.parquet",
        index=False
    )

    # Product input for SCD 2.
    product_input = products.merge(
        translation,
        on="product_category_name",
        how="left"
    ).drop_duplicates(
        subset=["product_id"]
    )

    product_input.to_parquet(
        BUSINESS_PATH /
        "product_dimension_input.parquet",
        index=False
    )

    # Payment summary.
    payment_summary = payments.groupby(
        "order_id",
        as_index=False
    ).agg(
        total_payment=(
            "payment_value",
            "sum"
        ),
        payment_count=(
            "payment_sequential",
            "count"
        )
    )

    # Review summary.
    reviews["review_score"] = pd.to_numeric(
        reviews["review_score"],
        errors="coerce"
    )

    review_summary = reviews.groupby(
        "order_id",
        as_index=False
    ).agg(
        average_review_score=(
            "review_score",
            "mean"
        ),
        review_count=(
            "review_id",
            "count"
        )
    )

    # Join business datasets.
    business_data = (
        orders
        .merge(
            customers,
            on="customer_id",
            how="left"
        )
        .merge(
            items,
            on="order_id",
            how="left"
        )
        .merge(
            products,
            on="product_id",
            how="left"
        )
        .merge(
            sellers,
            on="seller_id",
            how="left"
        )
        .merge(
            translation,
            on="product_category_name",
            how="left"
        )
        .merge(
            payment_summary,
            on="order_id",
            how="left"
        )
        .merge(
            review_summary,
            on="order_id",
            how="left"
        )
    )

    # Derived business columns.
    purchase_date = business_data[
        "order_purchase_timestamp"
    ]

    business_data["order_year"] = (
        purchase_date.dt.year
    )

    business_data["order_month"] = (
        purchase_date.dt.month
    )

    business_data["order_quarter"] = (
        purchase_date.dt.quarter
    )

    business_data["delivery_days"] = (
        business_data[
            "order_delivered_customer_date"
        ]
        - purchase_date
    ).dt.days

    business_data["sales_amount"] = (
        business_data["price"].fillna(0)
    )

    business_data["item_total"] = (
        business_data["price"].fillna(0)
        + business_data[
            "freight_value"
        ].fillna(0)
    )

    business_data[
        "customer_order_number"
    ] = (
        business_data
        .groupby("customer_unique_id")[
            "order_purchase_timestamp"
        ]
        .rank(method="dense")
    )

    business_data[
        "transformation_timestamp"
    ] = datetime.now()

    business_data.to_parquet(
        BUSINESS_PATH /
        "business_dataset.parquet",
        index=False
    )

    duration = (
        datetime.now() - start_time
    ).total_seconds()

    print(f"Business rows : {len(business_data)}")
    print(f"Customer rows : {len(customer_input)}")
    print(f"Product rows  : {len(product_input)}")
    print(f"Execution time: {duration:.2f} seconds")

    print("=" * 60)
    print("BUSINESS TRANSFORMATION COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_business_transformation()