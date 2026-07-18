import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    GOLD_PATH,
    create_project_folders,
)


def run_gold():

    create_project_folders()

    print("=" * 60)
    print("GOLD LAYER STARTED")
    print("=" * 60)

    source_file = (
        GOLD_PATH / "business_dataset.parquet"
    )

    if not source_file.exists():
        raise FileNotFoundError(
            "business_dataset.parquet not found"
        )

    data = pd.read_parquet(source_file)

    # Customer summary
    customer_summary = (
        data
        .groupby(
            [
                "customer_unique_id",
                "customer_city",
            ],
            as_index=False,
        )
        .agg(
            total_orders=("order_id", "nunique"),
            total_items=("order_item_id", "count"),
            total_spent=("item_total", "sum"),
        )
    )

    # Customer ranking
    customer_summary["customer_rank"] = (
        customer_summary["total_spent"]
        .rank(
            method="dense",
            ascending=False,
        )
        .astype(int)
    )

    customer_summary.to_parquet(
        GOLD_PATH / "customer_summary.parquet",
        index=False,
    )

    # Product summary
    product_summary = (
        data
        .groupby(
            [
                "product_id",
                "product_category_name_english",
            ],
            as_index=False,
        )
        .agg(
            total_orders=("order_id", "nunique"),
            total_units=("order_item_id", "count"),
            total_sales=("price", "sum"),
        )
    )

    product_summary.to_parquet(
        GOLD_PATH / "product_summary.parquet",
        index=False,
    )

    # Monthly summary
    monthly_summary = (
        data
        .groupby(
            [
                "order_year",
                "order_month",
            ],
            as_index=False,
        )
        .agg(
            total_orders=("order_id", "nunique"),
            total_customers=(
                "customer_unique_id",
                "nunique",
            ),
            total_revenue=("item_total", "sum"),
        )
    )

    monthly_summary.to_parquet(
        GOLD_PATH / "monthly_summary.parquet",
        index=False,
    )

    # Seller summary
    seller_summary = (
        data
        .groupby(
            [
                "seller_id",
                "seller_city",
            ],
            as_index=False,
        )
        .agg(
            total_orders=("order_id", "nunique"),
            total_items=("order_item_id", "count"),
            total_sales=("price", "sum"),
        )
    )

    seller_summary.to_parquet(
        GOLD_PATH / "seller_summary.parquet",
        index=False,
    )

    print(
        f"Customer summary : {len(customer_summary)} rows"
    )
    print(
        f"Product summary  : {len(product_summary)} rows"
    )
    print(
        f"Monthly summary  : {len(monthly_summary)} rows"
    )
    print(
        f"Seller summary   : {len(seller_summary)} rows"
    )

    print("=" * 60)
    print("GOLD LAYER COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_gold()