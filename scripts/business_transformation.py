import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


from scripts.config import (
    SILVER_PATH,
    GOLD_PATH,
    create_project_folders,
)


TECHNICAL_COLUMNS = [
    "load_timestamp",
    "batch_id",
    "source_file",
    "silver_timestamp",
]


def read_silver(filename):
    """
    Read a Silver file and remove technical columns
    before joining.
    """

    dataframe = pd.read_parquet(
        SILVER_PATH / filename
    )

    return dataframe.drop(
        columns=TECHNICAL_COLUMNS,
        errors="ignore",
    )


def run_business_transformation():

    create_project_folders()

    print("=" * 60)
    print("BUSINESS TRANSFORMATION STARTED")
    print("=" * 60)

    # Read cleaned Silver datasets
    customers = read_silver("customers.parquet")
    orders = read_silver("orders.parquet")
    order_items = read_silver("order_items.parquet")
    payments = read_silver("payments.parquet")
    products = read_silver("products.parquet")
    sellers = read_silver("sellers.parquet")

    translation = read_silver(
        "category_translation.parquet"
    )

    # Aggregate payments by order
    payment_summary = (
        payments
        .groupby("order_id", as_index=False)
        .agg(
            total_payment=(
                "payment_value",
                "sum",
            ),
            payment_count=(
                "payment_sequential",
                "count",
            ),
        )
    )

    # Join orders and customers
    business_data = orders.merge(
        customers,
        on="customer_id",
        how="left",
    )

    # Join order items
    business_data = business_data.merge(
        order_items,
        on="order_id",
        how="left",
    )

    # Join products
    business_data = business_data.merge(
        products,
        on="product_id",
        how="left",
    )

    # Join sellers
    business_data = business_data.merge(
        sellers,
        on="seller_id",
        how="left",
        suffixes=("_customer", "_seller"),
    )

    # Lookup category English name
    business_data = business_data.merge(
        translation,
        on="product_category_name",
        how="left",
    )

    # Join payment summary
    business_data = business_data.merge(
        payment_summary,
        on="order_id",
        how="left",
    )

    # Derived columns
    business_data["order_year"] = (
        business_data[
            "order_purchase_timestamp"
        ].dt.year
    )

    business_data["order_month"] = (
        business_data[
            "order_purchase_timestamp"
        ].dt.month
    )

    business_data["delivery_days"] = (
        business_data[
            "order_delivered_customer_date"
        ]
        - business_data[
            "order_purchase_timestamp"
        ]
    ).dt.days

    business_data["item_total"] = (
        business_data["price"].fillna(0)
        + business_data["freight_value"].fillna(0)
    )

    # Window function
    business_data["customer_order_number"] = (
        business_data
        .groupby("customer_unique_id")[
            "order_purchase_timestamp"
        ]
        .rank(method="dense")
    )

    # Add final metadata
    business_data[
        "transformation_timestamp"
    ] = datetime.now()

    output_file = (
        GOLD_PATH / "business_dataset.parquet"
    )

    business_data.to_parquet(
        output_file,
        index=False,
    )

    print(f"Rows created    : {len(business_data)}")
    print(f"Columns created : {len(business_data.columns)}")
    print(f"Saved           : {output_file.name}")

    print("=" * 60)
    print("BUSINESS TRANSFORMATION COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_business_transformation()