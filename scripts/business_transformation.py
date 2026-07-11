import pandas as pd
import os
from datetime import datetime

SILVER_PATH = "/opt/airflow/data/silver"
OUTPUT_PATH = "/opt/airflow/data/business_transformation"


def business_transformation():

    print("=" * 70)
    print("BUSINESS TRANSFORMATION STARTED")
    print("=" * 70)

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Read Silver data
    orders = pd.read_parquet(f"{SILVER_PATH}/orders.parquet")
    order_items = pd.read_parquet(f"{SILVER_PATH}/order_items.parquet")
    customers = pd.read_parquet(f"{SILVER_PATH}/customers.parquet")
    products = pd.read_parquet(f"{SILVER_PATH}/products.parquet")
    categories = pd.read_parquet(
        f"{SILVER_PATH}/category_translation.parquet"
    )

    # Remove audit columns from lookup tables
    audit_cols = [
        "source_file",
        "load_timestamp",
        "batch_id"
    ]

    customers = customers.drop(columns=audit_cols, errors="ignore")
    order_items = order_items.drop(columns=audit_cols, errors="ignore")
    products = products.drop(columns=audit_cols, errors="ignore")
    categories = categories.drop(columns=audit_cols, errors="ignore")

    # -------------------------------
    # 1. JOIN TRANSFORMATION
    # -------------------------------

    df = orders.merge(
        customers,
        on="customer_id",
        how="left"
    )

    df = df.merge(
        order_items,
        on="order_id",
        how="left"
    )

    df = df.merge(
        products,
        on="product_id",
        how="left"
    )

    # -------------------------------
    # 2. LOOKUP MAPPING
    # -------------------------------

    if "product_category_name" in df.columns:
        df = df.merge(
            categories,
            on="product_category_name",
            how="left"
        )

    # -------------------------------
    # 3. DERIVED COLUMNS
    # -------------------------------

    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"]
    )

    df["order_delivered_customer_date"] = pd.to_datetime(
        df["order_delivered_customer_date"]
    )

    df["delivery_days"] = (
        df["order_delivered_customer_date"]
        - df["order_purchase_timestamp"]
    ).dt.days

    df["purchase_year"] = (
        df["order_purchase_timestamp"].dt.year
    )

    df["purchase_month"] = (
        df["order_purchase_timestamp"].dt.month
    )

    # -------------------------------
    # 4. AGGREGATION
    # -------------------------------

    customer_summary = (
        df.groupby("customer_id")
        .agg(
            total_orders=("order_id", "nunique"),
            total_spend=("price", "sum"),
            avg_order_value=("price", "mean")
        )
        .reset_index()
    )

    # -------------------------------
    # 5. WINDOW FUNCTION
    # -------------------------------

    customer_summary["customer_rank"] = (
        customer_summary["total_spend"]
        .rank(
            ascending=False,
            method="dense"
        )
    )

    # Merge customer summary
    final_df = df.merge(
        customer_summary,
        on="customer_id",
        how="left"
    )

    final_df["transformation_timestamp"] = datetime.now()

    output_file = (
        f"{OUTPUT_PATH}/business_dataset.parquet"
    )

    final_df.to_parquet(output_file, index=False)

    print(f"Business transformation completed: {output_file}")
    print("=" * 70)


if __name__ == "__main__":
    business_transformation()