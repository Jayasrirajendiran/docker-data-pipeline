from datetime import datetime
import logging
import pandas as pd

from config import SILVER_PATH, GOLD_PATH
from utils import (
    setup_logging,
    print_header,
    print_footer,
    get_execution_time
)


def run_gold():

    start_time = datetime.now()

    setup_logging("gold.log")

    print_header("GOLD LAYER")

    try:

        GOLD_PATH.mkdir(parents=True, exist_ok=True)

        # -------------------------------------------------
        # Read Silver Data
        # -------------------------------------------------

        customers = pd.read_parquet(
            SILVER_PATH / "customers.parquet"
        )

        orders = pd.read_parquet(
            SILVER_PATH / "orders.parquet"
        )

        order_items = pd.read_parquet(
            SILVER_PATH / "order_items.parquet"
        )

        products = pd.read_parquet(
            SILVER_PATH / "products.parquet"
        )

        print("Silver datasets loaded successfully")

        # -------------------------------------------------
        # Remove Metadata Columns
        # -------------------------------------------------

        metadata_cols = [
            "source_file",
            "batch_id",
            "load_timestamp"
        ]

        for df in [
            customers,
            orders,
            order_items,
            products
        ]:
            df.drop(
                columns=metadata_cols,
                errors="ignore",
                inplace=True
            )

        # -------------------------------------------------
        # Business Dataset
        # -------------------------------------------------

        business = (
            orders
            .merge(
                customers,
                on="customer_id",
                how="left"
            )
            .merge(
                order_items,
                on="order_id",
                how="left"
            )
            .merge(
                products,
                on="product_id",
                how="left"
            )
        )

        business.to_parquet(
            GOLD_PATH / "business_dataset.parquet",
            index=False
        )

        print(
            f"Business Dataset Created : {len(business)} rows"
        )

        # -------------------------------------------------
        # Customer Summary
        # -------------------------------------------------

        customer_summary = (
            business
            .groupby("customer_id")
            .agg(
                total_orders=("order_id", "nunique"),
                total_spend=("price", "sum"),
                avg_order_value=("price", "mean")
            )
            .reset_index()
        )

        customer_summary.to_parquet(
            GOLD_PATH / "customer_summary.parquet",
            index=False
        )

        print("Customer Summary Created")

        # -------------------------------------------------
        # Product Summary
        # -------------------------------------------------

        product_summary = (
            business
            .groupby("product_category_name")
            .agg(
                total_products=("product_id", "count"),
                total_sales=("price", "sum")
            )
            .reset_index()
        )

        product_summary.to_parquet(
            GOLD_PATH / "product_summary.parquet",
            index=False
        )

        print("Product Summary Created")

        # -------------------------------------------------
        # Monthly Summary
        # -------------------------------------------------

        business["order_purchase_timestamp"] = pd.to_datetime(
            business["order_purchase_timestamp"],
            errors="coerce"
        )

        monthly_summary = (
            business
            .groupby(
                business["order_purchase_timestamp"].dt.to_period("M")
            )
            .agg(
                total_orders=("order_id", "nunique"),
                total_sales=("price", "sum")
            )
            .reset_index()
        )

        monthly_summary[
            "order_purchase_timestamp"
        ] = monthly_summary[
            "order_purchase_timestamp"
        ].astype(str)

        monthly_summary.to_parquet(
            GOLD_PATH / "monthly_summary.parquet",
            index=False
        )

        print("Monthly Summary Created")

        # -------------------------------------------------
        # Customer Rank
        # -------------------------------------------------

        customer_rank = (
            business
            .groupby("customer_id")
            .agg(
                total_orders=("order_id", "nunique"),
                total_spend=("price", "sum")
            )
            .reset_index()
        )

        customer_rank["customer_rank"] = (
            customer_rank["total_spend"]
            .rank(
                ascending=False,
                method="dense"
            )
        )

        customer_rank = customer_rank.sort_values(
            by="customer_rank"
        )

        customer_rank.to_parquet(
            GOLD_PATH / "customer_rank.parquet",
            index=False
        )

        print("Customer Rank Created")

        execution_time = get_execution_time(
            start_time
        )

        logging.info(
            f"Gold completed in {execution_time} seconds"
        )

        print_footer("GOLD LAYER")

    except Exception as e:

        logging.exception(e)
        raise


if __name__ == "__main__":
    run_gold()