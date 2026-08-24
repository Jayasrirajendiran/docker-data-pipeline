from datetime import datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BUSINESS_FILE = (
    PROJECT_ROOT
    / "data"
    / "business_transformation"
    / "business_dataset.parquet"
)

SCD0_FILE = (
    PROJECT_ROOT
    / "data"
    / "scd_type0"
    / "customer_original_dimension.parquet"
)

SCD1_FILE = (
    PROJECT_ROOT
    / "data"
    / "scd_type1"
    / "customer_dimension.parquet"
)

SCD2_FILE = (
    PROJECT_ROOT
    / "data"
    / "scd_type2"
    / "product_dimension_history.parquet"
)

GOLD_PATH = PROJECT_ROOT / "data" / "gold"


def read_required(file_path):
    """Read a required Parquet file."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Missing file: {file_path}"
        )

    return pd.read_parquet(file_path)


def run_gold():
    """Create Gold fact, dimension and summary datasets."""

    start_time = datetime.now()

    GOLD_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    print("=" * 60)
    print("GOLD LAYER STARTED")
    print("=" * 60)

    business = read_required(BUSINESS_FILE)
    customer_original = read_required(SCD0_FILE)
    customer_current = read_required(SCD1_FILE)
    product_history = read_required(SCD2_FILE)

    # =====================================================
    # CUSTOMER DIMENSIONS
    # =====================================================

    customer_original.to_parquet(
        GOLD_PATH /
        "dim_customer_original.parquet",
        index=False
    )

    customer_current.to_parquet(
        GOLD_PATH /
        "dim_customer_current.parquet",
        index=False
    )

    # =====================================================
    # PRODUCT DIMENSION
    # =====================================================

    product_history.to_parquet(
        GOLD_PATH /
        "dim_product_history.parquet",
        index=False
    )

    current_products = (
        product_history[
            product_history["is_current"] == True
        ]
        .drop_duplicates(
            subset=["product_id"],
            keep="last"
        )
    )

    # =====================================================
    # SELLER DIMENSION
    # =====================================================

    seller_columns = [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    ]

    dim_seller = (
        business[seller_columns]
        .drop_duplicates(subset=["seller_id"])
        .dropna(subset=["seller_id"])
        .reset_index(drop=True)
    )

    dim_seller.insert(
        0,
        "seller_key",
        range(1, len(dim_seller) + 1)
    )

    dim_seller.to_parquet(
        GOLD_PATH / "dim_seller.parquet",
        index=False
    )

    # =====================================================
    # DATE DIMENSION
    # =====================================================

    purchase_date = pd.to_datetime(
        business["order_purchase_timestamp"],
        errors="coerce"
    )

    valid_dates = purchase_date.dropna()

    date_range = pd.date_range(
        start=valid_dates.min().normalize(),
        end=valid_dates.max().normalize(),
        freq="D"
    )

    dim_date = pd.DataFrame({
        "full_date": date_range
    })

    dim_date["date_key"] = (
        dim_date["full_date"]
        .dt.strftime("%Y%m%d")
        .astype(int)
    )

    dim_date["day"] = (
        dim_date["full_date"].dt.day
    )

    dim_date["month"] = (
        dim_date["full_date"].dt.month
    )

    dim_date["month_name"] = (
        dim_date["full_date"].dt.month_name()
    )

    dim_date["quarter"] = (
        dim_date["full_date"].dt.quarter
    )

    dim_date["year"] = (
        dim_date["full_date"].dt.year
    )

    dim_date = dim_date[
        [
            "date_key",
            "full_date",
            "day",
            "month",
            "month_name",
            "quarter",
            "year"
        ]
    ]

    dim_date.to_parquet(
        GOLD_PATH / "dim_date.parquet",
        index=False
    )

    # =====================================================
    # FACT SALES
    # =====================================================

    customer_mapping = (
        customer_current
        .drop_duplicates("customer_id")
        .set_index("customer_id")[
            "customer_key"
        ]
    )

    product_mapping = (
        current_products
        .set_index("product_id")[
            "product_key"
        ]
    )

    seller_mapping = (
        dim_seller
        .set_index("seller_id")[
            "seller_key"
        ]
    )

    fact_sales = business.copy()

    fact_sales["customer_key"] = (
        fact_sales["customer_id"]
        .map(customer_mapping)
    )

    fact_sales["product_key"] = (
        fact_sales["product_id"]
        .map(product_mapping)
    )

    fact_sales["seller_key"] = (
        fact_sales["seller_id"]
        .map(seller_mapping)
    )

    fact_sales["date_key"] = (
        purchase_date
        .dt.strftime("%Y%m%d")
    )

    fact_sales["date_key"] = pd.to_numeric(
        fact_sales["date_key"],
        errors="coerce"
    )

    # Prevent total payment from being counted repeatedly
    # when one order contains multiple items.
    fact_sales["items_in_order"] = (
        fact_sales
        .groupby("order_id")["order_item_id"]
        .transform("count")
        .replace(0, 1)
    )

    fact_sales["allocated_payment"] = (
        fact_sales["total_payment"].fillna(0)
        / fact_sales["items_in_order"]
    )

    fact_sales.insert(
        0,
        "sales_key",
        range(1, len(fact_sales) + 1)
    )

    fact_columns = [
        "sales_key",
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "seller_key",
        "date_key",
        "order_status",
        "price",
        "freight_value",
        "item_total",
        "allocated_payment",
        "delivery_days",
        "average_review_score"
    ]

    fact_sales = fact_sales[
        fact_columns
    ]

    fact_sales["gold_timestamp"] = (
        datetime.now()
    )

    fact_sales.to_parquet(
        GOLD_PATH / "fact_sales.parquet",
        index=False
    )

    # =====================================================
    # SUMMARY TABLES
    # =====================================================

    customer_summary = (
        business
        .groupby(
            [
                "customer_unique_id",
                "customer_city"
            ],
            as_index=False
        )
        .agg(
            total_orders=(
                "order_id",
                "nunique"
            ),
            total_items=(
                "order_item_id",
                "count"
            ),
            total_spent=(
                "item_total",
                "sum"
            )
        )
    )

    customer_summary["customer_rank"] = (
        customer_summary["total_spent"]
        .rank(
            method="dense",
            ascending=False
        )
        .astype(int)
    )

    product_summary = (
        business
        .groupby(
            [
                "product_id",
                "product_category_name_english"
            ],
            as_index=False
        )
        .agg(
            total_orders=(
                "order_id",
                "nunique"
            ),
            total_units=(
                "order_item_id",
                "count"
            ),
            total_sales=(
                "price",
                "sum"
            )
        )
    )

    monthly_summary = (
        business
        .groupby(
            [
                "order_year",
                "order_month"
            ],
            as_index=False
        )
        .agg(
            total_orders=(
                "order_id",
                "nunique"
            ),
            total_customers=(
                "customer_unique_id",
                "nunique"
            ),
            total_revenue=(
                "item_total",
                "sum"
            )
        )
    )

    seller_summary = (
        business
        .groupby(
            [
                "seller_id",
                "seller_city"
            ],
            as_index=False
        )
        .agg(
            total_orders=(
                "order_id",
                "nunique"
            ),
            total_items=(
                "order_item_id",
                "count"
            ),
            total_sales=(
                "price",
                "sum"
            )
        )
    )

    summaries = {
        "customer_summary": customer_summary,
        "product_summary": product_summary,
        "monthly_summary": monthly_summary,
        "seller_summary": seller_summary
    }

    for name, dataframe in summaries.items():
        dataframe.to_parquet(
            GOLD_PATH / f"{name}.parquet",
            index=False
        )

        print(
            f"{name}: {len(dataframe)} rows"
        )

    duration = (
        datetime.now() - start_time
    ).total_seconds()

    print(f"Fact sales       : {len(fact_sales)} rows")
    print(f"Customer SCD 0   : {len(customer_original)} rows")
    print(f"Customer SCD 1   : {len(customer_current)} rows")
    print(f"Product SCD 2    : {len(product_history)} rows")
    print(f"Seller dimension : {len(dim_seller)} rows")
    print(f"Date dimension   : {len(dim_date)} rows")
    print(f"Execution time   : {duration:.2f} seconds")

    print("=" * 60)
    print("GOLD LAYER COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_gold()