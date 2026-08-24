import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.config import (
    GOLD_PATH,
    SQLALCHEMY_DATABASE_URI
)


OLAP_SCHEMA = "olist_olap"


DATASETS = {
    # Dimensions
    "dim_customer_original":
        GOLD_PATH / "dim_customer_original.parquet",

    "dim_customer_current":
        GOLD_PATH / "dim_customer_current.parquet",

    "dim_product_history":
        GOLD_PATH / "dim_product_history.parquet",

    "dim_seller":
        GOLD_PATH / "dim_seller.parquet",

    "dim_date":
        GOLD_PATH / "dim_date.parquet",

    # Fact
    "fact_sales":
        GOLD_PATH / "fact_sales.parquet",

    # Reporting summaries
    "customer_summary":
        GOLD_PATH / "customer_summary.parquet",

    "product_summary":
        GOLD_PATH / "product_summary.parquet",

    "monthly_summary":
        GOLD_PATH / "monthly_summary.parquet",

    "seller_summary":
        GOLD_PATH / "seller_summary.parquet"
}


KEY_COLUMNS = [
    "sales_key",
    "customer_key",
    "product_key",
    "seller_key",
    "date_key"
]


def recreate_olap_schema(engine):
    """
    Remove the previous OLAP schema and create a clean one.
    """

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                DROP SCHEMA IF EXISTS
                {OLAP_SCHEMA} CASCADE
                """
            )
        )

        connection.execute(
            text(
                f"""
                CREATE SCHEMA
                {OLAP_SCHEMA}
                """
            )
        )

    print(
        f"PostgreSQL schema recreated: "
        f"{OLAP_SCHEMA}"
    )


def standardize_key_datatypes(dataframe):
    """Convert Star Schema keys to integer datatype."""

    dataframe = dataframe.copy()

    for column in KEY_COLUMNS:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(
                dataframe[column],
                errors="coerce"
            ).astype("Int64")

    return dataframe


def load_gold_tables(engine):
    """Load every Gold dataset into PostgreSQL."""

    for table_name, file_path in DATASETS.items():
        if not file_path.exists():
            raise FileNotFoundError(
                f"Missing Gold file: {file_path}"
            )

        dataframe = pd.read_parquet(
            file_path
        )

        dataframe = standardize_key_datatypes(
            dataframe
        )

        dataframe.to_sql(
            name=table_name,
            con=engine,
            schema=OLAP_SCHEMA,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi"
        )

        print(
            f"{OLAP_SCHEMA}.{table_name}: "
            f"{len(dataframe)} rows loaded"
        )


def create_primary_keys(engine):
    """Create primary keys for dimensions and fact."""

    commands = [
        """
        ALTER TABLE olist_olap.dim_customer_original
        ADD PRIMARY KEY (customer_key)
        """,

        """
        ALTER TABLE olist_olap.dim_customer_current
        ADD PRIMARY KEY (customer_key)
        """,

        """
        ALTER TABLE olist_olap.dim_product_history
        ADD PRIMARY KEY (product_key)
        """,

        """
        ALTER TABLE olist_olap.dim_seller
        ADD PRIMARY KEY (seller_key)
        """,

        """
        ALTER TABLE olist_olap.dim_date
        ADD PRIMARY KEY (date_key)
        """,

        """
        ALTER TABLE olist_olap.fact_sales
        ADD PRIMARY KEY (sales_key)
        """
    ]

    with engine.begin() as connection:
        for command in commands:
            connection.execute(
                text(command)
            )

    print("Primary keys created")


def create_foreign_keys(engine):
    """Connect Fact Sales to its dimensions."""

    commands = [
        """
        ALTER TABLE olist_olap.fact_sales
        ADD CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_key)
        REFERENCES olist_olap.dim_customer_current
        (customer_key)
        """,

        """
        ALTER TABLE olist_olap.fact_sales
        ADD CONSTRAINT fk_fact_product
        FOREIGN KEY (product_key)
        REFERENCES olist_olap.dim_product_history
        (product_key)
        """,

        """
        ALTER TABLE olist_olap.fact_sales
        ADD CONSTRAINT fk_fact_seller
        FOREIGN KEY (seller_key)
        REFERENCES olist_olap.dim_seller
        (seller_key)
        """,

        """
        ALTER TABLE olist_olap.fact_sales
        ADD CONSTRAINT fk_fact_date
        FOREIGN KEY (date_key)
        REFERENCES olist_olap.dim_date
        (date_key)
        """
    ]

    with engine.begin() as connection:
        for command in commands:
            connection.execute(
                text(command)
            )

    print("Foreign keys created")


def verify_olap_tables(engine):
    """Display the final OLAP table row counts."""

    table_query = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema_name
        ORDER BY table_name
        """
    )

    with engine.connect() as connection:
        tables = connection.execute(
            table_query,
            {
                "schema_name": OLAP_SCHEMA
            }
        ).fetchall()

        print("-" * 60)
        print("OLAP TABLE VERIFICATION")
        print("-" * 60)

        for table in tables:
            table_name = table[0]

            row_count = connection.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {OLAP_SCHEMA}.{table_name}
                    """
                )
            ).scalar()

            print(
                f"{table_name}: "
                f"{row_count} rows"
            )


def run_load_postgres():
    """Rebuild and load the PostgreSQL OLAP schema."""

    print("=" * 60)
    print("POSTGRESQL LOAD STARTED")
    print("=" * 60)

    engine = create_engine(
        SQLALCHEMY_DATABASE_URI
    )

    try:
        recreate_olap_schema(engine)

        load_gold_tables(engine)

        create_primary_keys(engine)

        create_foreign_keys(engine)

        verify_olap_tables(engine)

        print("=" * 60)
        print("POSTGRESQL LOAD COMPLETED")
        print("=" * 60)

    except Exception as error:
        print("=" * 60)
        print("POSTGRESQL LOAD FAILED")
        print(f"Error: {error}")
        print("=" * 60)

        raise

    finally:
        engine.dispose()


if __name__ == "__main__":
    run_load_postgres()