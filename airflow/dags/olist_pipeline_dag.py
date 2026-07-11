from datetime import datetime
import sys

# Add scripts folder to Python path
sys.path.insert(0, "/opt/airflow/scripts")

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion import run_ingestion
from bronze import run_bronze
from validation import run_validation
from silver import run_silver
from business_transformation import business_transformation
from scd_type1 import run_scd_type1
from scd_type2 import run_scd_type2
from gold import run_gold
from incremental import run_incremental
from load_postgres import run_load_postgres
from audit import run_audit
from metadata import run_metadata


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2026, 7, 1),
}


with DAG(
    dag_id="olist_data_pipeline",
    default_args=default_args,
    description="End-to-End Olist Data Engineering Pipeline",
    schedule=None,
    catchup=False,
    tags=["olist", "etl", "data-engineering"],
) as dag:

    ingestion_task = PythonOperator(
        task_id="ingestion",
        python_callable=run_ingestion,
    )

    bronze_task = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze,
    )

    validation_task = PythonOperator(
        task_id="validation",
        python_callable=run_validation,
    )

    silver_task = PythonOperator(
        task_id="silver",
        python_callable=run_silver,
    )

    business_transformation_task = PythonOperator(
        task_id="business_transformation",
        python_callable=business_transformation,
    )

    scd_type1_task = PythonOperator(
        task_id="scd_type1",
        python_callable=run_scd_type1,
    )

    scd_type2_task = PythonOperator(
        task_id="scd_type2",
        python_callable=run_scd_type2,
    )

    gold_task = PythonOperator(
        task_id="gold",
        python_callable=run_gold,
    )

    incremental_task = PythonOperator(
        task_id="incremental",
        python_callable=run_incremental,
    )

    postgres_task = PythonOperator(
        task_id="load_postgres",
        python_callable=run_load_postgres,
    )

    audit_task = PythonOperator(
        task_id="audit",
        python_callable=run_audit,
    )

    metadata_task = PythonOperator(
        task_id="metadata",
        python_callable=run_metadata,
    )

    (
        ingestion_task
        >> bronze_task
        >> validation_task
        >> silver_task
        >> business_transformation_task
        >> scd_type1_task
        >> scd_type2_task
        >> gold_task
        >> incremental_task
        >> postgres_task
        >> audit_task
        >> metadata_task
    )