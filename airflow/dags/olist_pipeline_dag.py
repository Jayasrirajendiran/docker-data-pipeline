import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


sys.path.insert(0, "/opt/airflow")


from scripts.multiple_datatypes import (
    create_order_events_json,
    create_customer_reviews_text,
    create_product_categories_xml
)
from scripts.ingestion import run_ingestion
from scripts.staging import run_staging
from scripts.bronze import run_bronze
from scripts.validation import run_validation
from scripts.silver import run_silver
from scripts.business_transformation import (
    run_business_transformation
)
from scripts.scd_type0 import run_scd_type0
from scripts.scd_type1 import run_scd_type1
from scripts.scd_type2 import run_scd_type2
from scripts.gold import run_gold
from scripts.load_postgres import run_load_postgres
from scripts.metadata import run_metadata
from scripts.audit import run_audit
from scripts.email_notification import (
    run_email_notification
)


def pipeline_failure_callback(context):
    """Record and email a failed pipeline result."""

    dag_run = context.get("dag_run")
    task_instance = context.get(
        "task_instance"
    )
    exception = context.get(
        "exception"
    )

    failed_task = (
        task_instance.task_id
        if task_instance
        else "Unknown"
    )

    start_time = (
        dag_run.start_date
        if dag_run
        else datetime.now()
    )

    run_id = (
        dag_run.run_id
        if dag_run
        else None
    )

    error_message = (
        f"Failed task: {failed_task}. "
        f"Error: {exception}"
    )

    run_audit(
        status="FAILED",
        start_time=start_time,
        run_id=run_id,
        error_message=error_message
    )

    try:
        run_email_notification()

    except Exception as email_error:
        print(
            "Failure email could not be sent: "
            f"{email_error}"
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0
}


with DAG(
    dag_id="olist_data_pipeline",
    description="Olist Data Pipeline",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["olist"],
    on_failure_callback=pipeline_failure_callback
) as dag:

    csv_task = EmptyOperator(
        task_id="csv"
    )

    json_task = PythonOperator(
        task_id="json",
        python_callable=create_order_events_json
    )

    xml_task = PythonOperator(
        task_id="xml",
        python_callable=create_product_categories_xml
    )

    text_task = PythonOperator(
        task_id="text",
        python_callable=create_customer_reviews_text
    )

    ingestion_task = PythonOperator(
        task_id="ingestion",
        python_callable=run_ingestion
    )

    staging_task = PythonOperator(
        task_id="staging",
        python_callable=run_staging
    )

    bronze_task = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze
    )

    validation_task = PythonOperator(
        task_id="validation",
        python_callable=run_validation
    )

    silver_task = PythonOperator(
        task_id="silver",
        python_callable=run_silver
    )

    transformation_task = PythonOperator(
        task_id="transformation",
        python_callable=run_business_transformation
    )

    scd0_task = PythonOperator(
        task_id="scd0",
        python_callable=run_scd_type0
    )

    scd1_task = PythonOperator(
        task_id="scd1",
        python_callable=run_scd_type1
    )

    scd2_task = PythonOperator(
        task_id="scd2",
        python_callable=run_scd_type2
    )

    gold_task = PythonOperator(
        task_id="gold",
        python_callable=run_gold
    )

    postgres_task = PythonOperator(
        task_id="postgres",
        python_callable=run_load_postgres
    )

    metadata_task = PythonOperator(
        task_id="metadata",
        python_callable=run_metadata
    )

    audit_task = PythonOperator(
        task_id="audit",
        python_callable=run_audit
    )

    email_task = PythonOperator(
        task_id="email",
        python_callable=run_email_notification
    )

    csv_task >> [
        json_task,
        xml_task,
        text_task
    ]

    [
        json_task,
        xml_task,
        text_task
    ] >> ingestion_task

    (
        ingestion_task
        >> staging_task
        >> bronze_task
        >> validation_task
        >> silver_task
        >> transformation_task
        >> scd0_task
        >> scd1_task
        >> scd2_task
        >> gold_task
        >> postgres_task
        >> metadata_task
        >> audit_task
        >> email_task
    )