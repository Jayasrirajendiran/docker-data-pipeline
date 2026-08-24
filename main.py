import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


sys.path.insert(0, "/opt/airflow")


from scripts.ingestion import run_ingestion
from scripts.bronze import run_bronze
from scripts.validation import run_validation
from scripts.silver import run_silver
from scripts.scd_type1 import run_scd_type1
from scripts.scd_type2 import run_scd_type2
from scripts.business_transformation import (
    run_business_transformation,
)
from scripts.gold import run_gold
from scripts.load_postgres import run_load_postgres
from scripts.metadata import finalize_metadata
from scripts.audit import run_audit


def run_airflow_audit(**context):
    """
    Create one final audit record for the Airflow run.
    """

    dag_run = context["dag_run"]

    task_instances = (
        dag_run.get_task_instances()
    )

    failed_tasks = [
        task.task_id
        for task in task_instances
        if task.state in [
            "failed",
            "upstream_failed",
        ]
    ]

    if failed_tasks:

        status = "FAILED"

        error_message = (
            "Failed tasks: "
            + ", ".join(failed_tasks)
        )

    else:

        status = "SUCCESS"
        error_message = ""

    run_audit(
        status=status,
        start_time=dag_run.start_date,
        error_message=error_message,
        run_id=dag_run.run_id,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="olist_data_pipeline",
    description="Olist Data Engineering Pipeline",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=[
        "olist",
        "data-engineering",
    ],
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

    scd_type1_task = PythonOperator(
        task_id="scd_type1",
        python_callable=run_scd_type1,
    )

    scd_type2_task = PythonOperator(
        task_id="scd_type2",
        python_callable=run_scd_type2,
    )

    transformation_task = PythonOperator(
        task_id="business_transformation",
        python_callable=run_business_transformation,
    )

    gold_task = PythonOperator(
        task_id="gold",
        python_callable=run_gold,
    )

    postgres_task = PythonOperator(
        task_id="load_postgres",
        python_callable=run_load_postgres,
    )

    metadata_task = PythonOperator(
        task_id="metadata",
        python_callable=finalize_metadata,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    audit_task = PythonOperator(
        task_id="audit",
        python_callable=run_airflow_audit,
        trigger_rule=TriggerRule.ALL_DONE,
    )


    ingestion_task >> bronze_task
    bronze_task >> validation_task
    validation_task >> silver_task

    silver_task >> [
        scd_type1_task,
        scd_type2_task,
    ]

    [
        scd_type1_task,
        scd_type2_task,
    ] >> transformation_task

    transformation_task >> gold_task
    gold_task >> postgres_task
    postgres_task >> metadata_task
    metadata_task >> audit_task