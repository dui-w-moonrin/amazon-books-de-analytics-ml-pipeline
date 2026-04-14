from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="main_pipeline",
    start_date=datetime(2026, 4, 14),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "orchestration", "main"],
    description="Main orchestration DAG for Bronze -> Silver -> Gold pipeline",
) as dag:

    trigger_bronze_ingestion = TriggerDagRunOperator(
        task_id="trigger_bronze_ingestion",
        trigger_dag_id="bronze_ingestion",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_silver_books_flow = TriggerDagRunOperator(
        task_id="trigger_silver_books_flow",
        trigger_dag_id="silver_books_flow",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_gold_serving = TriggerDagRunOperator(
        task_id="trigger_gold_serving",
        trigger_dag_id="gold_serving",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_bronze_ingestion >> trigger_silver_books_flow >> trigger_gold_serving