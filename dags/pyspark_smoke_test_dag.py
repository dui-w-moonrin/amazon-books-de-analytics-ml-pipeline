from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id="pyspark_smoke_test",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "pyspark", "smoke-test"],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_ingest",
        external_dag_id="bronze_ingestion",
        external_task_id="ingest_books_rating",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        check_existence=True,
        mode="reschedule",
        poke_interval=15,
        timeout=600,
    )

    run_pyspark_smoke_test = BashOperator(
        task_id="run_pyspark_smoke_test",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_pyspark_smoke_test.py "
            "--dataset books_data "
            "--asset bronze_full"
        ),
    )

    wait_for_bronze >> run_pyspark_smoke_test