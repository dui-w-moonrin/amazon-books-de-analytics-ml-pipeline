from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="pyspark_smoke_test",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "pyspark", "smoke-test"],
) as dag:

    run_pyspark_smoke_test = BashOperator(
        task_id="run_pyspark_smoke_test",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_pyspark_smoke_test.py "
            "--dataset books_data "
            "--asset bronze_full"
        ),
    )