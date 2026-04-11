from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_job_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_bronze_job.py --config {config_path}"
    )


with DAG(
    dag_id="bronze_ingestion",
    start_date=datetime(2026, 4, 9),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "bronze"],
) as dag:

    ingest_books_data = BashOperator(
        task_id="ingest_books_data",
        bash_command=build_job_command(
            "config/bronze/books_data_bronze.json"
        ),
    )

    ingest_books_rating = BashOperator(
        task_id="ingest_books_rating",
        bash_command=build_job_command(
            "config/bronze/books_rating_bronze.json"
        ),
    )