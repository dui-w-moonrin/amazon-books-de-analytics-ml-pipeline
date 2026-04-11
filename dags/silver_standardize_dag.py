from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_job_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_silver_job.py --config {config_path}"
    )


with DAG(
    dag_id="silver_standardize",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver"],
) as dag:

    standardize_books_data = BashOperator(
        task_id="standardize_books_data",
        bash_command=build_job_command(
            "config/silver/books_data_silver.json"
        ),
    )

    standardize_books_rating = BashOperator(
        task_id="standardize_books_rating",
        bash_command=build_job_command(
            "config/silver/books_rating_silver.json"
        ),
    )