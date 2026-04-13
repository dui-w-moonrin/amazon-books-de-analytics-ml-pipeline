from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_data_quality_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_silver_data_quality_check.py --config {config_path}"
    )


with DAG(
    dag_id="silver_data_quality_check",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver", "data-quality"],
) as dag:

    dq_books_data = BashOperator(
        task_id="dq_books_data",
        bash_command=build_data_quality_command(
            "config/quality_checks/books_data_quality_checks.json"
        ),
    )

    dq_books_rating = BashOperator(
        task_id="dq_books_rating",
        bash_command=build_data_quality_command(
            "config/quality_checks/books_rating_quality_checks.json"
        ),
    )