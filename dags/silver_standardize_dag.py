from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="silver_standardize",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver"],
) as dag:

    standardize_books_data = BashOperator(
        task_id="standardize_books_data",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_silver_job.py --config config/silver/books_data_silver.json"
        ),
    )

    standardize_books_rating = BashOperator(
        task_id="standardize_books_rating",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_silver_job.py --config config/silver/books_rating_silver.json"
        ),
    )

    [standardize_books_data, standardize_books_rating]