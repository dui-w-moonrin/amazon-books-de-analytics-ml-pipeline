from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="bronze_ingestion",
    start_date=datetime(2026, 4, 9),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "bronze"],
) as dag:

    ingest_books_data = BashOperator(
        task_id="ingest_books_data",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_bronze_job.py --config config/books_data_bronze.json"
        ),
    )

    ingest_books_rating = BashOperator(
        task_id="ingest_books_rating",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_bronze_job.py --config config/books_rating_bronze.json"
        ),
    )

    ingest_books_data >> ingest_books_rating