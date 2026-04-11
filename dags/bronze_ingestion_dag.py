from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_bronze_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_bronze_job.py --config {config_path}"
    )


def build_snapshot_command(
    dataset_name: str,
    asset_name: str,
    stage_name: str,
    input_format: str = "parquet",
) -> str:
    return (
        "cd /opt/airflow && "
        "python scripts/run_dataset_snapshot.py "
        f"--dataset {dataset_name} "
        f"--asset {asset_name} "
        f"--stage {stage_name} "
        f"--input-format {input_format}"
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
        bash_command=build_bronze_command(
            "config/bronze/books_data_bronze.json"
        ),
    )

    ingest_books_rating = BashOperator(
        task_id="ingest_books_rating",
        bash_command=build_bronze_command(
            "config/bronze/books_rating_bronze.json"
        ),
    )

    snapshot_bronze_books_data = BashOperator(
        task_id="snapshot_bronze_books_data",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="bronze_full",
            stage_name="bronze",
            input_format="parquet",
        ),
    )

    snapshot_bronze_books_rating = BashOperator(
        task_id="snapshot_bronze_books_rating",
        bash_command=build_snapshot_command(
            dataset_name="books_rating",
            asset_name="bronze_full",
            stage_name="bronze",
            input_format="parquet",
        ),
    )

    ingest_books_data >> snapshot_bronze_books_data
    ingest_books_rating >> snapshot_bronze_books_rating