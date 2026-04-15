from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_bronze_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/local/run_bronze_job.py --config {config_path}"
    )


def build_snapshot_command(
    dataset_name: str,
    asset_name: str,
    stage_name: str,
    input_format: str = "parquet",
    sample_rows: int = 5,
) -> str:
    return (
        "cd /opt/airflow && "
        "python scripts/local/run_dataset_snapshot.py "
        f"--dataset {dataset_name} "
        f"--asset {asset_name} "
        f"--stage {stage_name} "
        f"--input-format {input_format} "
        f"--sample-rows {sample_rows}"
    )


with DAG(
    dag_id="bronze_ingestion_local",
    start_date=datetime(2026, 4, 9),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "bronze", "local"],
    description="Local Bronze ingestion DAG",
) as dag:

    # -----------------------------
    # ingestion tasks
    # -----------------------------

    ingest_books_data = BashOperator(
        task_id="ingest_books_data",
        bash_command=build_bronze_command(
            "config/local/bronze/books_data_bronze.json"
        ),
    )

    ingest_books_rating = BashOperator(
        task_id="ingest_books_rating",
        bash_command=build_bronze_command(
            "config/local/bronze/books_rating_bronze.json"
        ),
    )

    # -----------------------------
    # snapshot tasks
    # -----------------------------

    snapshot_bronze_books_data = BashOperator(
        task_id="snapshot_bronze_books_data",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="bronze_full",
            stage_name="bronze",
            input_format="parquet",
            sample_rows=5,
        ),
    )

    snapshot_bronze_books_rating = BashOperator(
        task_id="snapshot_bronze_books_rating",
        bash_command=build_snapshot_command(
            dataset_name="books_rating",
            asset_name="bronze_full",
            stage_name="bronze",
            input_format="parquet",
            sample_rows=5,
        ),
    )

    # -----------------------------
    # dependencies
    # -----------------------------

    ingest_books_data >> snapshot_bronze_books_data
    ingest_books_rating >> snapshot_bronze_books_rating