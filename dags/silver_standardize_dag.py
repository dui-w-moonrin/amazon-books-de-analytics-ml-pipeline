from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_silver_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_silver_job.py --config {config_path}"
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
    dag_id="silver_standardize",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver"],
) as dag:

    standardize_books_data = BashOperator(
        task_id="standardize_books_data",
        bash_command=build_silver_command(
            "config/silver/books_data_silver.json"
        ),
    )

    standardize_books_rating = BashOperator(
        task_id="standardize_books_rating",
        bash_command=build_silver_command(
            "config/silver/books_rating_silver.json"
        ),
    )

    snapshot_silver_books_data = BashOperator(
        task_id="snapshot_silver_books_data",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="silver_standardized",
            stage_name="silver",
            input_format="parquet",
        ),
    )

    snapshot_silver_books_rating = BashOperator(
        task_id="snapshot_silver_books_rating",
        bash_command=build_snapshot_command(
            dataset_name="books_rating",
            asset_name="silver_standardized",
            stage_name="silver",
            input_format="parquet",
        ),
    )

    standardize_books_data >> snapshot_silver_books_data
    standardize_books_rating >> snapshot_silver_books_rating