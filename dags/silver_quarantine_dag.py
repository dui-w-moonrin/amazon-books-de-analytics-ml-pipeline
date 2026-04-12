from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_quarantine_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_silver_quarantine.py --config {config_path}"
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
    dag_id="silver_quarantine",
    start_date=datetime(2026, 4, 12),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver", "quarantine"],
) as dag:

    quarantine_books_data = BashOperator(
        task_id="quarantine_books_data",
        bash_command=build_quarantine_command(
            "config/quarantine/books_data_quarantine.json"
        ),
    )

    snapshot_books_data_eligible = BashOperator(
        task_id="snapshot_books_data_eligible",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="silver_eligible",
            stage_name="silver-eligible",
            input_format="parquet",
        ),
    )

    snapshot_books_data_quarantine = BashOperator(
        task_id="snapshot_books_data_quarantine",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="silver_quarantine",
            stage_name="silver-quarantine",
            input_format="parquet",
        ),
    )

    quarantine_books_data >> [
        snapshot_books_data_eligible,
        snapshot_books_data_quarantine,
    ]