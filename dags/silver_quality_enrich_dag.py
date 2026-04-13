from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_quality_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_silver_quality_job.py --config {config_path}"
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
    dag_id="silver_quality_enrich",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver", "quality"],
) as dag:

    quality_books_data = BashOperator(
        task_id="quality_books_data",
        bash_command=build_quality_command(
            "config/quality/books_data_quality.json"
        ),
    )

    quality_books_rating = BashOperator(
        task_id="quality_books_rating",
        bash_command=build_quality_command(
            "config/quality/books_rating_quality.json"
        ),
    )

    snapshot_quality_books_data = BashOperator(
        task_id="snapshot_quality_books_data",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="silver_quality_enriched",
            stage_name="silver-quality",
            input_format="parquet",
        ),
    )

    snapshot_quality_books_rating = BashOperator(
        task_id="snapshot_quality_books_rating",
        bash_command=build_snapshot_command(
            dataset_name="books_rating",
            asset_name="silver_quality_enriched",
            stage_name="silver-quality",
            input_format="parquet",
        ),
    )

    quality_books_data >> snapshot_quality_books_data
    quality_books_rating >> snapshot_quality_books_rating