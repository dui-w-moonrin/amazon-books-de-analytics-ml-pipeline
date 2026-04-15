from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_gold_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/local/run_gold_serve.py --config {config_path}"
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
    dag_id="gold_serving_local",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "gold", "serving", "local"],
    description="Local Gold serving DAG",
) as dag:

    # -----------------------------
    # gold serving tasks for DA
    # -----------------------------

    serve_books_serving_da = BashOperator(
        task_id="serve_books_serving_da",
        bash_command=build_gold_command(
            "config/local/gold/gold_books_serving_da.json"
        ),
    )

    serve_reviews_serving_da = BashOperator(
        task_id="serve_reviews_serving_da",
        bash_command=build_gold_command(
            "config/local/gold/gold_reviews_serving_da.json"
        ),
    )

    snapshot_gold_books_serving_da = BashOperator(
        task_id="snapshot_gold_books_serving_da",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="gold_books_serving_da",
            stage_name="gold",
            input_format="parquet",
            sample_rows=5,
        ),
    )

    snapshot_gold_reviews_serving_da = BashOperator(
        task_id="snapshot_gold_reviews_serving_da",
        bash_command=build_snapshot_command(
            dataset_name="books_rating",
            asset_name="gold_reviews_serving_da",
            stage_name="gold",
            input_format="parquet",
            sample_rows=5,
        ),
    )

    # -----------------------------
    # gold serving tasks for DS
    # -----------------------------

    serve_reviews_serving_ds = BashOperator(
        task_id="serve_reviews_serving_ds",
        bash_command=build_gold_command(
            "config/local/gold/gold_reviews_serving_ds.json"
        ),
    )

    snapshot_gold_reviews_serving_ds = BashOperator(
        task_id="snapshot_gold_reviews_serving_ds",
        bash_command=build_snapshot_command(
            dataset_name="books_rating",
            asset_name="gold_reviews_serving_ds",
            stage_name="gold",
            input_format="parquet",
            sample_rows=5,
        ),
    )

    # -----------------------------
    # dependencies
    # -----------------------------

    serve_books_serving_da >> snapshot_gold_books_serving_da
    serve_reviews_serving_da >> snapshot_gold_reviews_serving_da
    serve_reviews_serving_ds >> snapshot_gold_reviews_serving_ds