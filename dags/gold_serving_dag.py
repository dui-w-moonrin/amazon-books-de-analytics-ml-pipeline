from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_gold_command(config_path: str) -> str:
    return (
        "cd /opt/airflow && "
        f"python scripts/run_gold_serve.py --config {config_path}"
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
    dag_id="gold_serving",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "gold", "serving"],
) as dag:

    serve_books_serving_da = BashOperator(
        task_id="serve_books_serving_da",
        bash_command=build_gold_command(
            "config/gold/gold_books_serving_da.json"
        ),
    )

    serve_reviews_serving_da = BashOperator(
        task_id="serve_reviews_serving_da",
        bash_command=build_gold_command(
            "config/gold/gold_reviews_serving_da.json"
        ),
    )

    snapshot_gold_books_serving_da = BashOperator(
        task_id="snapshot_gold_books_serving_da",
        bash_command=build_snapshot_command(
            dataset_name="books_data",
            asset_name="gold_books_serving_da",
            stage_name="gold",
            input_format="parquet",
        ),
    )

    snapshot_gold_reviews_serving_da = BashOperator(
        task_id="snapshot_gold_reviews_serving_da",
        bash_command=build_snapshot_command(
            dataset_name="books_rating",
            asset_name="gold_reviews_serving_da",
            stage_name="gold",
            input_format="parquet",
        ),
    )

    serve_books_serving_da >> snapshot_gold_books_serving_da
    serve_reviews_serving_da >> snapshot_gold_reviews_serving_da

    # serve_reviews_serving_ds = BashOperator(
    #     task_id="serve_reviews_serving_ds",
    #     bash_command=build_gold_command(
    #         "config/gold/gold_reviews_serving_ds.json"
    #     ),
    # )

    # snapshot_gold_reviews_serving_ds = BashOperator(
    #     task_id="snapshot_gold_reviews_serving_ds",
    #     bash_command=build_snapshot_command(
    #         dataset_name="books_rating",
    #         asset_name="gold_reviews_serving_ds",
    #         stage_name="gold",
    #         input_format="parquet",
    #     ),
    # )

    # serve_reviews_serving_ds >> snapshot_gold_reviews_serving_ds