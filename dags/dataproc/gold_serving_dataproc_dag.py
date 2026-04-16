import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


def load_runtime_config() -> dict:
    dag_dir = Path(__file__).resolve().parent
    config_path = dag_dir / "config" / "dataproc_runtime.json"

    if not config_path.exists():
        raise FileNotFoundError(
            f"dataproc_runtime.json not found at: {config_path}"
        )

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    required_keys = [
        "project_id",
        "region",
        "bucket_uri",
        "code_bundle_uri",
        "batch",
    ]
    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise ValueError(
            f"Missing required keys in dataproc_runtime.json: {missing}. "
            f"Found keys: {list(cfg.keys())}"
        )

    return cfg


def build_gcs_uri(bucket_uri: str, relative_path: str) -> str:
    return f"{bucket_uri.rstrip('/')}/{relative_path.lstrip('/')}"


def build_batch(runtime_cfg: dict, main_python_file_uri: str, args: list[str]) -> dict:
    pyspark_batch = {
        "main_python_file_uri": main_python_file_uri,
        "args": args,
    }

    code_bundle_uri = runtime_cfg.get("code_bundle_uri", "").strip()
    if code_bundle_uri:
        pyspark_batch["python_file_uris"] = [code_bundle_uri]

    batch = {
        "pyspark_batch": pyspark_batch,
        "runtime_config": {
            "version": runtime_cfg["batch"].get("runtime_version", "2.2"),
            "properties": runtime_cfg["batch"].get(
                "default_dataproc_properties", {}
            ),
        },
    }

    service_account = runtime_cfg["batch"].get("service_account", "").strip()
    subnetwork_uri = runtime_cfg["batch"].get("subnetwork_uri", "").strip()

    execution_config = {}

    if service_account:
        execution_config["service_account"] = service_account

    if subnetwork_uri:
        execution_config["subnetwork_uri"] = subnetwork_uri

    if execution_config:
        batch["environment_config"] = {
            "execution_config": execution_config
        }

    return batch


cfg = load_runtime_config()

PROJECT_ID = cfg["project_id"]
REGION = cfg["region"]
BUCKET_URI = cfg["bucket_uri"]

DATA_ASSETS_URI = build_gcs_uri(
    BUCKET_URI,
    "config/assets/dataproc.data_assets.json",
)

RUN_GOLD_SERVE_URI = build_gcs_uri(
    BUCKET_URI,
    "code/scripts/dataproc/run_gold_serve_dataproc.py",
)

RUN_DATASET_SNAPSHOT_URI = build_gcs_uri(
    BUCKET_URI,
    "code/scripts/dataproc/run_dataset_snapshot_dataproc.py",
)

GOLD_BOOKS_SERVING_DA_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/gold/gold_books_serving_da.json",
)
GOLD_REVIEWS_SERVING_DA_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/gold/gold_reviews_serving_da.json",
)
GOLD_REVIEWS_SERVING_DS_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/gold/gold_reviews_serving_ds.json",
)


with DAG(
    dag_id="gold_serving_dataproc",
    start_date=datetime(2026, 4, 16),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["amazon-books", "gold", "serving", "dataproc"],
    description="Dataproc Gold serving DAG in serial order due to CPU quota",
) as dag:

    serve_books_serving_da = DataprocCreateBatchOperator(
        task_id="serve_books_serving_da",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_GOLD_SERVE_URI,
            [
                "--config", GOLD_BOOKS_SERVING_DA_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="gold-books-da-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    snapshot_gold_books_serving_da = DataprocCreateBatchOperator(
        task_id="snapshot_gold_books_serving_da",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_DATASET_SNAPSHOT_URI,
            [
                "--dataset", "books_data",
                "--asset", "gold_books_serving_da",
                "--stage", "gold",
                "--input-format", "parquet",
                "--sample-rows", "5",
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="gold-snap-books-da-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    serve_reviews_serving_da = DataprocCreateBatchOperator(
        task_id="serve_reviews_serving_da",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_GOLD_SERVE_URI,
            [
                "--config", GOLD_REVIEWS_SERVING_DA_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="gold-reviews-da-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    snapshot_gold_reviews_serving_da = DataprocCreateBatchOperator(
        task_id="snapshot_gold_reviews_serving_da",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_DATASET_SNAPSHOT_URI,
            [
                "--dataset", "books_rating",
                "--asset", "gold_reviews_serving_da",
                "--stage", "gold",
                "--input-format", "parquet",
                "--sample-rows", "5",
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="gold-snap-reviews-da-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    serve_reviews_serving_ds = DataprocCreateBatchOperator(
        task_id="serve_reviews_serving_ds",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_GOLD_SERVE_URI,
            [
                "--config", GOLD_REVIEWS_SERVING_DS_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="gold-reviews-ds-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    snapshot_gold_reviews_serving_ds = DataprocCreateBatchOperator(
        task_id="snapshot_gold_reviews_serving_ds",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_DATASET_SNAPSHOT_URI,
            [
                "--dataset", "books_rating",
                "--asset", "gold_reviews_serving_ds",
                "--stage", "gold",
                "--input-format", "parquet",
                "--sample-rows", "5",
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="gold-snap-reviews-ds-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    serve_books_serving_da >> snapshot_gold_books_serving_da
    snapshot_gold_books_serving_da >> serve_reviews_serving_da
    serve_reviews_serving_da >> snapshot_gold_reviews_serving_da
    snapshot_gold_reviews_serving_da >> serve_reviews_serving_ds
    serve_reviews_serving_ds >> snapshot_gold_reviews_serving_ds