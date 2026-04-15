import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator


def load_runtime_config() -> dict:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / "config" / "dataproc" / "dataproc_runtime.json"

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    required_keys = ["project_id", "region", "gcs", "batch"]
    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise ValueError(
            f"Missing required keys in dataproc_runtime.json: {missing}. "
            f"Found keys: {list(cfg.keys())}"
        )

    required_gcs_keys = [
        "bronze_entrypoint_uri",
        "snapshot_entrypoint_uri",
        "data_assets_uri",
        "books_data_bronze_config_uri",
        "books_rating_bronze_config_uri",
    ]
    missing_gcs = [k for k in required_gcs_keys if k not in cfg["gcs"]]
    if missing_gcs:
        raise ValueError(
            f"Missing required keys in dataproc_runtime.json.gcs: {missing_gcs}"
        )

    return cfg


def build_batch(
    runtime_cfg: dict,
    main_python_file_uri: str,
    args: list[str],
) -> dict:
    batch = {
        "pyspark_batch": {
            "main_python_file_uri": main_python_file_uri,
            "args": args,
        },
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

BRONZE_ENTRYPOINT_URI = cfg["gcs"]["bronze_entrypoint_uri"]
SNAPSHOT_ENTRYPOINT_URI = cfg["gcs"]["snapshot_entrypoint_uri"]
DATA_ASSETS_URI = cfg["gcs"]["data_assets_uri"]

BOOKS_DATA_BRONZE_CONFIG_URI = cfg["gcs"]["books_data_bronze_config_uri"]
BOOKS_RATING_BRONZE_CONFIG_URI = cfg["gcs"]["books_rating_bronze_config_uri"]


with DAG(
    dag_id="bronze_ingestion_dataproc",
    start_date=datetime(2026, 4, 9),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "bronze", "dataproc"],
) as dag:

    ingest_books_data = DataprocCreateBatchOperator(
        task_id="ingest_books_data",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            runtime_cfg=cfg,
            main_python_file_uri=BRONZE_ENTRYPOINT_URI,
            args=[BOOKS_DATA_BRONZE_CONFIG_URI],
        ),
        batch_id="bronze-books-data-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    ingest_books_rating = DataprocCreateBatchOperator(
        task_id="ingest_books_rating",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            runtime_cfg=cfg,
            main_python_file_uri=BRONZE_ENTRYPOINT_URI,
            args=[BOOKS_RATING_BRONZE_CONFIG_URI],
        ),
        batch_id="bronze-books-rating-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    snapshot_bronze_books_data = DataprocCreateBatchOperator(
        task_id="snapshot_bronze_books_data",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            runtime_cfg=cfg,
            main_python_file_uri=SNAPSHOT_ENTRYPOINT_URI,
            args=[
                "--data-assets",
                DATA_ASSETS_URI,
                "--dataset",
                "books_data",
                "--asset",
                "bronze_full",
                "--stage",
                "bronze",
                "--input-format",
                "parquet",
                "--sample-rows",
                "5",
            ],
        ),
        batch_id="snapshot-bronze-books-data-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    snapshot_bronze_books_rating = DataprocCreateBatchOperator(
        task_id="snapshot_bronze_books_rating",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            runtime_cfg=cfg,
            main_python_file_uri=SNAPSHOT_ENTRYPOINT_URI,
            args=[
                "--data-assets",
                DATA_ASSETS_URI,
                "--dataset",
                "books_rating",
                "--asset",
                "bronze_full",
                "--stage",
                "bronze",
                "--input-format",
                "parquet",
                "--sample-rows",
                "5",
            ],
        ),
        batch_id="snapshot-bronze-books-rating-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    ingest_books_data >> snapshot_bronze_books_data
    ingest_books_rating >> snapshot_bronze_books_rating