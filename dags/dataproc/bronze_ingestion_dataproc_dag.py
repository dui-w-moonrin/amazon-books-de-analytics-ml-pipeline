import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_CONFIG_PATH = PROJECT_ROOT / "config" / "dataproc" / "dataproc_runtime.json"
RUNTIME = json.loads(RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"))


def build_batch(job_key: str, label_suffix: str) -> dict:
    pyspark_batch = {
        "main_python_file_uri": RUNTIME["main_python_uri"],
        "args": [
            "--config-uri",
            RUNTIME["jobs"][job_key],
            "--assets-config-uri",
            RUNTIME["assets_config_uri"]
        ]
    }

    python_file_uris = RUNTIME.get("python_file_uris", [])
    if python_file_uris:
        pyspark_batch["python_file_uris"] = python_file_uris

    batch = {
        "pyspark_batch": pyspark_batch,
        "runtime_config": {
            "version": RUNTIME.get("runtime_version", "2.2"),
            "properties": RUNTIME.get("spark_properties", {})
        },
        "labels": {
            "pipeline": "amazon-books",
            "layer": "bronze",
            "job": label_suffix,
            "env": "prod"
        }
    }

    execution_config = {}
    if RUNTIME.get("service_account"):
        execution_config["service_account"] = RUNTIME["service_account"]
    if RUNTIME.get("subnetwork_uri"):
        execution_config["subnetwork_uri"] = RUNTIME["subnetwork_uri"]

    if execution_config:
        batch["environment_config"] = {
            "execution_config": execution_config
        }

    return batch


with DAG(
    dag_id="bronze_ingestion_dataproc",
    start_date=datetime(2026, 4, 15),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "bronze", "dataproc", "prod"],
    description="Production Bronze ingestion DAG using Dataproc batch",
) as dag:

    ingest_books_data = DataprocCreateBatchOperator(
        task_id="ingest_books_data",
        project_id=RUNTIME["project_id"],
        region=RUNTIME["region"],
        batch=build_batch("books_data", "books-data"),
        batch_id="bronze-books-data-{{ ts_nodash | lower }}",
    )

    ingest_books_rating = DataprocCreateBatchOperator(
        task_id="ingest_books_rating",
        project_id=RUNTIME["project_id"],
        region=RUNTIME["region"],
        batch=build_batch("books_rating", "books-rating"),
        batch_id="bronze-books-rating-{{ ts_nodash | lower }}",
    )