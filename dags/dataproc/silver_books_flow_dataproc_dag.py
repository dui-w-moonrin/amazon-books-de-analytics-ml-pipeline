import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator


def load_runtime_config() -> dict:
    dag_dir = Path(__file__).resolve().parent
    config_path = dag_dir / "config" / "dataproc_runtime.json"

    if not config_path.exists():
        raise FileNotFoundError(
            f"dataproc_runtime.json not found at: {config_path}"
        )

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_gcs_uri(bucket_uri: str, relative_path: str) -> str:
    return f"{bucket_uri.rstrip('/')}/{relative_path.lstrip('/')}"


def build_batch(runtime_cfg: dict, main_python_file_uri: str, args: list[str]) -> dict:
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
    if service_account:
        batch["environment_config"] = {
            "execution_config": {
                "service_account": service_account
            }
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

RUN_SILVER_JOB_URI = build_gcs_uri(
    BUCKET_URI,
    "code/dataproc/run_silver_job_dataproc.py",
)
RUN_SILVER_QUALITY_JOB_URI = build_gcs_uri(
    BUCKET_URI,
    "code/dataproc/run_silver_quality_job_dataproc.py",
)
RUN_SILVER_QUARANTINE_URI = build_gcs_uri(
    BUCKET_URI,
    "code/dataproc/run_silver_quarantine_dataproc.py",
)
RUN_SILVER_DQ_CHECK_URI = build_gcs_uri(
    BUCKET_URI,
    "code/dataproc/run_silver_data_quality_check_dataproc.py",
)
RUN_SILVER_REL_CHECK_URI = build_gcs_uri(
    BUCKET_URI,
    "code/dataproc/run_silver_cross_check_relationship_dataproc.py",
)
RUN_DATASET_SNAPSHOT_URI = build_gcs_uri(
    BUCKET_URI,
    "code/dataproc/run_dataset_snapshot_dataproc.py",
)

BOOKS_DATA_SILVER_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/silver/books_data_silver.json",
)
BOOKS_RATING_SILVER_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/silver/books_rating_silver.json",
)
BOOKS_DATA_QUALITY_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/quality/books_data_quality.json",
)
BOOKS_RATING_QUALITY_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/quality/books_rating_quality.json",
)
BOOKS_DATA_QUARANTINE_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/quarantine/books_data_quarantine.json",
)
BOOKS_DATA_QUALITY_CHECK_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/quality_checks/books_data_quality_checks.json",
)
BOOKS_RATING_QUALITY_CHECK_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/quality_checks/books_rating_quality_checks.json",
)
BOOKS_RELATIONSHIP_CHECK_CONFIG_URI = build_gcs_uri(
    BUCKET_URI,
    "config/dataproc/relationship_checks/books_title_hash_relationship.json",
)


with DAG(
    dag_id="silver_books_flow_dataproc",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["amazon-books", "silver", "flow", "dataproc"],
    description="Dataproc Silver layer pipeline for books_data and books_rating",
) as dag:

    standardize_books_data = DataprocCreateBatchOperator(
        task_id="standardize_books_data",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_JOB_URI,
            [
                "--config", BOOKS_DATA_SILVER_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-std-books-data-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    quality_enrich_books_data = DataprocCreateBatchOperator(
        task_id="quality_enrich_books_data",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_QUALITY_JOB_URI,
            [
                "--config", BOOKS_DATA_QUALITY_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-qe-books-data-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    quarantine_books_data = DataprocCreateBatchOperator(
        task_id="quarantine_books_data",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_QUARANTINE_URI,
            [
                "--config", BOOKS_DATA_QUARANTINE_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-qt-books-data-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    books_data_completeness_check = DataprocCreateBatchOperator(
        task_id="books_data_completeness_check",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_DQ_CHECK_URI,
            [
                "--config", BOOKS_DATA_QUALITY_CHECK_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-dq-books-data-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    books_data_silver_snapshot = DataprocCreateBatchOperator(
        task_id="books_data_silver_snapshot",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_DATASET_SNAPSHOT_URI,
            [
                "--dataset", "books_data",
                "--asset", "silver_eligible",
                "--stage", "silver",
                "--input-format", "parquet",
                "--sample-rows", "5",
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-snap-books-data-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    standardize_books_rating = DataprocCreateBatchOperator(
        task_id="standardize_books_rating",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_JOB_URI,
            [
                "--config", BOOKS_RATING_SILVER_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-std-books-rating-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    quality_enrich_books_rating = DataprocCreateBatchOperator(
        task_id="quality_enrich_books_rating",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_QUALITY_JOB_URI,
            [
                "--config", BOOKS_RATING_QUALITY_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-qe-books-rating-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    books_rating_completeness_check = DataprocCreateBatchOperator(
        task_id="books_rating_completeness_check",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_DQ_CHECK_URI,
            [
                "--config", BOOKS_RATING_QUALITY_CHECK_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-dq-books-rating-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    books_rating_silver_snapshot = DataprocCreateBatchOperator(
        task_id="books_rating_silver_snapshot",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_DATASET_SNAPSHOT_URI,
            [
                "--dataset", "books_rating",
                "--asset", "silver_quality_enriched",
                "--stage", "silver",
                "--input-format", "parquet",
                "--sample-rows", "5",
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-snap-books-rating-{{ ds_nodash }}-{{ ti.try_number }}",
    )

    validate_review_to_book_relationship = DataprocCreateBatchOperator(
        task_id="validate_review_to_book_relationship",
        project_id=PROJECT_ID,
        region=REGION,
        batch=build_batch(
            cfg,
            RUN_SILVER_REL_CHECK_URI,
            [
                "--config", BOOKS_RELATIONSHIP_CHECK_CONFIG_URI,
                "--data-assets-path", DATA_ASSETS_URI,
            ],
        ),
        batch_id="silver-rel-books-{{ ds_nodash }}-{{ ti.try_number }}",
    )
    # avoid quota limit issues by not running book_data and books_rating tasks in parallel
    standardize_books_data >> quality_enrich_books_data >> quarantine_books_data
    quarantine_books_data >> books_data_completeness_check >> books_data_silver_snapshot
    books_data_silver_snapshot >> standardize_books_rating
    standardize_books_rating >> quality_enrich_books_rating
    quality_enrich_books_rating >> books_rating_completeness_check >> books_rating_silver_snapshot
    books_rating_silver_snapshot >> validate_review_to_book_relationship