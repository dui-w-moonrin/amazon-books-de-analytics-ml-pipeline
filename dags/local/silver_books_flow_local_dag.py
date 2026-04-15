from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_command(command: str) -> str:
    return f"cd /opt/airflow && {command}"


with DAG(
    dag_id="silver_books_flow_local",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver", "flow", "local"],
    description="Local Silver layer pipeline for books_data and books_rating",
) as dag:

    # -----------------------------
    # books_data flow
    # -----------------------------
    standardize_books_data = BashOperator(
        task_id="standardize_books_data",
        bash_command=build_command(
            "python scripts/local/run_silver_job.py "
            "--config config/local/silver/books_data_silver.json"
        ),
    )

    fill_defaults_books_data = BashOperator(
        task_id="fill_defaults_books_data",
        bash_command=build_command(
            "python scripts/local/run_silver_fill_defaults.py "
            "--config config/local/fill_defaults/books_data_fill_defaults.json"
        ),
    )

    quality_enrich_books_data = BashOperator(
        task_id="quality_enrich_books_data",
        bash_command=build_command(
            "python scripts/local/run_silver_quality_job.py "
            "--config config/local/quality/books_data_quality.json"
        ),
    )

    quarantine_books_data = BashOperator(
        task_id="quarantine_books_data",
        bash_command=build_command(
            "python scripts/local/run_silver_quarantine.py "
            "--config config/local/quarantine/books_data_quarantine.json"
        ),
    )

    books_data_completeness_check = BashOperator(
        task_id="books_data_completeness_check",
        bash_command=build_command(
            "python scripts/local/run_silver_data_quality_check.py "
            "--config config/local/quality_checks/books_data_quality_checks.json"
        ),
    )

    books_data_silver_snapshot = BashOperator(
        task_id="books_data_silver_snapshot",
        bash_command=build_command(
            "python scripts/local/run_dataset_snapshot.py "
            "--dataset books_data "
            "--asset silver_eligible "
            "--stage silver "
            "--input-format parquet "
            "--sample-rows 5"
        ),
    )

    # -----------------------------
    # books_rating flow
    # -----------------------------
    standardize_books_rating = BashOperator(
        task_id="standardize_books_rating",
        bash_command=build_command(
            "python scripts/local/run_silver_job.py "
            "--config config/local/silver/books_rating_silver.json"
        ),
    )

    fill_defaults_books_rating = BashOperator(
        task_id="fill_defaults_books_rating",
        bash_command=build_command(
            "python scripts/local/run_silver_fill_defaults.py "
            "--config config/local/fill_defaults/books_rating_fill_defaults.json"
        ),
    )

    quality_enrich_books_rating = BashOperator(
        task_id="quality_enrich_books_rating",
        bash_command=build_command(
            "python scripts/local/run_silver_quality_job.py "
            "--config config/local/quality/books_rating_quality.json"
        ),
    )

    books_rating_completeness_check = BashOperator(
        task_id="books_rating_completeness_check",
        bash_command=build_command(
            "python scripts/local/run_silver_data_quality_check.py "
            "--config config/local/quality_checks/books_rating_quality_checks.json"
        ),
    )

    books_rating_silver_snapshot = BashOperator(
        task_id="books_rating_silver_snapshot",
        bash_command=build_command(
            "python scripts/local/run_dataset_snapshot.py "
            "--dataset books_rating "
            "--asset silver_quality_enriched "
            "--stage silver "
            "--input-format parquet "
            "--sample-rows 5"
        ),
    )

    # -----------------------------
    # cross-table relationship check
    # -----------------------------
    validate_review_to_book_relationship = BashOperator(
        task_id="validate_review_to_book_relationship",
        bash_command=build_command(
            "python scripts/local/run_silver_cross_check_relationship.py "
            "--config config/local/relationship_checks/books_title_hash_relationship.json"
        ),
    )

    # -----------------------------
    # dependencies
    # -----------------------------
    standardize_books_data >> fill_defaults_books_data >> quality_enrich_books_data >> quarantine_books_data
    quarantine_books_data >> books_data_completeness_check >> books_data_silver_snapshot

    standardize_books_rating >> fill_defaults_books_rating >> quality_enrich_books_rating
    quality_enrich_books_rating >> books_rating_completeness_check >> books_rating_silver_snapshot

    [books_data_silver_snapshot, books_rating_silver_snapshot] >> validate_review_to_book_relationship