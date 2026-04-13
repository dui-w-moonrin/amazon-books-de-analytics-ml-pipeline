from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def build_command(command: str) -> str:
    return f"cd /opt/airflow && {command}"


with DAG(
    dag_id="silver_books_flow",
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["amazon-books", "silver", "flow"],
) as dag:

    standardize_books_data = BashOperator(
        task_id="standardize_books_data",
        bash_command=build_command(
            "python scripts/run_silver_job.py --config config/silver/books_data_silver.json"
        ),
    )

    quality_enrich_books_data = BashOperator(
        task_id="quality_enrich_books_data",
        bash_command=build_command(
            "python scripts/run_silver_quality_job.py --config config/quality/books_data_quality.json"
        ),
    )

    books_data_completeness_check = BashOperator(
        task_id="books_data_completeness_check",
        bash_command=build_command(
            "python scripts/run_silver_data_quality_check.py --config config/quality_checks/books_data_quality_checks.json"
        ),
    )

    books_data_consistency_check = BashOperator(
        task_id="books_data_consistency_check",
        bash_command='echo "books_data consistency checkpoint - reserved"',
    )

    books_data_validity_check = BashOperator(
        task_id="books_data_validity_check",
        bash_command='echo "books_data validity checkpoint - reserved"',
    )

    books_data_uniqueness_check = BashOperator(
        task_id="books_data_uniqueness_check",
        bash_command='echo "books_data uniqueness checkpoint - reserved"',
    )

    quarantine_books_data = BashOperator(
        task_id="quarantine_books_data",
        bash_command=build_command(
            "python scripts/run_silver_quarantine.py --config config/quarantine/books_data_quarantine.json"
        ),
    )

    standardize_books_rating = BashOperator(
        task_id="standardize_books_rating",
        bash_command=build_command(
            "python scripts/run_silver_job.py --config config/silver/books_rating_silver.json"
        ),
    )

    quality_enrich_books_rating = BashOperator(
        task_id="quality_enrich_books_rating",
        bash_command=build_command(
            "python scripts/run_silver_quality_job.py --config config/quality/books_rating_quality.json"
        ),
    )

    books_rating_completeness_check = BashOperator(
        task_id="books_rating_completeness_check",
        bash_command=build_command(
            "python scripts/run_silver_data_quality_check.py --config config/quality_checks/books_rating_quality_checks.json"
        ),
    )

    books_rating_consistency_check = BashOperator(
        task_id="books_rating_consistency_check",
        bash_command='echo "books_rating consistency checkpoint - reserved"',
    )

    books_rating_validity_check = BashOperator(
        task_id="books_rating_validity_check",
        bash_command='echo "books_rating validity checkpoint - reserved"',
    )

    books_rating_uniqueness_check = BashOperator(
        task_id="books_rating_uniqueness_check",
        bash_command='echo "books_rating uniqueness checkpoint - reserved"',
    )

    books_data_silver_snapshot = BashOperator(
        task_id="books_data_silver_snapshot",
        bash_command=build_command(
            "python scripts/run_dataset_snapshot.py "
            "--dataset books_data "
            "--asset silver_eligible "
            "--stage silver "
            "--input-format parquet "
            "--sample-rows 5"
        ),
    )

    books_rating_silver_snapshot = BashOperator(
        task_id="books_rating_silver_snapshot",
        bash_command=build_command(
            "python scripts/run_dataset_snapshot.py "
            "--dataset books_rating "
            "--asset silver_quality_enriched "
            "--stage silver "
            "--input-format parquet "
            "--sample-rows 5"
        ),
    )

    validate_review_to_book_relationship = BashOperator(
        task_id="validate_review_to_book_relationship",
        bash_command=build_command(
            "python scripts/run_silver_cross_check_relationship.py "
            "--config config/relationship_checks/books_title_hash_relationship.json"
        ),
    )

    standardize_books_data >> quality_enrich_books_data
    quality_enrich_books_data >> quarantine_books_data
    quarantine_books_data >> books_data_completeness_check
    books_data_completeness_check >> books_data_consistency_check
    books_data_consistency_check >> books_data_validity_check
    books_data_validity_check >> books_data_uniqueness_check
    books_data_uniqueness_check >> books_data_silver_snapshot

    standardize_books_rating >> quality_enrich_books_rating
    quality_enrich_books_rating >> books_rating_completeness_check
    books_rating_completeness_check >> books_rating_consistency_check
    books_rating_consistency_check >> books_rating_validity_check
    books_rating_validity_check >> books_rating_uniqueness_check
    books_rating_uniqueness_check >> books_rating_silver_snapshot

    [books_data_silver_snapshot, books_rating_silver_snapshot] >> validate_review_to_book_relationship