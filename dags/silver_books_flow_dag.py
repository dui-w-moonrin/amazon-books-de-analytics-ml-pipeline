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

    data_quality_check_books_data = BashOperator(
        task_id="data_quality_check_books_data",
        bash_command=build_command(
            "python scripts/run_silver_data_quality_check.py --config config/quality_checks/books_data_quality_checks.json"
        ),
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

    data_quality_check_books_rating = BashOperator(
        task_id="data_quality_check_books_rating",
        bash_command=build_command(
            "python scripts/run_silver_data_quality_check.py --config config/quality_checks/books_rating_quality_checks.json"
        ),
    )

    standardize_books_data >> quality_enrich_books_data >> data_quality_check_books_data >> quarantine_books_data
    standardize_books_rating >> quality_enrich_books_rating >> data_quality_check_books_rating