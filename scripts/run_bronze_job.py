import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.bronze_ingestion import BronzeIngestionJob
from src.utils.job_runtime import (
    load_json_file,
    load_simple_env,
    resolve_config_path,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run bronze ingestion job from config"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Config filename or relative/absolute path",
    )
    return parser.parse_args()


def main() -> None:
    load_simple_env(PROJECT_ROOT / ".env")
    args = parse_args()

    config_path = resolve_config_path(PROJECT_ROOT, args.config)
    config = load_json_file(config_path)

    duckdb_db_path = (
        config.get("duckdb_db_path")
        or os.getenv("DUCKDB_DATABASE_PATH")
        or ":memory:"
    )

    job = BronzeIngestionJob(
        project_root=PROJECT_ROOT,
        duckdb_db_path=duckdb_db_path,
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()