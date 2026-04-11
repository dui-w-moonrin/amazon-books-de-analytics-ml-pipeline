import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.dataset_snapshot import DatasetSnapshotJob
from src.utils.job_runtime import load_simple_env


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run dataset snapshot checkpoint job"
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset key in config/data_assets.json, e.g. books_data",
    )
    parser.add_argument(
        "--asset",
        required=True,
        help="Asset key in config/data_assets.json, e.g. bronze_full",
    )
    parser.add_argument(
        "--stage",
        required=True,
        help="Stage label for logging, e.g. bronze or silver",
    )
    parser.add_argument(
        "--input-format",
        default="parquet",
        help="Input format, e.g. parquet or csv",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="Number of sample rows to show",
    )
    return parser.parse_args()


def main() -> None:
    load_simple_env(PROJECT_ROOT / ".env")
    args = parse_args()

    job = DatasetSnapshotJob(
        project_root=PROJECT_ROOT,
        dataset_name=args.dataset,
        asset_name=args.asset,
        stage_name=args.stage,
        input_format=args.input_format,
        sample_rows=args.sample_rows,
    )
    job.run()


if __name__ == "__main__":
    main()