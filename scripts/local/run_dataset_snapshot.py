import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("PIPELINE_MODE", "local")
os.environ.setdefault("PIPELINE_CONFIG_ROOT", "config/local")
os.environ.setdefault(
    "DATA_ASSETS_CONFIG_PATH",
    "config/assets/local.data_assets.json",
)

from src.jobs.dataset_snapshot import DatasetSnapshotJob
from src.utils.job_runtime import load_simple_env


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run dataset snapshot job"
    )
    parser.add_argument("--dataset", required=True, help="Dataset name")
    parser.add_argument("--asset", required=True, help="Asset name")
    parser.add_argument("--stage", required=True, help="Pipeline stage name")
    parser.add_argument(
        "--input-format",
        default="parquet",
        help="Input format (default: parquet)",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="Number of sample rows to display",
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