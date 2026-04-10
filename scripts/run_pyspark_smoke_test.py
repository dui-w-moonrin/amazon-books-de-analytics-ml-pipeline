import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.utils.config_loader import get_asset_path


def parse_args():
    parser = argparse.ArgumentParser(description="PySpark parquet smoke test")
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset key in config/data_assets.json, e.g. books_data",
    )
    parser.add_argument(
        "--asset",
        default="bronze_full",
        help="Asset key in config/data_assets.json, e.g. bronze_full",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=5,
        help="Number of sample rows to show",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    source_path = Path(get_asset_path(args.dataset, args.asset))
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    spark = (
        SparkSession.builder
        .appName(f"pyspark-smoke-test-{args.dataset}")
        .master("local[*]")
        .getOrCreate()
    )

    print("=" * 80)
    print("PYSPARK PARQUET SMOKE TEST START")
    print(f"Dataset: {args.dataset}")
    print(f"Asset: {args.asset}")
    print(f"Source path: {source_path}")
    print("=" * 80)

    df = spark.read.parquet(str(source_path))

    print("\n[1] Columns")
    print(df.columns)

    print("\n[2] Schema")
    df.printSchema()

    print("\n[3] Sample rows")
    df.show(args.sample_rows, truncate=False)

    print("\n[4] Row count")
    print(df.count())

    print("\nPYSPARK PARQUET SMOKE TEST DONE")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()