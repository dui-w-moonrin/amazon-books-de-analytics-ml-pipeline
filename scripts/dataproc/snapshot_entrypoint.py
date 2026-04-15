import argparse
import json
import tempfile
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def load_json_from_path(path: str) -> dict[str, Any]:
    if path.startswith("gs://"):
        from google.cloud import storage

        parts = path.replace("gs://", "", 1).split("/", 1)
        bucket_name = parts[0]
        blob_path = parts[1]

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        with tempfile.NamedTemporaryFile(mode="w+b", delete=True) as tmp:
            blob.download_to_filename(tmp.name)
            with open(tmp.name, "r", encoding="utf-8") as f:
                return json.load(f)

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_asset_path(
    data_assets: dict[str, Any],
    dataset_name: str,
    asset_name: str,
) -> str:
    if dataset_name not in data_assets:
        raise KeyError(f"Dataset not found in data assets: {dataset_name}")

    dataset_assets = data_assets[dataset_name]

    if asset_name not in dataset_assets:
        raise KeyError(
            f"Asset not found in data assets: dataset={dataset_name}, asset={asset_name}"
        )

    return dataset_assets[asset_name]


def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_input_df(
    spark: SparkSession,
    input_path: str,
    input_format: str,
) -> DataFrame:
    if input_format == "parquet":
        return spark.read.parquet(input_path)

    if input_format == "csv":
        return spark.read.option("header", True).csv(input_path)

    raise ValueError(f"Unsupported input_format: {input_format}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run dataset snapshot checkpoint job on Dataproc"
    )
    parser.add_argument(
        "--data-assets",
        required=True,
        help="Path to dataproc.data_assets.json (gs://... or local path)",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset key in data assets, e.g. books_data",
    )
    parser.add_argument(
        "--asset",
        required=True,
        help="Asset key in data assets, e.g. bronze_full",
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
    args = parse_args()

    data_assets = load_json_from_path(args.data_assets)
    input_path = resolve_asset_path(
        data_assets=data_assets,
        dataset_name=args.dataset,
        asset_name=args.asset,
    )

    app_name = f"{args.stage}-snapshot-{args.dataset}-{args.asset}"
    spark = create_spark_session(app_name)

    try:
        df = read_input_df(
            spark=spark,
            input_path=input_path,
            input_format=args.input_format,
        )

        print("=" * 80)
        print("DATASET SNAPSHOT START")
        print(f"stage_name={args.stage}")
        print(f"dataset_name={args.dataset}")
        print(f"asset_name={args.asset}")
        print(f"input_format={args.input_format}")
        print(f"input_path={input_path}")
        print("=" * 80)

        print("\n[1] Columns")
        print(df.columns)

        print("\n[2] Schema")
        df.printSchema()

        print(f"\n[3] Sample rows (top {args.sample_rows})")
        df.show(args.sample_rows, truncate=False)

        print("\n[4] Row count")
        print(df.count())

        print("\nDATASET SNAPSHOT DONE")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()