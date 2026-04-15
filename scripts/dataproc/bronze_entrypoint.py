import json
import sys
import tempfile
from typing import Any

from pyspark.sql import SparkSession


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


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


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


def resolve_paths_from_config(config: dict[str, Any]) -> tuple[str, str]:
    """
    Support both config styles:

    Style A (old)
      - source_path
      - output_path

    Style B (new asset-driven)
      - dataset_name
      - source_asset
      - output_asset
      - data_assets_path
    """
    if "source_path" in config and "output_path" in config:
        return config["source_path"], config["output_path"]

    required_keys = ["dataset_name", "source_asset", "output_asset", "data_assets_path"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise KeyError(
            "Bronze config is missing required keys. "
            f"Need either ['source_path', 'output_path'] "
            f"or {required_keys}. "
            f"Found keys: {list(config.keys())}"
        )

    data_assets = load_json_from_path(config["data_assets_path"])
    dataset_name = config["dataset_name"]

    source_path = resolve_asset_path(
        data_assets=data_assets,
        dataset_name=dataset_name,
        asset_name=config["source_asset"],
    )
    output_path = resolve_asset_path(
        data_assets=data_assets,
        dataset_name=dataset_name,
        asset_name=config["output_asset"],
    )

    return source_path, output_path


def main() -> None:
    if len(sys.argv) < 2:
        raise ValueError("Usage: python bronze_entrypoint.py <config_path>")

    config_path = sys.argv[1]
    config = load_json_from_path(config_path)

    app_name = config.get("job_name", "bronze-dataproc")
    write_partitions = int(config.get("write_partitions", 1))
    write_mode = config.get("output_write_mode", "overwrite")

    # Short debug first
    print("DEBUG CONFIG START")
    print(f"config_path={config_path}")
    print(f"config_keys={list(config.keys())}")
    print("DEBUG CONFIG END")

    source_path, output_path = resolve_paths_from_config(config)

    spark = build_spark(app_name)

    try:
        print("DEBUG PATHS START")
        print(f"source_path={source_path}")
        print(f"output_path={output_path}")
        print(f"write_partitions={write_partitions}")
        print(f"write_mode={write_mode}")
        print("DEBUG PATHS END")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("ignoreLeadingWhiteSpace", True)
            .option("ignoreTrailingWhiteSpace", True)
            .option("mode", "PERMISSIVE")
            .csv(source_path)
        )

        actual_columns = df.columns
        expected_columns = [col["source"] for col in config["columns"]]
        missing_columns = [col for col in expected_columns if col not in actual_columns]

        print(f"actual_columns={actual_columns}")
        print(f"expected_columns={expected_columns}")

        if missing_columns:
            raise ValueError(f"Missing source columns: {missing_columns}")

        select_exprs = [
            f"`{col['source']}` AS `{col['target']}`"
            for col in config["columns"]
        ]

        output_df = df.selectExpr(*select_exprs).coalesce(write_partitions)

        (
            output_df.write
            .mode(write_mode)
            .parquet(output_path)
        )

        print("DONE")
        print(f"source_path={source_path}")
        print(f"output_path={output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()