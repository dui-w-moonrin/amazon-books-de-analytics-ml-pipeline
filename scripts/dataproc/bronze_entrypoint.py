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


def main() -> None:
    if len(sys.argv) < 2:
        raise ValueError("Usage: python bronze_entrypoint.py <config_path>")

    config_path = sys.argv[1]
    config = load_json_from_path(config_path)

    app_name = config.get("job_name", "bronze-dataproc")
    source_path = config["source_path"]
    output_path = config["output_path"]
    write_partitions = int(config.get("write_partitions", 1))
    write_mode = config.get("output_write_mode", "overwrite")

    spark = build_spark(app_name)

    try:
        print("DEBUG START")
        print(f"config_path={config_path}")
        print(f"source_path={source_path}")
        print(f"output_path={output_path}")
        print(f"write_partitions={write_partitions}")
        print(f"write_mode={write_mode}")
        print("DEBUG END")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", "\\")
            .option("mode", "PERMISSIVE")
            .csv(source_path)
        )

        actual_columns = df.columns
        expected_columns = [col["source"] for col in config["columns"]]
        missing = [col for col in expected_columns if col not in actual_columns]

        print(f"actual_columns={actual_columns}")
        print(f"expected_columns={expected_columns}")

        if missing:
            raise ValueError(f"Missing source columns: {missing}")

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