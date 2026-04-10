import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession


# ทำให้ import src/... ได้ ตอนรันแบบ:
# python scripts/run_silver_job.py --config ...
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.silver_standardize import standardize_silver
from src.utils.config_loader import get_asset_path


def load_json_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_input_df(spark: SparkSession, input_path: str, input_format: str):
    if input_format == "parquet":
        return spark.read.parquet(input_path)

    if input_format == "csv":
        return spark.read.option("header", True).csv(input_path)

    raise ValueError(f"Unsupported input_format: {input_format}")


def write_output_df(df, output_path: str, output_format: str, write_mode: str) -> None:
    writer = df.write.mode(write_mode)

    if output_format == "parquet":
        writer.parquet(output_path)
        return

    if output_format == "csv":
        writer.option("header", True).csv(output_path)
        return

    raise ValueError(f"Unsupported output_format: {output_format}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run silver standardize job")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to silver config JSON file"
    )
    args = parser.parse_args()

    config = load_json_config(args.config)

    dataset_name = config["dataset_name"]
    input_asset = config.get("input_asset", "bronze_full")
    output_asset = config.get("output_asset", "silver_standardized")
    input_format = config.get("input_format", "parquet")
    output_format = config.get("output_format", "parquet")
    output_write_mode = config.get("output_write_mode", "overwrite")
    write_partitions = config.get("write_partitions")

    input_path = get_asset_path(dataset_name, input_asset)
    output_path = get_asset_path(dataset_name, output_asset)

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName(f"silver-standardize-{dataset_name}")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.files.maxPartitionBytes", "16m")
        .getOrCreate()
    )

    try:
        input_df = read_input_df(spark, input_path, input_format)
        output_df = standardize_silver(input_df, config)

        if write_partitions:
            output_df = output_df.repartition(write_partitions)

        write_output_df(
            df=output_df,
            output_path=output_path,
            output_format=output_format,
            write_mode=output_write_mode,
        )

        print("DONE")
        print(f"dataset_name={dataset_name}")
        print(f"input_path={input_path}")
        print(f"output_path={output_path}")
        print(f"write_partitions={write_partitions}")
        print(f"columns={', '.join(output_df.columns)}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()