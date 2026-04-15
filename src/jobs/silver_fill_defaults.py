from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config_loader import get_resolved_asset_path


class SilverFillDefaultsJob:
    """
    Fill selected null / blank string fields with configured default values.

    Responsibilities:
    - resolve input/output asset paths from config
    - read silver standardized dataset
    - fill configured columns when value is NULL or blank string
    - write result back to configured silver asset

    This step is intended to sit between:
      standardize -> fill_defaults -> quality_enrich
    """

    def __init__(
        self,
        project_root: Path,
        config: dict[str, Any],
    ):
        self.project_root = project_root
        self.config = config

    def _get_dataset_name(self) -> str:
        return self.config["dataset_name"]

    def _resolve_input_path(self) -> Path:
        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=self._get_dataset_name(),
            asset_name=self.config["input_asset"],
        )

    def _resolve_output_path(self) -> Path:
        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=self._get_dataset_name(),
            asset_name=self.config["output_asset"],
        )

    def _create_spark_session(self) -> SparkSession:
        spark_config = self.config.get("spark", {})

        builder = (
            SparkSession.builder
            .appName(
                spark_config.get(
                    "app_name",
                    f"silver-fill-defaults-{self._get_dataset_name()}",
                )
            )
        )

        master = spark_config.get("master")
        if master:
            builder = builder.master(master)

        driver_memory = spark_config.get("driver_memory")
        if driver_memory:
            builder = builder.config("spark.driver.memory", str(driver_memory))

        shuffle_partitions = spark_config.get("shuffle_partitions")
        if shuffle_partitions:
            builder = builder.config(
                "spark.sql.shuffle.partitions",
                str(shuffle_partitions),
            )

        default_parallelism = spark_config.get("default_parallelism")
        if default_parallelism:
            builder = builder.config(
                "spark.default.parallelism",
                str(default_parallelism),
            )

        max_partition_bytes = spark_config.get("max_partition_bytes")
        if max_partition_bytes:
            builder = builder.config(
                "spark.sql.files.maxPartitionBytes",
                str(max_partition_bytes),
            )

        return builder.getOrCreate()

    def _read_input_df(
        self,
        spark: SparkSession,
        input_path: Path,
    ) -> DataFrame:
        input_format = self.config.get("input_format", "parquet")

        if input_format == "parquet":
            return spark.read.parquet(str(input_path))

        raise ValueError(f"Unsupported input_format: {input_format}")

    def _apply_fill_rules(self, df: DataFrame) -> DataFrame:
        fill_rules = self.config.get("fill_rules", [])

        for rule in fill_rules:
            column_name = rule["column"]
            fill_value = rule["value"]

            df = df.withColumn(
                column_name,
                F.when(
                    F.col(column_name).isNull()
                    | (F.trim(F.col(column_name)) == ""),
                    F.lit(fill_value),
                ).otherwise(F.col(column_name))
            )

        return df

    def run(self) -> None:
        input_path = self._resolve_input_path()
        output_path = self._resolve_output_path()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        spark = self._create_spark_session()

        try:
            df = self._read_input_df(spark, input_path)
            output_df = self._apply_fill_rules(df)

            write_partitions = self.config.get("write_partitions")
            if write_partitions:
                output_df = output_df.coalesce(write_partitions)

            (
                output_df.write
                .mode(self.config.get("output_write_mode", "overwrite"))
                .format(self.config.get("output_format", "parquet"))
                .save(str(output_path))
            )

            print("DONE")
            print(f"dataset_name={self._get_dataset_name()}")
            print(f"input_path={input_path}")
            print(f"output_path={output_path}")
            print(
                "filled_columns="
                + ", ".join(
                    rule["column"] for rule in self.config.get("fill_rules", [])
                )
            )

        finally:
            spark.stop()