from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.transformers.config_driven_column_transformer import (
    ConfigDrivenColumnTransformer,
)
from src.utils.config_loader import get_resolved_asset_path
from src.utils.job_runtime import resolve_path


class SilverStandardizeJob:
    def __init__(self, project_root: Path, config: dict[str, Any]):
        self.project_root = project_root
        self.config = config

    def _get_dataset_name(self) -> str:
        dataset_name = self.config.get("dataset_name")
        if not dataset_name:
            raise KeyError("Missing required config key: dataset_name")
        return dataset_name

    def _resolve_input_path(self) -> Path:
        dataset_name = self._get_dataset_name()
        input_asset = self.config.get("input_asset", "bronze_full")
        input_path = self.config.get("input_path")

        if input_path:
            return resolve_path(self.project_root, input_path)

        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=dataset_name,
            asset_name=input_asset,
        )

    def _resolve_output_path(self) -> Path:
        dataset_name = self._get_dataset_name()
        output_asset = self.config.get("output_asset", "silver_standardized")
        output_path = self.config.get("output_path")

        if output_path:
            return resolve_path(self.project_root, output_path)

        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=dataset_name,
            asset_name=output_asset,
        )

    def _create_spark_session(self) -> SparkSession:
        dataset_name = self._get_dataset_name()
        spark_config = self.config.get("spark", {})

        builder = (
            SparkSession.builder
            .master(spark_config.get("master", "local[1]"))
            .appName(
                spark_config.get(
                    "app_name",
                    f"silver-standardize-{dataset_name}",
                )
            )
            .config(
                "spark.driver.memory",
                spark_config.get("driver_memory", "4g"),
            )
            .config(
                "spark.sql.shuffle.partitions",
                str(spark_config.get("shuffle_partitions", 8)),
            )
            .config(
                "spark.default.parallelism",
                str(spark_config.get("default_parallelism", 2)),
            )
            .config(
                "spark.sql.files.maxPartitionBytes",
                spark_config.get("max_partition_bytes", "16m"),
            )
        )

        return builder.getOrCreate()

    def _read_input_df(
        self,
        spark: SparkSession,
        input_path: Path,
        input_format: str,
    ) -> DataFrame:
        if input_format == "parquet":
            return spark.read.parquet(str(input_path))

        if input_format == "csv":
            return spark.read.option("header", True).csv(str(input_path))

        raise ValueError(f"Unsupported input_format: {input_format}")

    def _write_output_df(
        self,
        df: DataFrame,
        output_path: Path,
        output_format: str,
        write_mode: str,
    ) -> None:
        writer = df.write.mode(write_mode)

        if output_format == "parquet":
            writer.parquet(str(output_path))
            return

        if output_format == "csv":
            writer.option("header", True).csv(str(output_path))
            return

        raise ValueError(f"Unsupported output_format: {output_format}")

    def run(self) -> None:
        dataset_name = self._get_dataset_name()
        input_format = self.config.get("input_format", "parquet")
        output_format = self.config.get("output_format", "parquet")
        output_write_mode = self.config.get("output_write_mode", "overwrite")
        write_partitions = self.config.get("write_partitions")

        input_path = self._resolve_input_path()
        output_path = self._resolve_output_path()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        spark = self._create_spark_session()

        try:
            input_df = self._read_input_df(spark, input_path, input_format)

            transformer = ConfigDrivenColumnTransformer(input_df, self.config)
            output_df = transformer.transform()

            if write_partitions:
                output_df = output_df.repartition(write_partitions)

            self._write_output_df(
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