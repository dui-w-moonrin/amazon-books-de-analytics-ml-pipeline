from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.config_loader import get_resolved_asset_path
from src.utils.job_runtime import resolve_path


class SilverQuarantineJob:
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
        input_asset = self.config.get("input_asset", "silver_standardized")
        input_path = self.config.get("input_path")

        if input_path:
            return resolve_path(self.project_root, input_path)

        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=dataset_name,
            asset_name=input_asset,
        )

    def _resolve_eligible_output_path(self) -> Path:
        dataset_name = self._get_dataset_name()
        output_asset = self.config.get("eligible_output_asset", "silver_eligible")
        output_path = self.config.get("eligible_output_path")

        if output_path:
            return resolve_path(self.project_root, output_path)

        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=dataset_name,
            asset_name=output_asset,
        )

    def _resolve_quarantine_output_path(self) -> Path:
        dataset_name = self._get_dataset_name()
        output_asset = self.config.get("quarantine_output_asset", "silver_quarantine")
        output_path = self.config.get("quarantine_output_path")

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
                    f"silver-quarantine-{dataset_name}",
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
        write_partitions: int | None = None,
    ) -> None:
        if write_partitions:
            df = df.repartition(write_partitions)

        writer = df.write.mode(write_mode)

        if output_format == "parquet":
            writer.parquet(str(output_path))
            return

        if output_format == "csv":
            writer.option("header", True).csv(str(output_path))
            return

        raise ValueError(f"Unsupported output_format: {output_format}")

    def _build_quarantine_condition(self):
        required_cols = self.config.get("rules", {}).get(
            "require_non_null_columns",
            ["title", "title_key"],
        )

        if not required_cols:
            raise ValueError("Quarantine rules require at least one column.")

        condition = None
        for col_name in required_cols:
            current = F.col(col_name).isNull()
            condition = current if condition is None else (condition | current)

        return condition

    def _build_quarantine_reason(self):
        return (
            F.when(
                F.col("title").isNull() & F.col("title_key").isNull(),
                F.lit("missing_title_and_title_key"),
            )
            .when(
                F.col("title").isNull(),
                F.lit("missing_title"),
            )
            .when(
                F.col("title_key").isNull(),
                F.lit("missing_title_key"),
            )
            .otherwise(F.lit("quarantine_rule_matched"))
        )

    def run(self) -> None:
        dataset_name = self._get_dataset_name()
        input_format = self.config.get("input_format", "parquet")
        output_format = self.config.get("output_format", "parquet")
        output_write_mode = self.config.get("output_write_mode", "overwrite")
        write_partitions = self.config.get("write_partitions")
        quarantine_stage = self.config.get("quarantine_stage", "silver")
        quarantine_entity = self.config.get("quarantine_entity", dataset_name)

        input_path = self._resolve_input_path()
        eligible_output_path = self._resolve_eligible_output_path()
        quarantine_output_path = self._resolve_quarantine_output_path()

        eligible_output_path.parent.mkdir(parents=True, exist_ok=True)
        quarantine_output_path.parent.mkdir(parents=True, exist_ok=True)

        spark = self._create_spark_session()

        try:
            input_df = self._read_input_df(spark, input_path, input_format)

            quarantine_condition = self._build_quarantine_condition()

            quarantine_df = (
                input_df
                .filter(quarantine_condition)
                .withColumn("quarantine_reason", self._build_quarantine_reason())
                .withColumn("quarantine_stage", F.lit(quarantine_stage))
                .withColumn("quarantine_entity", F.lit(quarantine_entity))
            )

            eligible_df = input_df.filter(~quarantine_condition)

            self._write_output_df(
                df=eligible_df,
                output_path=eligible_output_path,
                output_format=output_format,
                write_mode=output_write_mode,
                write_partitions=write_partitions,
            )

            self._write_output_df(
                df=quarantine_df,
                output_path=quarantine_output_path,
                output_format=output_format,
                write_mode=output_write_mode,
                write_partitions=write_partitions,
            )

            print("DONE")
            print(f"dataset_name={dataset_name}")
            print(f"input_path={input_path}")
            print(f"eligible_output_path={eligible_output_path}")
            print(f"quarantine_output_path={quarantine_output_path}")
            print(f"input_count={input_df.count()}")
            print(f"eligible_count={eligible_df.count()}")
            print(f"quarantine_count={quarantine_df.count()}")

        finally:
            spark.stop()