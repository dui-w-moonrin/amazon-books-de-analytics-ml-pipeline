from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.quality.cross_table_relationship_checker import (
    CrossTableRelationshipChecker,
)
from src.utils.config_loader import get_resolved_asset_path
from src.utils.job_runtime import resolve_path


class SilverCrossCheckRelationshipJob:
    def __init__(self, project_root: Path, config: dict[str, Any]):
        self.project_root = project_root
        self.config = config

    def _resolve_input_path(self, side: str) -> Path:
        dataset_name = self.config[f"{side}_dataset_name"]
        asset_name = self.config[f"{side}_asset_name"]
        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=dataset_name,
            asset_name=asset_name,
        )

    def _resolve_summary_output_path(self) -> Path:
        raw_path = self.config["summary_output_path"]
        return resolve_path(self.project_root, raw_path)

    def _resolve_failed_rows_output_path(self) -> Path:
        raw_path = self.config["failed_rows_output_path"]
        return resolve_path(self.project_root, raw_path)

    def _create_spark_session(self) -> SparkSession:
        spark_config = self.config.get("spark", {})

        return (
            SparkSession.builder
            .master(spark_config.get("master", "local[1]"))
            .appName(spark_config.get("app_name", "silver-cross-check-relationship"))
            .config("spark.driver.memory", spark_config.get("driver_memory", "4g"))
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
            .getOrCreate()
        )

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

    def _write_summary_df(
        self,
        spark: SparkSession,
        summary_row: dict[str, Any],
        output_path: Path,
        output_format: str,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        summary_df = spark.createDataFrame([summary_row])

        if output_format == "csv":
            (
                summary_df.coalesce(1)
                .write.mode("overwrite")
                .option("header", True)
                .csv(str(output_path))
            )
            return

        if output_format == "parquet":
            summary_df.write.mode("overwrite").parquet(str(output_path))
            return

        raise ValueError(f"Unsupported summary_output_format: {output_format}")

    def _write_failed_rows_df(
        self,
        failed_df: DataFrame,
        output_path: Path,
        output_format: str,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_format == "csv":
            (
                failed_df.coalesce(1)
                .write.mode("overwrite")
                .option("header", True)
                .csv(str(output_path))
            )
            return

        if output_format == "parquet":
            failed_df.write.mode("overwrite").parquet(str(output_path))
            return

        raise ValueError(f"Unsupported failed_rows_output_format: {output_format}")

    def run(self) -> None:
        input_format = self.config.get("input_format", "parquet")
        summary_output_format = self.config.get("summary_output_format", "csv")
        failed_rows_output_format = self.config.get(
            "failed_rows_output_format",
            "csv",
        )

        left_input_path = self._resolve_input_path("left")
        right_input_path = self._resolve_input_path("right")
        summary_output_path = self._resolve_summary_output_path()
        failed_rows_output_path = self._resolve_failed_rows_output_path()

        spark = self._create_spark_session()

        try:
            left_df = self._read_input_df(spark, left_input_path, input_format)
            right_df = self._read_input_df(spark, right_input_path, input_format)

            checker = CrossTableRelationshipChecker(
                left_df=left_df,
                right_df=right_df,
                config=self.config,
            )
            summary_row, failed_df = checker.run_check()

            self._write_summary_df(
                spark=spark,
                summary_row=summary_row,
                output_path=summary_output_path,
                output_format=summary_output_format,
            )

            self._write_failed_rows_df(
                failed_df=failed_df,
                output_path=failed_rows_output_path,
                output_format=failed_rows_output_format,
            )

            print("DONE")
            print(f"check_name={summary_row['check_name']}")
            print(f"left_input_path={left_input_path}")
            print(f"right_input_path={right_input_path}")
            print(f"status={summary_row['status']}")
            print(f"left_total_keys={summary_row['left_total_keys']}")
            print(f"matched_keys={summary_row['matched_keys']}")
            print(f"missing_keys={summary_row['missing_keys']}")

            print("\n[RELATIONSHIP CHECK SUMMARY]")
            spark.createDataFrame([summary_row]).show(truncate=False)

            print("\n[MISSING TITLE_HASH SAMPLE]")
            failed_df.show(self.config.get("sample_limit", 20), truncate=False)

            if summary_row["severity"] == "critical" and summary_row["missing_keys"] > 0:
                raise ValueError(
                    "Critical cross-table relationship check failed: "
                    f"{summary_row['check_name']}"
                )

        finally:
            spark.stop()