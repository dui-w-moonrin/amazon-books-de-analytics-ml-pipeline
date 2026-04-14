from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.quality.config_driven_data_quality_checker import (
    ConfigDrivenDataQualityChecker,
)
from src.utils.config_loader import get_resolved_asset_path
from src.utils.job_runtime import resolve_path


class SilverDataQualityCheckJob:
    """
    Run data quality checks against a Silver dataset.

    Responsibilities:
    - read the configured Silver input
    - execute config-driven quality checks
    - write summary results
    - write failed row samples for each failed check
    - raise an error when critical checks fail

    Inputs:
    - project root path
    - data quality check config

    Output:
    - quality summary dataset
    - failed row sample outputs
    - pipeline failure when critical checks fail
    """
    def __init__(self, project_root: Path, config: dict[str, Any]):
        self.project_root = project_root
        self.config = config

    def _get_dataset_name(self) -> str:
        dataset_name = self.config.get("dataset_name")
        if not dataset_name:
            raise KeyError("Missing required config key: dataset_name")
        return dataset_name

    def _get_quality_dimension(self) -> str:
        quality_dimension = self.config.get("quality_dimension")
        if not quality_dimension:
            raise KeyError("Missing required config key: quality_dimension")
        return quality_dimension

    def _resolve_input_path(self) -> Path:
        dataset_name = self._get_dataset_name()
        input_asset = self.config.get("input_asset", "silver_quality_enriched")
        input_path = self.config.get("input_path")

        if input_path:
            return resolve_path(self.project_root, input_path)

        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=dataset_name,
            asset_name=input_asset,
        )

    def _resolve_summary_output_path(self) -> Path:
        raw_path = self.config.get("summary_output_path")
        if not raw_path:
            raise KeyError("Missing required config key: summary_output_path")
        return resolve_path(self.project_root, raw_path)

    def _resolve_failed_rows_base_path(self) -> Path:
        raw_path = self.config.get("failed_rows_base_path")
        if not raw_path:
            raise KeyError("Missing required config key: failed_rows_base_path")
        return resolve_path(self.project_root, raw_path)

    def _create_spark_session(self) -> SparkSession:
        dataset_name = self._get_dataset_name()
        quality_dimension = self._get_quality_dimension()
        spark_config = self.config.get("spark", {})

        builder = (
            SparkSession.builder
            .master(spark_config.get("master", "local[1]"))
            .appName(
                spark_config.get(
                    "app_name",
                    f"silver-data-quality-check-{dataset_name}-{quality_dimension}",
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

    def _write_summary_df(
        self,
        spark: SparkSession,
        summary_rows: list[dict[str, Any]],
        output_path: Path,
        output_format: str,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        summary_df = spark.createDataFrame(summary_rows)

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

    def _write_failed_samples(
        self,
        failed_samples: dict[str, DataFrame],
        base_path: Path,
        output_format: str,
    ) -> None:
        base_path.mkdir(parents=True, exist_ok=True)

        for check_name, failed_df in failed_samples.items():
            output_path = base_path / check_name

            if output_format == "csv":
                (
                    failed_df.coalesce(1)
                    .write.mode("overwrite")
                    .option("header", True)
                    .csv(str(output_path))
                )
                continue

            if output_format == "parquet":
                failed_df.write.mode("overwrite").parquet(str(output_path))
                continue

            raise ValueError(
                f"Unsupported failed_rows_output_format: {output_format}"
            )

    @staticmethod
    def _print_summary_block(
        dataset_name: str,
        quality_dimension: str,
        summary_rows: list[dict[str, Any]],
    ) -> None:
        total_checks = len(summary_rows)
        passed_checks = sum(
            1 for row in summary_rows if row["status"] == "passed"
        )
        failed_checks = sum(
            1 for row in summary_rows if row["status"] == "failed"
        )
        critical_failed_checks = sum(
            1
            for row in summary_rows
            if row["severity"] == "critical" and row["status"] == "failed"
        )
        warning_failed_checks = sum(
            1
            for row in summary_rows
            if row["severity"] == "warning" and row["status"] == "failed"
        )

        print("=" * 60)
        print("DATA QUALITY SUMMARY")
        print(f"dataset_name={dataset_name}")
        print(f"quality_dimension={quality_dimension}")
        print(f"total_checks={total_checks}")
        print(f"passed_checks={passed_checks}")
        print(f"failed_checks={failed_checks}")
        print(f"critical_failed_checks={critical_failed_checks}")
        print(f"warning_failed_checks={warning_failed_checks}")
        print("=" * 60)

        failed_rows = [
            row for row in summary_rows if row["status"] == "failed"
        ]

        if failed_rows:
            print("FAILED CHECKS")
            for row in failed_rows:
                print(
                    f"- {row['check_name']}: "
                    f"severity={row['severity']} "
                    f"failed_rows={row['failed_rows']} "
                    f"failed_pct={row['failed_pct']}"
                )
            print("=" * 60)
        else:
            print("ALL CHECKS PASSED")
            print("=" * 60)

    def run(self) -> None:
        dataset_name = self._get_dataset_name()
        quality_dimension = self._get_quality_dimension()
        input_format = self.config.get("input_format", "parquet")
        summary_output_format = self.config.get("summary_output_format", "csv")
        failed_rows_output_format = self.config.get(
            "failed_rows_output_format",
            "csv",
        )

        input_path = self._resolve_input_path()
        summary_output_path = self._resolve_summary_output_path()
        failed_rows_base_path = self._resolve_failed_rows_base_path()

        spark = self._create_spark_session()

        try:
            input_df = self._read_input_df(spark, input_path, input_format)

            checker = ConfigDrivenDataQualityChecker(input_df, self.config)
            summary_rows, failed_samples = checker.run_checks()

            self._write_summary_df(
                spark=spark,
                summary_rows=summary_rows,
                output_path=summary_output_path,
                output_format=summary_output_format,
            )

            self._write_failed_samples(
                failed_samples=failed_samples,
                base_path=failed_rows_base_path,
                output_format=failed_rows_output_format,
            )

            critical_failures = [
                row for row in summary_rows
                if row["severity"] == "critical" and row["failed_rows"] > 0
            ]

            print("DONE")
            print(f"dataset_name={dataset_name}")
            print(f"quality_dimension={quality_dimension}")
            print(f"input_path={input_path}")
            print(f"summary_output_path={summary_output_path}")
            print(f"failed_rows_base_path={failed_rows_base_path}")

            for row in summary_rows:
                print(
                    f"dimension={row['quality_dimension']} "
                    f"check={row['check_name']} "
                    f"severity={row['severity']} "
                    f"status={row['status']} "
                    f"failed_rows={row['failed_rows']}"
                )

            self._print_summary_block(
                dataset_name=dataset_name,
                quality_dimension=quality_dimension,
                summary_rows=summary_rows,
            )

            if critical_failures:
                failed_names = ", ".join(
                    row["check_name"] for row in critical_failures
                )
                raise ValueError(
                    f"Critical data quality checks failed for "
                    f"{dataset_name}/{quality_dimension}: {failed_names}"
                )

        finally:
            spark.stop()