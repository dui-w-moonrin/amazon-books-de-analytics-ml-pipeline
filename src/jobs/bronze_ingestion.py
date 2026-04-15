from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from src.utils.config_loader import get_resolved_asset_path
from src.utils.job_runtime import resolve_path


class BronzeIngestionJob:
    """
    Ingest raw source data into the Bronze layer using PySpark.

    Responsibilities:
    - resolve source and output paths from config
    - validate source schema against configured column mappings
    - read raw CSV data with Spark
    - rename and select configured columns
    - write Bronze output as Parquet

    Notes:
    - Spark writes a parquet dataset directory, even if the output path ends with `.parquet`
    - This implementation is tuned to reduce memory pressure for large text-heavy CSV inputs
    """

    def __init__(
        self,
        project_root: Path,
        config: dict[str, Any],
    ):
        self.project_root = project_root
        self.config = config

    def _resolve_asset_or_path(
        self,
        dataset_key_field: str,
        asset_key_field: str,
        fallback_path_field: str,
    ) -> Path:
        dataset_name = self.config.get(dataset_key_field)
        asset_name = self.config.get(asset_key_field)

        if dataset_name and asset_name:
            return get_resolved_asset_path(
                project_root=self.project_root,
                dataset_name=dataset_name,
                asset_name=asset_name,
            )

        raw_path = self.config.get(fallback_path_field)
        if raw_path:
            return resolve_path(self.project_root, raw_path)

        raise KeyError(
            f"Missing config keys: either "
            f"({dataset_key_field}, {asset_key_field}) "
            f"or {fallback_path_field} must be provided."
        )

    def _resolve_source_path(self) -> Path:
        return self._resolve_asset_or_path(
            dataset_key_field="source_dataset",
            asset_key_field="source_asset",
            fallback_path_field="source_path",
        )

    def _resolve_output_path(self) -> Path:
        return self._resolve_asset_or_path(
            dataset_key_field="output_dataset",
            asset_key_field="output_asset",
            fallback_path_field="output_path",
        )

    def _get_csv_option(self, key: str, default: Any) -> Any:
        return self.config.get("csv_options", {}).get(key, default)

    def _get_source_columns(self) -> list[str]:
        return [col["source"] for col in self.config["columns"]]

    def _get_target_columns(self) -> list[str]:
        return [col["target"] for col in self.config["columns"]]

    def _get_spark_conf(self) -> dict[str, Any]:
        return self.config.get("spark", {})

    def _get_write_mode(self) -> str:
        return self.config.get("output_write_mode", "overwrite")

    def _get_write_partitions(self) -> int:
        return int(self.config.get("write_partitions", 2))

    def _get_max_records_per_file(self) -> int:
        return int(self.config.get("max_records_per_file", 50000))

    def _validate_job_config(self) -> None:
        source_type = self.config.get("source_type", "csv")
        output_type = self.config.get("output_type", "parquet")

        if source_type != "csv":
            raise ValueError(f"Unsupported bronze source_type: {source_type}")

        if output_type != "parquet":
            raise ValueError(f"Unsupported bronze output_type: {output_type}")

        source_cols = self._get_source_columns()
        target_cols = self._get_target_columns()

        if len(source_cols) != len(target_cols):
            raise ValueError("Source and target column counts do not match")

        if len(set(source_cols)) != len(source_cols):
            raise ValueError("Duplicate source column names found in config")

        if len(set(target_cols)) != len(target_cols):
            raise ValueError("Duplicate target column names found in config")

    def _build_spark(self) -> SparkSession:
        spark_conf = self._get_spark_conf()

        app_name = spark_conf.get(
            "app_name",
            self.config.get("job_name", "bronze_ingestion"),
        )

        builder = SparkSession.builder.appName(app_name)

        master = spark_conf.get("master")
        if master:
            builder = builder.master(master)

        driver_memory = spark_conf.get("driver_memory")
        if driver_memory:
            builder = builder.config("spark.driver.memory", driver_memory)

        shuffle_partitions = spark_conf.get("shuffle_partitions")
        if shuffle_partitions is not None:
            builder = builder.config(
                "spark.sql.shuffle.partitions",
                str(shuffle_partitions),
            )

        default_parallelism = spark_conf.get("default_parallelism")
        if default_parallelism is not None:
            builder = builder.config(
                "spark.default.parallelism",
                str(default_parallelism),
            )

        max_partition_bytes = spark_conf.get("max_partition_bytes")
        if max_partition_bytes:
            builder = builder.config(
                "spark.sql.files.maxPartitionBytes",
                max_partition_bytes,
            )

        parquet_block_size = spark_conf.get("parquet_block_size")
        if parquet_block_size:
            builder = builder.config(
                "parquet.block.size",
                parquet_block_size,
            )

        parquet_page_size = spark_conf.get("parquet_page_size")
        if parquet_page_size:
            builder = builder.config(
                "parquet.page.size",
                parquet_page_size,
            )

        return builder.getOrCreate()

    def _read_source_dataframe(
        self,
        spark: SparkSession,
        source_path: Path,
    ) -> DataFrame:
        header = bool(self._get_csv_option("header", True))
        delimiter = self._get_csv_option("delimiter", ",")
        encoding = self._get_csv_option("encoding", "UTF-8")
        multiline = bool(self._get_csv_option("multiLine", True))
        quote = self._get_csv_option("quote", '"')
        escape = self._get_csv_option("escape", '"')
        mode = self._get_csv_option("mode", "PERMISSIVE")
        ignore_leading_whitespace = bool(
            self._get_csv_option("ignoreLeadingWhiteSpace", True)
        )
        ignore_trailing_whitespace = bool(
            self._get_csv_option("ignoreTrailingWhiteSpace", True)
        )
        bad_records_path = self._get_csv_option("badRecordsPath", None)

        reader = (
            spark.read
            .option("header", header)
            .option("inferSchema", False)
            .option("delimiter", delimiter)
            .option("encoding", encoding)
            .option("multiLine", multiline)
            .option("quote", quote)
            .option("escape", escape)
            .option("mode", mode)
            .option("ignoreLeadingWhiteSpace", ignore_leading_whitespace)
            .option("ignoreTrailingWhiteSpace", ignore_trailing_whitespace)
        )

        if bad_records_path:
            bad_records_resolved = resolve_path(self.project_root, bad_records_path)
            bad_records_resolved.parent.mkdir(parents=True, exist_ok=True)
            reader = reader.option("badRecordsPath", str(bad_records_resolved))

        return reader.csv(str(source_path))

    def _validate_source_schema(self, actual_columns: list[str]) -> None:
        expected_columns = self._get_source_columns()
        missing = [col for col in expected_columns if col not in actual_columns]

        if missing:
            raise ValueError(f"Missing source columns: {missing}")

    def _build_selected_dataframe(self, df: DataFrame) -> DataFrame:
        select_exprs = []

        for col in self.config["columns"]:
            source = col["source"]
            target = col["target"]
            select_exprs.append(f"`{source}` AS `{target}`")

        return df.selectExpr(*select_exprs)

    def run(self) -> None:
        self._validate_job_config()

        source_path = self._resolve_source_path()
        output_path = self._resolve_output_path()

        write_mode = self._get_write_mode()
        write_partitions = self._get_write_partitions()
        max_records_per_file = self._get_max_records_per_file()
        spark_conf = self._get_spark_conf()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        spark = self._build_spark()

        try:
            df = self._read_source_dataframe(spark, source_path)

            actual_columns = df.columns
            self._validate_source_schema(actual_columns)

            selected_df = self._build_selected_dataframe(df)
            output_df = selected_df.coalesce(write_partitions)

            writer = (
                output_df.write
                .mode(write_mode)
                .option("maxRecordsPerFile", max_records_per_file)
            )

            parquet_block_size = spark_conf.get("parquet_block_size")
            if parquet_block_size:
                writer = writer.option("parquet.block.size", parquet_block_size)

            parquet_page_size = spark_conf.get("parquet_page_size")
            if parquet_page_size:
                writer = writer.option("parquet.page.size", parquet_page_size)

            compression = spark_conf.get("compression")
            if compression:
                writer = writer.option("compression", compression)

            writer.parquet(str(output_path))

            print("DONE")
            print(f"source_path={source_path}")
            print(f"output_path={output_path}")
            print("columns=" + ", ".join(self._get_target_columns()))

        finally:
            spark.stop()