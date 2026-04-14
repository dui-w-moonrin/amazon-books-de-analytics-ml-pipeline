from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.transformers.config_driven_serving_transformer import (
    ConfigDrivenServingTransformer,
)
from src.utils.config_loader import get_resolved_asset_path
from src.utils.job_runtime import resolve_path


class GoldServeJob:
    """
    Build Gold serving datasets for downstream consumers.

    Responsibilities:
    - resolve input datasets from config
    - read one or more prepared upstream datasets
    - apply config-driven serving logic
    - write serving outputs for DA or DS use cases

    Inputs:
    - project root path
    - Gold serving config

    Output:
    - Gold dataset written to the configured serving output path
    """

    def __init__(self, project_root: Path, config: dict):
        self.project_root = project_root
        self.config = config

    def _get_output_name(self) -> str:
        output_name = self.config.get("output_name")
        if not output_name:
            raise KeyError("Missing required config key: output_name")
        return output_name

    def _create_spark_session(self) -> SparkSession:
        output_name = self._get_output_name()
        spark_config = self.config.get("spark", {})

        builder = (
            SparkSession.builder
            .master(spark_config.get("master", "local[1]"))
            .appName(spark_config.get("app_name", f"gold-serve-{output_name}"))
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
        )
        return builder.getOrCreate()

    def _resolve_input_path(self, input_name: str, input_cfg: dict) -> Path:
        input_path = input_cfg.get("path")
        if input_path:
            return resolve_path(self.project_root, input_path)

        dataset_name = input_cfg.get("dataset_name")
        asset_name = input_cfg.get("asset_name")

        if not dataset_name or not asset_name:
            raise KeyError(
                f"Input '{input_name}' must define either path "
                f"or (dataset_name, asset_name)"
            )

        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=dataset_name,
            asset_name=asset_name,
        )

    def _resolve_output_path(self) -> Path:
        output_cfg = self.config.get("output", {})
        output_path = output_cfg.get("path")
        if not output_path:
            raise KeyError("output.path is required")

        return resolve_path(self.project_root, output_path)

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

    def run(self) -> None:
        output_name = self._get_output_name()
        inputs_cfg = self.config.get("inputs", {})
        output_cfg = self.config.get("output", {})

        if not inputs_cfg:
            raise ValueError("inputs config must not be empty")

        spark = self._create_spark_session()

        try:
            input_dfs: dict[str, DataFrame] = {}

            for input_name, input_cfg in inputs_cfg.items():
                input_format = input_cfg.get("format", "parquet")
                input_path = self._resolve_input_path(input_name, input_cfg)
                input_dfs[input_name] = self._read_input_df(
                    spark=spark,
                    input_path=input_path,
                    input_format=input_format,
                )

            transformer = ConfigDrivenServingTransformer(
                input_dfs=input_dfs,
                config=self.config,
            )
            output_df = transformer.transform()

            output_path = self._resolve_output_path()
            output_path.parent.mkdir(parents=True, exist_ok=True)

            self._write_output_df(
                df=output_df,
                output_path=output_path,
                output_format=output_cfg.get("format", "parquet"),
                write_mode=output_cfg.get("mode", "overwrite"),
                write_partitions=output_cfg.get("partitions"),
            )

            print("DONE")
            print(f"output_name={output_name}")
            print(f"output_path={output_path}")
            print(f"row_count={output_df.count()}")
            print(f"columns={', '.join(output_df.columns)}")

        finally:
            spark.stop()