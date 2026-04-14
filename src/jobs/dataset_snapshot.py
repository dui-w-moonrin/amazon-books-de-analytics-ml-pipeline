from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.utils.config_loader import get_resolved_asset_path


class DatasetSnapshotJob:
    """
    Print a lightweight snapshot of a dataset for inspection.

    Responsibilities:
    - resolve dataset asset path
    - read the input dataset with Spark
    - print columns, schema, sample rows, and row count

    Inputs:
    - dataset name
    - asset name
    - stage label
    - input format
    - sample row limit

    Output:
    - console snapshot for debugging and pipeline checkpoint visibility
    """
    def __init__(
        self,
        project_root: Path,
        dataset_name: str,
        asset_name: str,
        stage_name: str,
        input_format: str = "parquet",
        sample_rows: int = 5,
    ):
        self.project_root = project_root
        self.dataset_name = dataset_name
        self.asset_name = asset_name
        self.stage_name = stage_name
        self.input_format = input_format
        self.sample_rows = sample_rows

    def _resolve_input_path(self) -> Path:
        return get_resolved_asset_path(
            project_root=self.project_root,
            dataset_name=self.dataset_name,
            asset_name=self.asset_name,
        )

    def _create_spark_session(self) -> SparkSession:
        return (
            SparkSession.builder
            .master("local[1]")
            .appName(
                f"{self.stage_name}-snapshot-{self.dataset_name}-{self.asset_name}"
            )
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "2")
            .getOrCreate()
        )

    def _read_input_df(
        self,
        spark: SparkSession,
        input_path: Path,
    ) -> DataFrame:
        if self.input_format == "parquet":
            return spark.read.parquet(str(input_path))

        if self.input_format == "csv":
            return spark.read.option("header", True).csv(str(input_path))

        raise ValueError(f"Unsupported input_format: {self.input_format}")

    def run(self) -> None:
        input_path = self._resolve_input_path()
        spark = self._create_spark_session()

        try:
            df = self._read_input_df(spark, input_path)

            print("=" * 80)
            print("DATASET SNAPSHOT START")
            print(f"stage_name={self.stage_name}")
            print(f"dataset_name={self.dataset_name}")
            print(f"asset_name={self.asset_name}")
            print(f"input_format={self.input_format}")
            print(f"input_path={input_path}")
            print("=" * 80)

            print("\n[1] Columns")
            print(df.columns)

            print("\n[2] Schema")
            df.printSchema()

            print("\n[3] Sample rows")
            df.show(self.sample_rows, truncate=False)

            print("\n[4] Row count")
            print(df.count())

            print("\nDATASET SNAPSHOT DONE")
            print("=" * 80)

        finally:
            spark.stop()