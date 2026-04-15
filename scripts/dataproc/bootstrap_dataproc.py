import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def bootstrap_dataproc(data_assets_path: str) -> None:
    os.environ["PIPELINE_MODE"] = "dataproc"
    os.environ["DATA_ASSETS_CONFIG_PATH"] = data_assets_path

    import src.utils.job_runtime_dataproc as job_runtime_dataproc
    import src.utils.config_loader_dataproc as config_loader_dataproc

    sys.modules["src.utils.job_runtime"] = job_runtime_dataproc
    sys.modules["src.utils.config_loader"] = config_loader_dataproc


def patch_job_create_spark_session(job_cls, fallback_prefix: str):
    from pyspark.sql import SparkSession

    def _patched_create_spark_session(self):
        spark_config = getattr(self, "config", {}).get("spark", {})

        dataset_name = "job"
        if hasattr(self, "_get_dataset_name"):
            try:
                dataset_name = self._get_dataset_name()
            except Exception:
                dataset_name = "job"
        elif hasattr(self, "dataset_name"):
            dataset_name = getattr(self, "dataset_name")

        app_name = spark_config.get(
            "app_name",
            f"{fallback_prefix}-{dataset_name}",
        )

        builder = SparkSession.builder.appName(app_name)

        driver_memory = spark_config.get("driver_memory")
        if driver_memory:
            builder = builder.config(
                "spark.driver.memory",
                str(driver_memory),
            )

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

        compression = spark_config.get("compression")
        if compression:
            builder = builder.config(
                "spark.sql.parquet.compression.codec",
                str(compression),
            )

        return builder.getOrCreate()

    job_cls._create_spark_session = _patched_create_spark_session
    return job_cls