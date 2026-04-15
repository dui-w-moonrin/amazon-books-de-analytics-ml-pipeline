from pyspark.sql import SparkSession


def build_spark_session_dataproc(
    app_name: str,
    spark_config: dict | None = None,
) -> SparkSession:
    spark_config = spark_config or {}

    builder = SparkSession.builder.appName(app_name)

    if "driver_memory" in spark_config:
        builder = builder.config(
            "spark.driver.memory",
            str(spark_config["driver_memory"]),
        )

    if "shuffle_partitions" in spark_config:
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(spark_config["shuffle_partitions"]),
        )

    if "default_parallelism" in spark_config:
        builder = builder.config(
            "spark.default.parallelism",
            str(spark_config["default_parallelism"]),
        )

    if "max_partition_bytes" in spark_config:
        builder = builder.config(
            "spark.sql.files.maxPartitionBytes",
            str(spark_config["max_partition_bytes"]),
        )

    if "compression" in spark_config:
        builder = builder.config(
            "spark.sql.parquet.compression.codec",
            str(spark_config["compression"]),
        )

    return builder.getOrCreate()