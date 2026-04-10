from pyspark.sql import DataFrame

from src.transformers.config_driven_column_transformer import (
    ConfigDrivenColumnTransformer,
)


def standardize_silver(df: DataFrame, config: dict) -> DataFrame:
    return ConfigDrivenColumnTransformer(df, config).transform()