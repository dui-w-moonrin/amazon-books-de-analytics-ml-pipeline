from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ConfigDrivenServingTransformer:
    """
    Generic serving-layer transformer.

    Current supported flow:
    - choose one base input
    - optional joins
    - optional null filters
    - final select
    - optional deduplication

    DA and DS can share the same transformer.
    The difference should come from config only.
    """

    def __init__(
        self,
        input_dfs: dict[str, DataFrame],
        config: dict[str, Any],
    ):
        self.input_dfs = input_dfs
        self.config = config
        self.serving_config = config.get("serving", {})

        self._validate_config()

    def _validate_config(self) -> None:
        base_input = self.serving_config.get("base_input")
        if not base_input:
            raise ValueError("serving.base_input is required")

        if base_input not in self.input_dfs:
            raise ValueError(
                f"serving.base_input '{base_input}' not found in input_dfs. "
                f"Available: {sorted(self.input_dfs.keys())}"
            )

        select_columns = self.serving_config.get("select_columns", [])
        if not select_columns:
            raise ValueError("serving.select_columns must not be empty")

    def transform(self) -> DataFrame:
        base_input = self.serving_config["base_input"]
        df = self.input_dfs[base_input]

        df = self._apply_joins(df)
        df = self._apply_filters(df)
        df = self._apply_select(df)
        df = self._apply_deduplicate(df)

        return df

    def _apply_joins(self, df: DataFrame) -> DataFrame:
        joins = self.serving_config.get("joins", [])

        for join_cfg in joins:
            right_input = join_cfg["right_input"]
            right_df = self.input_dfs[right_input]

            right_select_columns = join_cfg.get("right_select_columns")
            if right_select_columns:
                right_df = right_df.select(*right_select_columns)

            left_on = join_cfg["left_on"]
            right_on = join_cfg["right_on"]
            how = join_cfg.get("how", "left")

            if len(left_on) != len(right_on):
                raise ValueError(
                    f"Join key length mismatch: left_on={left_on}, right_on={right_on}"
                )

            # Same join key names on both sides:
            # use list-based join so Spark keeps only one copy of the join keys.
            if left_on == right_on:
                df = df.join(right_df, on=left_on, how=how)
                continue

            # Different key names on left/right:
            # rename right-side join keys temporarily to avoid ambiguity.
            renamed_right_df = right_df
            renamed_right_keys: list[str] = []

            for right_col in right_on:
                renamed_col = f"{right_col}__right_join_key"
                renamed_right_df = renamed_right_df.withColumnRenamed(
                    right_col,
                    renamed_col,
                )
                renamed_right_keys.append(renamed_col)

            join_condition = None
            for left_col, renamed_right_col in zip(left_on, renamed_right_keys):
                cond = df[left_col] == renamed_right_df[renamed_right_col]
                join_condition = cond if join_condition is None else (join_condition & cond)

            df = df.join(renamed_right_df, on=join_condition, how=how)

            if renamed_right_keys:
                df = df.drop(*renamed_right_keys)

        return df

    def _apply_filters(self, df: DataFrame) -> DataFrame:
        filters = self.serving_config.get("filters", [])

        for rule in filters:
            column = rule["column"]
            condition = rule["condition"]

            if condition == "is_not_null":
                df = df.filter(F.col(column).isNotNull())
            elif condition == "is_null":
                df = df.filter(F.col(column).isNull())
            else:
                raise ValueError(f"Unsupported filter condition: {condition}")

        return df

    def _apply_select(self, df: DataFrame) -> DataFrame:
        select_columns = self.serving_config["select_columns"]
        missing = [col for col in select_columns if col not in df.columns]
        if missing:
            raise ValueError(
                f"Missing select columns: {missing}. Available columns: {sorted(df.columns)}"
            )

        return df.select(*select_columns)

    def _apply_deduplicate(self, df: DataFrame) -> DataFrame:
        deduplicate_on = self.serving_config.get("deduplicate_on", [])
        if deduplicate_on:
            df = df.dropDuplicates(deduplicate_on)

        return df