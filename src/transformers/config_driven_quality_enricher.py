from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ConfigDrivenQualityEnricher:
    """
    Add quality-related derived columns to a Spark DataFrame.

    Responsibilities:
    - validate quality enrichment rules
    - generate hash columns
    - generate boolean quality flags
    - generate score columns from configured flag sets

    Inputs:
    - input Spark DataFrame
    - quality enrichment config

    Output:
    - enriched Spark DataFrame with quality helper columns
    """
    def __init__(self, df: DataFrame, config: dict[str, Any]):
        self.df = df
        self.config = config
        self.hash_rules: list[dict[str, Any]] = config.get("hash_rules", [])
        self.flag_rules: list[dict[str, Any]] = config.get("flag_rules", [])
        self.score_rules: list[dict[str, Any]] = config.get("score_rules", [])

        self._validate_config()

    def _validate_config(self) -> None:
        available_columns = set(self.df.columns)
        target_columns: list[str] = []

        for i, rule in enumerate(self.hash_rules, start=1):
            source = rule.get("source")
            target = rule.get("target")
            method = rule.get("method")

            if not source:
                raise ValueError(
                    f"Hash rule #{i} missing 'source': {rule}"
                )

            if not target:
                raise ValueError(
                    f"Hash rule #{i} missing 'target': {rule}"
                )

            if source not in available_columns:
                raise ValueError(
                    f"Hash rule #{i}: source column '{source}' not found. "
                    f"Available columns: {sorted(available_columns)}"
                )

            if method != "xxhash64":
                raise ValueError(
                    f"Hash rule #{i}: unsupported method '{method}'"
                )

            target_columns.append(target)
            available_columns.add(target)

        for i, rule in enumerate(self.flag_rules, start=1):
            source = rule.get("source")
            target = rule.get("target")
            flag_type = rule.get("rule")

            if not source:
                raise ValueError(
                    f"Flag rule #{i} missing 'source': {rule}"
                )

            if not target:
                raise ValueError(
                    f"Flag rule #{i} missing 'target': {rule}"
                )

            if source not in available_columns:
                raise ValueError(
                    f"Flag rule #{i}: source column '{source}' not found. "
                    f"Available columns: {sorted(available_columns)}"
                )

            if flag_type != "is_not_null":
                raise ValueError(
                    f"Flag rule #{i}: unsupported rule '{flag_type}'"
                )

            target_columns.append(target)
            available_columns.add(target)

        for i, rule in enumerate(self.score_rules, start=1):
            target = rule.get("target")
            method = rule.get("method")
            sources = rule.get("sources", [])

            if not target:
                raise ValueError(
                    f"Score rule #{i} missing 'target': {rule}"
                )

            if method != "sum_flags":
                raise ValueError(
                    f"Score rule #{i}: unsupported method '{method}'"
                )

            if not sources:
                raise ValueError(
                    f"Score rule #{i}: 'sources' must not be empty"
                )

            missing_sources = [col for col in sources if col not in available_columns]
            if missing_sources:
                raise ValueError(
                    f"Score rule #{i}: source columns not found: {missing_sources}"
                )

            target_columns.append(target)
            available_columns.add(target)

        if len(set(target_columns)) != len(target_columns):
            raise ValueError("Duplicate target column names found in quality config")

    def transform(self) -> DataFrame:
        result_df = self.df

        for rule in self.hash_rules:
            result_df = result_df.withColumn(
                rule["target"],
                self._build_hash_expression(rule),
            )

        for rule in self.flag_rules:
            result_df = result_df.withColumn(
                rule["target"],
                self._build_flag_expression(rule),
            )

        for rule in self.score_rules:
            result_df = result_df.withColumn(
                rule["target"],
                self._build_score_expression(rule),
            )

        return result_df

    def preview(self, n: int = 5) -> None:
        self.transform().show(n, truncate=False)

    def _build_hash_expression(self, rule: dict[str, Any]):
        source_col = F.col(rule["source"])

        return (
            F.when(source_col.isNull(), F.lit(None).cast("long"))
            .otherwise(F.xxhash64(source_col))
            .cast("long")
        )

    def _build_flag_expression(self, rule: dict[str, Any]):
        source_col = F.col(rule["source"])

        return F.when(source_col.isNotNull(), F.lit(True)).otherwise(F.lit(False))

    def _build_score_expression(self, rule: dict[str, Any]):
        sources = rule["sources"]

        score_expr = None
        for source in sources:
            current = F.when(F.col(source) == F.lit(True), F.lit(1)).otherwise(F.lit(0))
            score_expr = current if score_expr is None else (score_expr + current)

        return score_expr.cast("int")