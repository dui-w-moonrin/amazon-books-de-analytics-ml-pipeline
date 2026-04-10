from typing import Any, Dict, List, Set

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class ConfigDrivenColumnTransformer:
    """
    Simple config-driven DataFrame transformer.

    Scope v2:
    - trim
    - blank_to_null
    - collapse_spaces
    - lower
    - cast
    - cast_via
    - multi-format date parse
    - optional surrogate key generation
    - optional drop intermediate columns
    """

    def __init__(self, df: DataFrame, config: Dict[str, Any]):
        self.df = df
        self.config = config
        self.rules: List[Dict[str, Any]] = config.get("column_rules", [])
        self.output_mode: str = config.get("output_mode", "append")
        self.passthrough_columns: List[str] = config.get("passthrough_columns", [])
        self.surrogate_key_config: Dict[str, Any] = config.get("surrogate_key", {})
        self.drop_columns_after_transform: List[str] = config.get("drop_columns_after_transform", [])
        self._validate_config()

    def _validate_config(self) -> None:
        if not self.rules:
            raise ValueError("Config must contain at least one rule in 'column_rules'.")

        if self.output_mode not in {"append", "select_targets_only"}:
            raise ValueError("output_mode must be either 'append' or 'select_targets_only'.")

        available_columns: Set[str] = set(self.df.columns)

        for i, rule in enumerate(self.rules, start=1):
            source = rule.get("source")
            target = rule.get("target")

            if not source:
                raise ValueError(f"Rule #{i} missing 'source': {rule}")
            if not target:
                raise ValueError(f"Rule #{i} missing 'target': {rule}")

            if source not in available_columns:
                raise ValueError(
                    f"Rule #{i}: source column '{source}' not available yet. "
                    f"Available columns: {sorted(available_columns)}"
                )

            available_columns.add(target)

        if self.surrogate_key_config:
            sk_source = self.surrogate_key_config.get("source")
            sk_target = self.surrogate_key_config.get("target")

            if not sk_source or not sk_target:
                raise ValueError(
                    "surrogate_key config must contain 'source' and 'target'."
                )

    def transform(self) -> DataFrame:
        result_df = self.df

        # 1) apply column rules
        for rule in self.rules:
            expr = self._build_expression(rule)
            result_df = result_df.withColumn(rule["target"], expr)

        # 2) generate surrogate key if requested
        if self.surrogate_key_config:
            result_df = self._apply_surrogate_key(result_df)

        # 3) drop columns if requested
        if self.drop_columns_after_transform:
            existing_drop_cols = [c for c in self.drop_columns_after_transform if c in result_df.columns]
            if existing_drop_cols:
                result_df = result_df.drop(*existing_drop_cols)

        # 4) final select mode
        if self.output_mode == "select_targets_only":
            target_columns = [rule["target"] for rule in self.rules]

            if self.surrogate_key_config:
                target_columns.append(self.surrogate_key_config["target"])

            final_columns = self._deduplicate_preserve_order(self.passthrough_columns + target_columns)
            final_columns = [c for c in final_columns if c in result_df.columns]
            result_df = result_df.select(*final_columns)

        return result_df

    def preview(self, n: int = 5) -> None:
        self.transform().show(n, truncate=False)

    @staticmethod
    def _deduplicate_preserve_order(items: List[str]) -> List[str]:
        seen = set()
        result = []
        for item in items:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result

    def _build_expression(self, rule: Dict[str, Any]):
        expr = F.col(rule["source"])

        # 1) string cleanup
        expr = self._apply_string_rules(expr, rule)

        # 2) parse modes
        parse_mode = rule.get("parse_mode")
        if parse_mode == "multi_format_date":
            expr = self._parse_multi_format_date(expr)
        elif parse_mode == "year_from_date":
            expr = F.when(expr.isNull(), F.lit(None)).otherwise(F.year(expr))
        elif parse_mode == "unix_seconds_to_timestamp":
            expr = self._parse_unix_seconds_to_timestamp(expr)
        elif parse_mode == "timestamp_to_date":
            expr = self._parse_timestamp_to_date(expr)

        # 3) cast
        target_type = rule.get("target_type")
        cast_via = rule.get("cast_via")

        if target_type and parse_mode not in {
            "multi_format_date",
            "year_from_date",
            "unix_seconds_to_timestamp",
            "timestamp_to_date"
        }:
            expr = self._cast_expression(expr, target_type, cast_via)

        return expr

    def _apply_string_rules(self, expr, rule: Dict[str, Any]):
        if rule.get("trim", False):
            expr = F.trim(expr)

        if rule.get("collapse_spaces", False):
            expr = F.regexp_replace(expr, r"\s+", " ")

        if rule.get("trim", False):
            expr = F.trim(expr)

        if rule.get("blank_to_null", False):
            expr = F.when(expr.isNull() | (expr == ""), F.lit(None)).otherwise(expr)

        if rule.get("lower", False):
            expr = F.when(expr.isNull(), F.lit(None)).otherwise(F.lower(expr))

        return expr

    def _parse_multi_format_date(self, expr):
        return (
            F.when(expr.isNull(), F.lit(None).cast("date"))
             .when(expr.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(expr, "yyyy-MM-dd"))
             .when(expr.rlike(r"^\d{4}-\d{2}$"), F.to_date(F.concat(expr, F.lit("-01")), "yyyy-MM-dd"))
             .when(expr.rlike(r"^\d{4}$"), F.to_date(F.concat(expr, F.lit("-01-01")), "yyyy-MM-dd"))
             .otherwise(F.lit(None).cast("date"))
        )

    def _cast_expression(self, expr, target_type: str, cast_via: str = None):
        if cast_via:
            expr = expr.cast(cast_via)
        return expr.cast(target_type)

    def _apply_surrogate_key(self, df: DataFrame) -> DataFrame:
        sk_source = self.surrogate_key_config["source"]
        sk_target = self.surrogate_key_config["target"]
        method = self.surrogate_key_config.get("method", "row_number_sorted")
        start_at = self.surrogate_key_config.get("start_at", 1)
        require_unique_source = self.surrogate_key_config.get("require_unique_source", True)
        drop_source_after_create = self.surrogate_key_config.get("drop_source_after_create", False)

        if sk_source not in df.columns:
            raise ValueError(f"surrogate_key source column '{sk_source}' not found after transforms.")

        if method != "row_number_sorted":
            raise ValueError(f"Unsupported surrogate key method: {method}")

        # ใช้เฉพาะแถวที่ source key ไม่เป็น null มาสร้าง mapping
        eligible_df = df.filter(F.col(sk_source).isNotNull())

        # ถ้าอยาก strict เรื่อง uniqueness ก็เช็กเฉพาะ eligible rows
        if require_unique_source:
            dup_cnt = (
                eligible_df.groupBy(sk_source)
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            if dup_cnt > 0:
                raise ValueError(
                    f"Cannot generate surrogate key from '{sk_source}' because it is not unique "
                    f"among non-null rows. Found {dup_cnt} duplicated key values."
                )

        # สร้าง mapping table: title_key -> book_id
        mapping_df = eligible_df.select(sk_source).distinct()

        window_spec = Window.orderBy(F.col(sk_source))
        mapping_df = mapping_df.withColumn(
            sk_target,
            F.row_number().over(window_spec) + F.lit(start_at - 1)
        )

        # join กลับเข้าตารางเดิม
        df = df.join(mapping_df, on=sk_source, how="left")

        if drop_source_after_create:
            df = df.drop(sk_source)

        return df
    
    def _parse_unix_seconds_to_timestamp(self, expr):
         return (
        F.when(expr.isNull(), F.lit(None).cast("timestamp"))
         .otherwise(F.to_timestamp(F.from_unixtime(expr.cast("bigint"))))
        )

    def _parse_timestamp_to_date(self, expr):
        return (
        F.when(expr.isNull(), F.lit(None).cast("date"))
         .otherwise(F.to_date(expr))
        )