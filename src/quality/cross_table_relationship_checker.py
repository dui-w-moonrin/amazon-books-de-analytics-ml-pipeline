from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class CrossTableRelationshipChecker:
    """
    Validate key coverage between two related datasets.

    Responsibilities:
    - compare distinct relationship keys across two DataFrames
    - identify keys that exist on the left side but not on the right side
    - calculate relationship coverage metrics
    - return failed key samples for inspection

    Inputs:
    - left Spark DataFrame
    - right Spark DataFrame
    - relationship check config

    Output:
    - summary metrics for the relationship check
    - failed key sample DataFrame
    """
    def __init__(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        config: dict[str, Any],
    ):
        self.left_df = left_df
        self.right_df = right_df
        self.config = config

        self.check_name: str = config["check_name"]
        self.left_key: str = config["left_key"]
        self.right_key: str = config["right_key"]
        self.severity: str = config.get("severity", "critical")
        self.sample_limit: int = int(config.get("sample_limit", 100))

        self.left_dataset_name: str = config.get("left_dataset_name", "left_dataset")
        self.right_dataset_name: str = config.get("right_dataset_name", "right_dataset")

        self._validate_config()

    def _validate_config(self) -> None:
        if self.severity not in {"critical", "warning"}:
            raise ValueError(f"Unsupported severity: {self.severity}")

        if self.left_key not in self.left_df.columns:
            raise ValueError(
                f"Left key '{self.left_key}' not found. "
                f"Available columns: {sorted(self.left_df.columns)}"
            )

        if self.right_key not in self.right_df.columns:
            raise ValueError(
                f"Right key '{self.right_key}' not found. "
                f"Available columns: {sorted(self.right_df.columns)}"
            )

    def run_check(self) -> tuple[dict[str, Any], DataFrame]:
        left_distinct_df = (
            self.left_df
            .select(F.col(self.left_key).alias("relationship_key"))
            .filter(F.col("relationship_key").isNotNull())
            .distinct()
        )

        right_distinct_df = (
            self.right_df
            .select(F.col(self.right_key).alias("relationship_key"))
            .filter(F.col("relationship_key").isNotNull())
            .distinct()
        )

        missing_keys_df = left_distinct_df.join(
            right_distinct_df,
            on="relationship_key",
            how="left_anti",
        )

        left_total_keys = left_distinct_df.count()
        missing_keys = missing_keys_df.count()
        matched_keys = left_total_keys - missing_keys
        status = "passed" if missing_keys == 0 else "failed"

        summary_row = {
            "check_name": self.check_name,
            "left_dataset_name": self.left_dataset_name,
            "right_dataset_name": self.right_dataset_name,
            "left_key": self.left_key,
            "right_key": self.right_key,
            "severity": self.severity,
            "status": status,
            "left_total_keys": left_total_keys,
            "matched_keys": matched_keys,
            "missing_keys": missing_keys,
            "missing_pct": round((missing_keys / left_total_keys * 100), 4)
            if left_total_keys > 0 else 0.0,
        }

        failed_sample_df = missing_keys_df.limit(self.sample_limit)

        return summary_row, failed_sample_df