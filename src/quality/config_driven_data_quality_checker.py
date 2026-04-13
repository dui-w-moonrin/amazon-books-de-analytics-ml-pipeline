from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ConfigDrivenDataQualityChecker:
    def __init__(self, df: DataFrame, config: dict[str, Any]):
        self.df = df
        self.config = config
        self.dataset_name: str = config.get("dataset_name", "unknown_dataset")
        self.checks: list[dict[str, Any]] = config.get("checks", [])

        self._validate_config()

    def _validate_config(self) -> None:
        if not self.checks:
            raise ValueError("Config must contain at least one check in 'checks'.")

        available_columns = set(self.df.columns)

        for i, check in enumerate(self.checks, start=1):
            name = check.get("name")
            check_type = check.get("type")
            severity = check.get("severity")

            if not name:
                raise ValueError(f"Check #{i} missing 'name': {check}")

            if not check_type:
                raise ValueError(f"Check #{i} missing 'type': {check}")

            if severity not in {"critical", "warning"}:
                raise ValueError(
                    f"Check #{i}: unsupported severity '{severity}'"
                )

            if check_type in {"not_null", "non_negative"}:
                column = check.get("column")
                if not column:
                    raise ValueError(
                        f"Check #{i}: '{check_type}' requires 'column'"
                    )
                if column not in available_columns:
                    raise ValueError(
                        f"Check #{i}: column '{column}' not found. "
                        f"Available columns: {sorted(available_columns)}"
                    )

            elif check_type == "value_in_set":
                column = check.get("column")
                allowed_values = check.get("allowed_values", [])

                if not column:
                    raise ValueError(
                        f"Check #{i}: 'value_in_set' requires 'column'"
                    )
                if column not in available_columns:
                    raise ValueError(
                        f"Check #{i}: column '{column}' not found. "
                        f"Available columns: {sorted(available_columns)}"
                    )
                if not allowed_values:
                    raise ValueError(
                        f"Check #{i}: 'value_in_set' requires non-empty 'allowed_values'"
                    )

            elif check_type == "not_in_future":
                column = check.get("column")
                if not column:
                    raise ValueError(
                        f"Check #{i}: 'not_in_future' requires 'column'"
                    )
                if column not in available_columns:
                    raise ValueError(
                        f"Check #{i}: column '{column}' not found. "
                        f"Available columns: {sorted(available_columns)}"
                    )

            elif check_type == "requires_when_present":
                required_column = check.get("required_column")
                when_present_column = check.get("when_present_column")

                if not required_column or not when_present_column:
                    raise ValueError(
                        f"Check #{i}: 'requires_when_present' requires "
                        f"'required_column' and 'when_present_column'"
                    )

                for column in [required_column, when_present_column]:
                    if column not in available_columns:
                        raise ValueError(
                            f"Check #{i}: column '{column}' not found. "
                            f"Available columns: {sorted(available_columns)}"
                        )

            elif check_type == "duplicate_key":
                key_columns = check.get("key_columns", [])
                if not key_columns:
                    raise ValueError(
                        f"Check #{i}: 'duplicate_key' requires non-empty 'key_columns'"
                    )
                missing = [c for c in key_columns if c not in available_columns]
                if missing:
                    raise ValueError(
                        f"Check #{i}: key columns not found: {missing}"
                    )

            else:
                raise ValueError(
                    f"Check #{i}: unsupported type '{check_type}'"
                )

    def run_checks(self) -> tuple[list[dict[str, Any]], dict[str, DataFrame]]:
        total_rows = self.df.count()
        results: list[dict[str, Any]] = []
        failed_samples: dict[str, DataFrame] = {}

        for check in self.checks:
            failed_df = self._build_failed_df(check)
            failed_count = failed_df.count()
            passed_count = total_rows - failed_count
            status = "passed" if failed_count == 0 else "failed"

            results.append(
                {
                    "dataset_name": self.dataset_name,
                    "check_name": check["name"],
                    "check_type": check["type"],
                    "severity": check["severity"],
                    "status": status,
                    "total_rows": total_rows,
                    "passed_rows": passed_count,
                    "failed_rows": failed_count,
                    "failed_pct": round(
                        (failed_count / total_rows * 100), 4
                    ) if total_rows > 0 else 0.0,
                }
            )

            if failed_count > 0:
                sample_limit = int(check.get("sample_limit", 100))
                failed_samples[check["name"]] = failed_df.limit(sample_limit)

        return results, failed_samples

    def _build_failed_df(self, check: dict[str, Any]) -> DataFrame:
        check_type = check["type"]

        if check_type == "not_null":
            column = check["column"]
            return self.df.filter(F.col(column).isNull())

        if check_type == "non_negative":
            column = check["column"]
            return self.df.filter(
                F.col(column).isNotNull() & (F.col(column) < F.lit(0))
            )

        if check_type == "value_in_set":
            column = check["column"]
            allowed_values = check["allowed_values"]
            return self.df.filter(~F.col(column).isin(allowed_values))

        if check_type == "not_in_future":
            column = check["column"]
            return self.df.filter(
                F.col(column).isNotNull() & (F.col(column) > F.current_date())
            )

        if check_type == "requires_when_present":
            required_column = check["required_column"]
            when_present_column = check["when_present_column"]
            return self.df.filter(
                F.col(when_present_column).isNotNull()
                & F.col(required_column).isNull()
            )

        if check_type == "duplicate_key":
            key_columns = check["key_columns"]
            dup_keys_df = (
                self.df.groupBy(*key_columns)
                .count()
                .filter(F.col("count") > 1)
                .drop("count")
            )
            return self.df.join(dup_keys_df, on=key_columns, how="inner")

        raise ValueError(f"Unsupported check type: {check_type}")