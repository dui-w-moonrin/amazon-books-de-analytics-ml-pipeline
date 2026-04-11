from pathlib import Path
from typing import Any

import duckdb

from src.utils.config_loader import get_resolved_asset_path
from src.utils.job_runtime import resolve_path


class BronzeIngestionJob:
    def __init__(
        self,
        project_root: Path,
        duckdb_db_path: str,
        config: dict[str, Any],
    ):
        self.project_root = project_root
        self.duckdb_db_path = duckdb_db_path
        self.config = config

    def _resolve_asset_or_path(
        self,
        dataset_key_field: str,
        asset_key_field: str,
        fallback_path_field: str,
    ) -> Path:
        dataset_name = self.config.get(dataset_key_field)
        asset_name = self.config.get(asset_key_field)

        if dataset_name and asset_name:
            return get_resolved_asset_path(
                project_root=self.project_root,
                dataset_name=dataset_name,
                asset_name=asset_name,
            )

        raw_path = self.config.get(fallback_path_field)
        if raw_path:
            return resolve_path(self.project_root, raw_path)

        raise KeyError(
            f"Missing config keys: either "
            f"({dataset_key_field}, {asset_key_field}) "
            f"or {fallback_path_field} must be provided."
        )

    def _resolve_source_path(self) -> Path:
        return self._resolve_asset_or_path(
            dataset_key_field="source_dataset",
            asset_key_field="source_asset",
            fallback_path_field="source_path",
        )

    def _resolve_output_path(self) -> Path:
        return self._resolve_asset_or_path(
            dataset_key_field="output_dataset",
            asset_key_field="output_asset",
            fallback_path_field="output_path",
        )

    def _sql_escape(self, path: Path) -> str:
        return str(path).replace("'", "''")

    def _sql_bool(self, value: bool) -> str:
        return "true" if value else "false"

    def _get_csv_option(self, key: str, default: Any) -> Any:
        return self.config.get("csv_options", {}).get(key, default)

    def _get_source_columns(self) -> list[str]:
        return [col["source"] for col in self.config["columns"]]

    def _get_target_columns(self) -> list[str]:
        return [col["target"] for col in self.config["columns"]]

    def _validate_job_config(self) -> None:
        source_type = self.config.get("source_type", "csv")
        output_type = self.config.get("output_type", "parquet")

        if source_type != "csv":
            raise ValueError(f"Unsupported bronze source_type: {source_type}")

        if output_type != "parquet":
            raise ValueError(f"Unsupported bronze output_type: {output_type}")

        source_cols = self._get_source_columns()
        target_cols = self._get_target_columns()

        if len(source_cols) != len(target_cols):
            raise ValueError("Source and target column counts do not match")

        if len(set(source_cols)) != len(source_cols):
            raise ValueError("Duplicate source column names found in config")

        if len(set(target_cols)) != len(target_cols):
            raise ValueError("Duplicate target column names found in config")

    def _fetch_actual_columns(
        self,
        con: duckdb.DuckDBPyConnection,
        source_path: Path,
    ) -> list[str]:
        source_sql = self._sql_escape(source_path)
        header_sql = self._sql_bool(self._get_csv_option("header", True))
        all_varchar_sql = self._sql_bool(
            self._get_csv_option("all_varchar", True)
        )

        rows = con.execute(
            f"""
            DESCRIBE
            SELECT *
            FROM read_csv_auto(
                '{source_sql}',
                header = {header_sql},
                all_varchar = {all_varchar_sql}
            )
            """
        ).fetchall()

        return [row[0] for row in rows]

    def _validate_source_schema(self, actual_columns: list[str]) -> None:
        expected_columns = self._get_source_columns()
        missing = [col for col in expected_columns if col not in actual_columns]

        if missing:
            raise ValueError(f"Missing source columns: {missing}")

    def _build_select_clause(self) -> str:
        lines = []

        for col in self.config["columns"]:
            source = col["source"]
            target = col["target"]
            lines.append(f'"{source}" AS "{target}"')

        return ",\n                    ".join(lines)

    def run(self) -> None:
        self._validate_job_config()

        source_path = self._resolve_source_path()
        output_path = self._resolve_output_path()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        header_sql = self._sql_bool(self._get_csv_option("header", True))
        all_varchar_sql = self._sql_bool(self._get_csv_option("all_varchar", True))

        con = duckdb.connect(self.duckdb_db_path)

        try:
            actual_columns = self._fetch_actual_columns(con, source_path)
            self._validate_source_schema(actual_columns)

            source_sql = self._sql_escape(source_path)
            output_sql = self._sql_escape(output_path)
            select_clause = self._build_select_clause()

            con.execute(
                f"""
                CREATE OR REPLACE VIEW vw_bronze_output AS
                SELECT
                    {select_clause}
                FROM read_csv_auto(
                    '{source_sql}',
                    header = {header_sql},
                    all_varchar = {all_varchar_sql}
                )
                """
            )

            con.execute(
                f"""
                COPY (
                    SELECT * FROM vw_bronze_output
                )
                TO '{output_sql}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )

            print("DONE")
            print(f"source_path={source_path}")
            print(f"output_path={output_path}")
            print("columns=" + ", ".join(self._get_target_columns()))

        finally:
            con.close()