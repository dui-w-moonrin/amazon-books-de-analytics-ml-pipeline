from pathlib import Path
from typing import Dict, List

import duckdb

from src.utils.config_loader import get_asset_path


class BronzeIngestionJob:
    def __init__(self, project_root: Path, duckdb_db_path: str, config: Dict):
        self.project_root = project_root
        self.duckdb_db_path = duckdb_db_path
        self.config = config

    def _resolve_path(self, raw_path: str) -> Path:
        path = Path(raw_path)
        if not path.is_absolute():
            path = self.project_root / path
        return path.resolve()

    def _resolve_asset_path(
        self,
        dataset_key_field: str,
        asset_key_field: str,
        fallback_path_field: str,
    ) -> Path:
        dataset_name = self.config.get(dataset_key_field)
        asset_name = self.config.get(asset_key_field)

        if dataset_name and asset_name:
            asset_path = get_asset_path(dataset_name, asset_name)
            return self._resolve_path(asset_path)

        raw_path = self.config.get(fallback_path_field)
        if raw_path:
            return self._resolve_path(raw_path)

        raise KeyError(
            f"Missing config keys: either "
            f"({dataset_key_field}, {asset_key_field}) "
            f"or {fallback_path_field} must be provided."
        )

    def _resolve_source_path(self) -> Path:
        return self._resolve_asset_path(
            dataset_key_field="source_dataset",
            asset_key_field="source_asset",
            fallback_path_field="source_path",
        )

    def _resolve_output_path(self) -> Path:
        return self._resolve_asset_path(
            dataset_key_field="output_dataset",
            asset_key_field="output_asset",
            fallback_path_field="output_path",
        )

    def _sql_escape(self, path: Path) -> str:
        return str(path).replace("'", "''")

    def _sql_bool(self, value: bool) -> str:
        return "true" if value else "false"

    def _get_csv_option(self, key: str, default):
        return self.config.get("csv_options", {}).get(key, default)

    def _get_source_columns(self) -> List[str]:
        return [col["source"] for col in self.config["columns"]]

    def _get_target_columns(self) -> List[str]:
        return [col["target"] for col in self.config["columns"]]

    def _validate_column_config(self) -> None:
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
    ) -> List[str]:
        source_sql = self._sql_escape(source_path)
        header_sql = self._sql_bool(self._get_csv_option("header", True))
        all_varchar_sql = self._sql_bool(self._get_csv_option("all_varchar", True))

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

    def _validate_source_schema(
        self,
        actual_columns: List[str],
    ) -> None:
        expected_columns = self._get_source_columns()
        missing = [col for col in expected_columns if col not in actual_columns]

        if missing:
            raise ValueError(f"Missing source columns: {missing}")

    def _build_select_clause(self) -> str:
        lines = []
        for col in self.config["columns"]:
            lines.append(f'"{col["source"]}" AS {col["target"]}')
        return ",\n                    ".join(lines)

    def run(self) -> None:
        self._validate_column_config()

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