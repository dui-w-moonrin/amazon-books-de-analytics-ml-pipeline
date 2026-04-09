import json
from pathlib import Path
from typing import Dict, List

import duckdb


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

    def _sql_escape(self, path: Path) -> str:
        return str(path).replace("'", "''")

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

        rows = con.execute(
            f"""
            DESCRIBE
            SELECT *
            FROM read_csv_auto(
                '{source_sql}',
                header = true,
                all_varchar = true
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
        return ",\n                ".join(lines)

    def run(self) -> None:
        self._validate_column_config()

        source_path = self._resolve_path(self.config["source_path"])
        output_path = self._resolve_path(self.config["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)

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
                    header = true,
                    all_varchar = true
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