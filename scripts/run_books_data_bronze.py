import json
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.jobs.bronze_ingestion import BronzeIngestionJob


def load_simple_env(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def resolve_path(project_root: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def main() -> None:
    load_simple_env(project_root / ".env")

    config_path_raw = os.getenv("BRONZE_JOB_CONFIG")
    if not config_path_raw:
        raise ValueError("Missing BRONZE_JOB_CONFIG in .env")

    duckdb_db_path = os.getenv("DUCKDB_DATABASE_PATH", ":memory:")

    config_path = resolve_path(project_root, config_path_raw)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    job = BronzeIngestionJob(
        project_root=project_root,
        duckdb_db_path=duckdb_db_path,
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()