import argparse
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


def resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def main() -> None:
    load_simple_env(project_root / ".env")

    parser = argparse.ArgumentParser(
        description="Run bronze ingestion job from external config"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Config filename (inside CONFIG_DIR) or relative/absolute path",
    )
    args = parser.parse_args()

    config_dir_raw = os.getenv("CONFIG_DIR", "config")
    duckdb_db_path = os.getenv("DUCKDB_DATABASE_PATH", ":memory:")

    config_dir = resolve_path(project_root, config_dir_raw)
    config_arg = Path(args.config)

    # กรณีส่งมาแค่ชื่อไฟล์ เช่น books_data_bronze.json
    if not config_arg.is_absolute():
        if config_arg.parent == Path("."):
            config_path = (config_dir / config_arg).resolve()
        else:
            config_path = (project_root / config_arg).resolve()
    else:
        config_path = config_arg.resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = json.loads(config_path.read_text(encoding="utf-8"))

    job = BronzeIngestionJob(
        project_root=project_root,
        duckdb_db_path=duckdb_db_path,
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()