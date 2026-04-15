import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("PIPELINE_MODE", "local")
os.environ.setdefault("PIPELINE_CONFIG_ROOT", "config/local")
os.environ.setdefault(
    "DATA_ASSETS_CONFIG_PATH",
    "config/assets/local.data_assets.json",
)

from src.jobs.silver_cross_check_relationship import SilverCrossCheckRelationshipJob
from src.utils.job_runtime import (
    load_json_file,
    load_simple_env,
    resolve_config_path,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run silver cross-table relationship check job from config"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Config filename or relative/absolute path",
    )
    return parser.parse_args()


def main() -> None:
    load_simple_env(PROJECT_ROOT / ".env")
    args = parse_args()

    config_path = resolve_config_path(PROJECT_ROOT, args.config)
    config = load_json_file(config_path)

    job = SilverCrossCheckRelationshipJob(
        project_root=PROJECT_ROOT,
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()