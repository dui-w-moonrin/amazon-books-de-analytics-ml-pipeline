import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.gold_serve import GoldServeJob
from src.utils.job_runtime import load_json_file, resolve_config_path, load_simple_env


def main() -> None:
    load_simple_env(PROJECT_ROOT / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to JSON config")
    args = parser.parse_args()

    config_path = resolve_config_path(PROJECT_ROOT, args.config)
    config = load_json_file(config_path)

    job = GoldServeJob(
        project_root=PROJECT_ROOT,
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()