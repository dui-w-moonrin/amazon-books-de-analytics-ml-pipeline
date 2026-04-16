import argparse
from pathlib import Path

from scripts.dataproc.bootstrap_dataproc import (
    bootstrap_dataproc,
    patch_job_create_spark_session,
)
from src.utils.job_runtime_dataproc import (
    load_json_file,
    resolve_config_path,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run gold serving job on Dataproc"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--data-assets-path", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bootstrap_dataproc(args.data_assets_path)

    from src.jobs.gold_serve import GoldServeJob

    patch_job_create_spark_session(
        GoldServeJob,
        "gold-serve",
    )

    project_root = Path(__file__).resolve().parents[2]
    config_path = resolve_config_path(project_root, args.config)
    config = load_json_file(config_path)

    job = GoldServeJob(
        project_root=project_root,
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()