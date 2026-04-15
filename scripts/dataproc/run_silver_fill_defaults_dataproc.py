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
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--data-assets-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    bootstrap_dataproc(args.data_assets_path)

    from src.jobs.silver_fill_defaults import SilverFillDefaultsJob

    patch_job_create_spark_session(
        SilverFillDefaultsJob,
        "silver-fill-defaults",
    )

    project_root = Path(__file__).resolve().parents[2]
    config_path = resolve_config_path(project_root, args.config)
    config = load_json_file(config_path)

    job = SilverFillDefaultsJob(
        project_root=project_root,
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()