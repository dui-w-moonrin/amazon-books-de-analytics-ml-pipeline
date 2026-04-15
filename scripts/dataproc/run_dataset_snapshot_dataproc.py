import argparse

from scripts.dataproc.bootstrap_dataproc import bootstrap_dataproc, patch_job_create_spark_session


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--asset", required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--input-format", default="parquet")
    parser.add_argument("--sample-rows", type=int, default=5)
    parser.add_argument("--data-assets-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    bootstrap_dataproc(args.data_assets_path)

    from src.jobs.dataset_snapshot import DatasetSnapshotJob

    patch_job_create_spark_session(
        DatasetSnapshotJob,
        "dataset-snapshot",
    )

    project_root = __import__("pathlib").Path(__file__).resolve().parents[2]

    job = DatasetSnapshotJob(
        project_root=project_root,
        dataset_name=args.dataset,
        asset_name=args.asset,
        stage_name=args.stage,
        input_format=args.input_format,
        sample_rows=args.sample_rows,
    )
    job.run()


if __name__ == "__main__":
    main()