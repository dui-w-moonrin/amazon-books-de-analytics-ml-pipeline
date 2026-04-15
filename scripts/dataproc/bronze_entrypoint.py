import argparse
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from google.cloud import storage

from src.jobs.bronze_ingestion import BronzeIngestionJob
from src.utils.job_runtime import load_json_file


def parse_args():
    parser = argparse.ArgumentParser(
        description="Dataproc entrypoint for Bronze ingestion"
    )
    parser.add_argument("--config-uri", required=True)
    parser.add_argument("--assets-config-uri", required=True)
    return parser.parse_args()


def download_gcs_uri_to_temp(gcs_uri: str, suffix: str = ".json") -> Path:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got: {gcs_uri}")

    parsed = urlparse(gcs_uri)
    bucket_name = parsed.netloc
    blob_name = parsed.path.lstrip("/")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    fd, temp_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)

    blob.download_to_filename(temp_path)
    return Path(temp_path)


def main() -> None:
    args = parse_args()

    local_job_config = download_gcs_uri_to_temp(args.config_uri)
    local_assets_config = download_gcs_uri_to_temp(args.assets_config_uri)

    os.environ["PIPELINE_MODE"] = "dataproc"
    os.environ["DATA_ASSETS_CONFIG_PATH"] = str(local_assets_config)

    config = load_json_file(local_job_config)

    job = BronzeIngestionJob(
        project_root=Path.cwd(),
        config=config,
    )
    job.run()


if __name__ == "__main__":
    main()