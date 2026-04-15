import json
import tempfile
from pathlib import Path
from typing import Any


class _NoOpParent:
    def mkdir(self, *args, **kwargs):
        return None


class GCSPath:
    def __init__(self, raw_path: str):
        self.raw_path = raw_path

    @property
    def parent(self):
        return _NoOpParent()

    def __str__(self) -> str:
        return self.raw_path

    def __repr__(self) -> str:
        return self.raw_path


def is_gcs_path(raw_path: str | Path) -> bool:
    return str(raw_path).startswith("gs://")


def load_json_file(path: str | Path) -> dict[str, Any]:
    raw = str(path)

    if raw.startswith("gs://"):
        from google.cloud import storage

        bucket_name, blob_path = raw.replace("gs://", "", 1).split("/", 1)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        with tempfile.NamedTemporaryFile(mode="w+b", delete=True) as tmp:
            blob.download_to_filename(tmp.name)
            return json.loads(Path(tmp.name).read_text(encoding="utf-8"))

    path_obj = Path(raw)
    if not path_obj.exists():
        raise FileNotFoundError(f"Config file not found: {path_obj}")

    return json.loads(path_obj.read_text(encoding="utf-8"))


def resolve_path(base_dir: Path, raw_path: str | Path):
    raw = str(raw_path)

    if raw.startswith("gs://"):
        return GCSPath(raw)

    path = Path(raw)
    if not path.is_absolute():
        path = (base_dir / path).resolve()

    return path


def resolve_config_path(project_root: Path, config_arg: str) -> str:
    if config_arg.startswith("gs://"):
        return config_arg

    config_path = Path(config_arg)

    if not config_path.is_absolute():
        config_path = (project_root / config_path).resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return str(config_path)