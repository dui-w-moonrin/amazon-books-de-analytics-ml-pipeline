import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def bootstrap_dataproc(data_assets_path: str) -> None:
    os.environ["PIPELINE_MODE"] = "dataproc"
    os.environ["DATA_ASSETS_CONFIG_PATH"] = data_assets_path

    import src.utils.job_runtime_dataproc as job_runtime_dataproc
    import src.utils.config_loader_dataproc as config_loader_dataproc

    sys.modules["src.utils.job_runtime"] = job_runtime_dataproc
    sys.modules["src.utils.config_loader"] = config_loader_dataproc


def patch_job_create_spark_session(job_cls, fallback_prefix: str):
    from src.utils.spark_session_factory_dataproc import (
        build_spark_session_dataproc,
    )

    def _patched_create_spark_session(self):
        spark_config = getattr(self, "config", {}).get("spark", {})

        dataset_name = "job"
        if hasattr(self, "_get_dataset_name"):
            try:
                dataset_name = self._get_dataset_name()
            except Exception:
                dataset_name = "job"
        elif hasattr(self, "dataset_name"):
            dataset_name = getattr(self, "dataset_name")

        app_name = spark_config.get(
            "app_name",
            f"{fallback_prefix}-{dataset_name}",
        )

        return build_spark_session_dataproc(
            app_name=app_name,
            spark_config=spark_config,
        )

    job_cls._create_spark_session = _patched_create_spark_session
    return job_cls