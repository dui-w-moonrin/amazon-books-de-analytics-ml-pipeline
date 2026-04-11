from pathlib import Path

from src.utils.job_runtime import load_json_file, resolve_path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "data_assets.json"


def load_data_assets() -> dict:
    return load_json_file(CONFIG_PATH)


def get_asset_path(dataset_name: str, asset_name: str) -> str:
    assets = load_data_assets()

    if dataset_name not in assets:
        raise KeyError(f"Dataset not found: {dataset_name}")

    dataset_assets = assets[dataset_name]

    if asset_name not in dataset_assets:
        raise KeyError(
            f"Asset '{asset_name}' not found for dataset '{dataset_name}'"
        )

    return dataset_assets[asset_name]


def get_resolved_asset_path(
    project_root: Path,
    dataset_name: str,
    asset_name: str,
) -> Path:
    raw_path = get_asset_path(dataset_name, asset_name)
    return resolve_path(project_root, raw_path)