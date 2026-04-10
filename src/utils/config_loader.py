import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "data_assets.json"


def load_data_assets() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_asset_path(dataset_name: str, asset_name: str) -> str:
    assets = load_data_assets()

    if dataset_name not in assets:
        raise KeyError(f"Dataset not found: {dataset_name}")

    dataset_assets = assets[dataset_name]

    if asset_name not in dataset_assets:
        raise KeyError(
            f"Asset '{asset_name}' not found for dataset '{dataset_name}'"
        )

    return assets[dataset_name][asset_name]