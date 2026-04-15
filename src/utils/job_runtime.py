import json
import os
from pathlib import Path
from typing import Any


def load_simple_env(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def resolve_path(base_dir: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path)

    if not path.is_absolute():
        path = base_dir / path

    return path.resolve()


def get_pipeline_mode() -> str:
    return os.getenv("PIPELINE_MODE", "local").strip().lower()


def get_config_root(project_root: Path) -> Path:
    raw_path = os.getenv("PIPELINE_CONFIG_ROOT")
    if raw_path:
        return resolve_path(project_root, raw_path)

    mode = get_pipeline_mode()
    return resolve_path(project_root, f"config/{mode}")


def resolve_config_path(project_root: Path, config_arg: str) -> Path:
    config_root = get_config_root(project_root)
    config_path = Path(config_arg)

    if not config_path.is_absolute():
        if config_path.parent == Path("."):
            config_path = config_root / config_path
        else:
            config_path = project_root / config_path

    config_path = config_path.resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return config_path


def load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    return json.loads(path.read_text(encoding="utf-8"))