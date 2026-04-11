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


def resolve_config_path(project_root: Path, config_arg: str) -> Path:
    config_dir_raw = os.getenv("CONFIG_DIR", "config")
    config_dir = resolve_path(project_root, config_dir_raw)

    config_path = Path(config_arg)

    if not config_path.is_absolute():
        if config_path.parent == Path("."):
            config_path = config_dir / config_path
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