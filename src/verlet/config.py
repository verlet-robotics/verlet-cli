"""Config file management for ~/.verlet/config.json."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CONFIG_DIR = Path.home() / ".verlet"
CONFIG_FILE = CONFIG_DIR / "config.json"

DEFAULT_API_BASE = "https://ego.verlet.co"


def ensure_config_dir() -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    return CONFIG_DIR


def load_config() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        return {}
    return json.loads(CONFIG_FILE.read_text())


def save_config(data: dict[str, Any]) -> None:
    ensure_config_dir()
    CONFIG_FILE.write_text(json.dumps(data, indent=2) + "\n")


def clear_config() -> None:
    if CONFIG_FILE.exists():
        CONFIG_FILE.unlink()
