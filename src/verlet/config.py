"""Configuration and credential management."""
import json
from pathlib import Path

CONFIG_DIR = Path.home() / ".verlet"
TOKEN_FILE = CONFIG_DIR / "token.json"
DEFAULT_API_URL = "https://api.verlet.co"


def get_api_url() -> str:
    return _load_config().get("api_url", DEFAULT_API_URL)


def get_token() -> str | None:
    config = _load_config()
    return config.get("token")


def save_credentials(token: str, customer_name: str, api_url: str | None = None) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    data = {"token": token, "customer_name": customer_name}
    if api_url:
        data["api_url"] = api_url
    TOKEN_FILE.write_text(json.dumps(data, indent=2))


def _load_config() -> dict:
    if not TOKEN_FILE.exists():
        return {}
    try:
        return json.loads(TOKEN_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


# ---------------------------------------------------------------------------
# Plan 30-10 — ~/.verlet/config.json (telemetry + non-credential CLI config).
#
# This is INTENTIONALLY separate from credentials.json (Phase 28) and from
# the legacy token.json above. Per D-DIST1: telemetry preference is local
# CLI state, not a credential, and lives in its own file with 0o600 mode.
# ---------------------------------------------------------------------------

def _config_path() -> Path:
    """Resolve ``~/.verlet/config.json`` lazily so tests can override Path.home().

    Module-level evaluation would freeze the developer's real home path
    at import time, breaking every test that uses the ``tmp_home`` fixture
    (which monkeypatches ``Path.home`` AFTER import).
    """
    return Path.home() / ".verlet" / "config.json"


# Back-compat name for callers that imported the module-level constant
# (none at the time of writing, but cheap to keep).
def __getattr__(name: str):
    if name == "CONFIG_PATH":
        return _config_path()
    raise AttributeError(name)


def load_config() -> dict:
    """Read ``~/.verlet/config.json`` -> dict; empty dict on absent / corrupt."""
    path = _config_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_config(data: dict) -> None:
    """Write ``data`` to ``~/.verlet/config.json`` with mode 0o600 (Pitfall 7).

    Mirrors the credentials.json file-mode invariant from Phase 28: 0o600
    means owner-only RW. We mkdir(parents=True, exist_ok=True) first so a
    fresh machine works without prior `verlet auth login` setup.
    """
    import os as _os

    path = _config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    _os.chmod(path, 0o600)
