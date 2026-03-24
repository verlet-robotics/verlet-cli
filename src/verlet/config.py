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
