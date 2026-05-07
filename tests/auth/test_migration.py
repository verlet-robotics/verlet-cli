"""CLIAUTH-08 — legacy ``~/.verlet/token.json`` migration."""

import json

from verlet.auth import credentials as creds
from verlet.auth.migration import migrate_legacy_token_json


def test_legacy_migration(tmp_home):
    """Synthetic legacy token.json migrates losslessly to the default profile."""
    legacy = tmp_home / ".verlet" / "token.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(
        json.dumps(
            {
                "token": "showcase-jwt-abc",
                "customer_name": "Acme Robotics",
                "api_url": "https://api.verlet.co",
            }
        )
    )

    # First call performs the migration.
    assert migrate_legacy_token_json() is True

    new = creds.credentials_path()
    assert new.exists()
    doc = json.loads(new.read_text())
    assert doc["version"] == 1
    assert doc["default_profile"] == "default"
    default = doc["profiles"]["default"]
    assert default["kind"] == "showcase_access_code"
    assert default["access_token"] == "showcase-jwt-abc"
    assert default["customer_name"] == "Acme Robotics"
    assert default["api_url"] == "https://api.verlet.co"
    assert "migrated_at" in default

    # Lossless: legacy file still present after migration.
    assert legacy.exists()

    # Idempotent: second call is a no-op once credentials.json exists.
    assert migrate_legacy_token_json() is False
