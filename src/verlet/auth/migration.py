"""One-shot migration of 0.4.x ``~/.verlet/token.json`` into the new schema.

The 0.4.x CLI stored a single showcase JWT at ``~/.verlet/token.json`` shaped
``{"token": "...", "customer_name": "...", "api_url": "..."}``. Phase 28
introduces ``~/.verlet/credentials.json`` with kind-discriminated multi-profile
entries. To avoid breaking existing users we run an idempotent, lossless
migration on every CLI invocation:

    if token.json exists AND credentials.json does NOT exist:
        copy token.json contents into the "default" profile under
        kind=showcase_access_code, write credentials.json (mode 0600 on POSIX),
        leave token.json in place (so a downgrade still works).

If both files exist, credentials.json wins and the legacy file is left as a
historical artifact. If neither exists, nothing happens.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from .credentials import (
    LEGACY_TOKEN_FILENAME,
    credentials_path,
    load_credentials,
    save_credentials,
    verlet_dir,
)


def _legacy_path() -> Path:
    return verlet_dir() / LEGACY_TOKEN_FILENAME


def migrate_legacy_token_json() -> bool:
    """Migrate ``~/.verlet/token.json`` into the new credentials.json schema.

    Returns ``True`` only if the migration ran on this call. Idempotent:
    a second call after a successful migration returns ``False`` because
    credentials.json now exists.
    """
    legacy = _legacy_path()
    new = credentials_path()
    if not legacy.exists():
        return False
    if new.exists():
        return False  # already migrated or new install

    try:
        with open(legacy) as fh:
            legacy_doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        sys.stderr.write(
            f"warning: could not read legacy ~/.verlet/token.json "
            f"({exc}); skipping migration.\n"
        )
        return False

    access_token = legacy_doc.get("token")
    if not access_token:
        return False

    doc = load_credentials()
    doc["default_profile"] = "default"
    doc["profiles"]["default"] = {
        "kind": "showcase_access_code",
        "access_token": access_token,
        "api_url": legacy_doc.get("api_url") or "https://api.verlet.co",
        "customer_name": legacy_doc.get("customer_name"),
        "migrated_at": datetime.now(timezone.utc).isoformat(),
    }
    save_credentials(doc)
    sys.stderr.write(
        "Migrated legacy credentials to ~/.verlet/credentials.json "
        "(default profile, kind=showcase_access_code).\n"
    )
    return True
