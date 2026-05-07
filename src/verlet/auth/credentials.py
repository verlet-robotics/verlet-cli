"""~/.verlet/credentials.json — kind-discriminated multi-profile credential store.

Schema (per Phase 28 Research §3-§5):

    {
      "version": 1,
      "default_profile": "default",
      "profiles": {
        "<name>": {
          "kind": "device_flow" | "pat" | "showcase_access_code",
          "access_token": "<bearer-string>",
          "api_url": "https://api.verlet.co",
          ... kind-specific fields ...
        },
        ...
      }
    }

Three coexisting kinds in one file. ``access_token`` is the universal field
each kind populates; the api_client always sends ``Authorization: Bearer <access_token>``.

Permissions: on POSIX, the file is chmod 0600 after every write and the parent
directory is chmod 0700 on first creation. On Windows, permissions defer to
the inherited NTFS ACL on ``%USERPROFILE%`` (matches gh / aws CLI behavior).
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Literal, TypedDict

ProfileKind = Literal["device_flow", "pat", "showcase_access_code"]
CREDENTIALS_FILENAME = "credentials.json"
LEGACY_TOKEN_FILENAME = "token.json"
SCHEMA_VERSION = 1


class CredentialsDoc(TypedDict):
    version: int
    default_profile: str
    profiles: dict[str, dict[str, Any]]


def verlet_dir() -> Path:
    """Return ``~/.verlet/``, creating it (mode 0700 on POSIX) if missing."""
    d = Path.home() / ".verlet"
    if not d.exists():
        d.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            try:
                os.chmod(d, 0o700)
            except OSError:
                # Best-effort: don't refuse to operate if chmod fails
                # (e.g., directory on a non-POSIX-perm filesystem).
                pass
    return d


def credentials_path() -> Path:
    return verlet_dir() / CREDENTIALS_FILENAME


def _empty_doc() -> CredentialsDoc:
    return {"version": SCHEMA_VERSION, "default_profile": "default", "profiles": {}}


def load_credentials() -> CredentialsDoc:
    """Read and return the credentials document.

    Returns an empty document (with no file written) when the file does not
    exist. Emits a stderr warning on overly permissive POSIX file modes
    (does not refuse to operate).
    """
    path = credentials_path()
    if not path.exists():
        return _empty_doc()
    warn_on_bad_permissions(path)
    with open(path) as fh:
        doc = json.load(fh)
    if doc.get("version") != SCHEMA_VERSION:
        # Forward-compat: tolerate read but warn loudly.
        sys.stderr.write(
            f"warning: ~/.verlet/credentials.json schema version "
            f"{doc.get('version')} is newer than this CLI supports "
            f"({SCHEMA_VERSION}). Read-only mode.\n"
        )
    doc.setdefault("default_profile", "default")
    doc.setdefault("profiles", {})
    return doc


def save_credentials(doc: CredentialsDoc) -> None:
    """Atomically write the credentials document and chmod 0600 (POSIX).

    Uses tempfile + fsync + ``os.replace`` for crash-safety: a process kill
    mid-write either leaves the previous file intact or replaces it atomically
    with the new content. Never an in-place partial write.
    """
    path = credentials_path()
    parent = path.parent
    fd, tmp_name = tempfile.mkstemp(
        dir=str(parent), prefix=".credentials.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
        if os.name != "nt":
            os.chmod(path, 0o600)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def get_profile(profile_name: str) -> dict[str, Any] | None:
    """Return the named profile entry, or ``None`` if it doesn't exist."""
    return load_credentials()["profiles"].get(profile_name)


def upsert_profile(profile_name: str, *, kind: ProfileKind, **fields: Any) -> None:
    """Create or replace the named profile with ``{"kind": kind, **fields}``.

    The first parameter is the PROFILE name (top-level key under ``profiles``).
    Note: ``fields`` may include a ``name`` key for ``kind=pat`` profiles —
    that's the PAT's own user-supplied name and is stored verbatim. We use
    ``profile_name`` as the positional name here to avoid the collision.
    """
    doc = load_credentials()
    entry: dict[str, Any] = {"kind": kind, **fields}
    doc["profiles"][profile_name] = entry
    save_credentials(doc)


def delete_profile(profile_name: str) -> bool:
    """Remove the named profile. Return True if it existed, False otherwise."""
    doc = load_credentials()
    if profile_name in doc["profiles"]:
        del doc["profiles"][profile_name]
        save_credentials(doc)
        return True
    return False


def set_default_profile(profile_name: str) -> None:
    """Set the file's ``default_profile`` field. Caller must ensure the profile exists."""
    doc = load_credentials()
    doc["default_profile"] = profile_name
    save_credentials(doc)


def warn_on_bad_permissions(path: Path) -> None:
    """Emit a stderr warning if POSIX file mode lets group or other read the file.

    No-op on Windows (Research §5 — matches gh / aws CLI behavior; rely on
    the inherited %USERPROFILE% NTFS ACL).
    """
    if os.name == "nt":
        return
    try:
        mode = os.stat(path).st_mode & 0o777
    except FileNotFoundError:
        return
    if mode & 0o077:
        sys.stderr.write(
            f"warning: ~/.verlet/credentials.json has overly permissive "
            f"mode 0{mode:o} (expected 0600).\n"
            f"         Other users on this machine may be able to read "
            f"your tokens.\n"
            f"         Run: chmod 600 {path}\n"
        )
