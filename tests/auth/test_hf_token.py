"""Plan 30-05 Task 1 — `verlet auth tokens set hf <token>` + hf_token storage.

Adds an optional ``hf_token`` field to the credentials.json profile schema
(D-FORMAT2). The field is exclusive of ``access_token`` — it only carries the
HuggingFace token used by ``verlet datasets push --to huggingface://...``.

Six behavior tests:

  * test_set_hf_token_writes_active_profile — `verlet auth tokens set hf hf_xxx`
    persists to the active (default) profile.
  * test_set_hf_token_targets_named_profile — `--profile staging` routes to the
    named profile.
  * test_set_hf_token_overwrites_existing — re-running the setter overwrites
    a previous ``hf_token`` value (idempotent).
  * test_read_hf_token_returns_value — ``read_hf_token(profile_name)``
    returns the stored value.
  * test_read_hf_token_none_when_unset — returns ``None`` when the profile
    has no ``hf_token`` field.
  * test_set_hf_token_preserves_0600_mode — credentials.json mode after the
    setter remains exactly ``0o600`` (preserves Phase 28 invariant).
"""
from __future__ import annotations

import os

import pytest

from verlet.auth import credentials as creds
from verlet.cli import cli


def _seed_default_profile() -> None:
    """Seed a logged-in device_flow profile so the setter can target it."""
    creds.upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _seed_named_profile(name: str) -> None:
    creds.upsert_profile(
        name,
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def test_set_hf_token_writes_active_profile(tmp_home, cli_runner):
    """`verlet auth tokens set hf hf_xxx` writes hf_token to the default profile."""
    _seed_default_profile()
    result = cli_runner.invoke(
        cli, ["auth", "tokens", "set", "hf", "hf_xxx_active"]
    )
    assert result.exit_code == 0, result.output
    profile = creds.get_profile("default")
    assert profile is not None
    assert profile.get("hf_token") == "hf_xxx_active"
    # Existing fields preserved.
    assert profile["kind"] == "device_flow"
    assert profile["access_token"] == "jwt.access.value"


def test_set_hf_token_targets_named_profile(tmp_home, cli_runner):
    """`--profile staging` routes the write to the named profile."""
    _seed_default_profile()
    _seed_named_profile("staging")
    result = cli_runner.invoke(
        cli,
        ["--profile", "staging", "auth", "tokens", "set", "hf", "hf_staging_xyz"],
    )
    assert result.exit_code == 0, result.output
    # Default unchanged.
    default_profile = creds.get_profile("default")
    assert default_profile is not None
    assert default_profile.get("hf_token") is None
    # Staging carries the new token.
    staging_profile = creds.get_profile("staging")
    assert staging_profile is not None
    assert staging_profile.get("hf_token") == "hf_staging_xyz"


def test_set_hf_token_overwrites_existing(tmp_home, cli_runner):
    """Re-running the setter overwrites the previous hf_token value."""
    _seed_default_profile()
    # First write.
    r1 = cli_runner.invoke(cli, ["auth", "tokens", "set", "hf", "hf_first"])
    assert r1.exit_code == 0, r1.output
    # Second write — same profile, new value.
    r2 = cli_runner.invoke(cli, ["auth", "tokens", "set", "hf", "hf_second"])
    assert r2.exit_code == 0, r2.output
    profile = creds.get_profile("default")
    assert profile is not None
    assert profile.get("hf_token") == "hf_second"


def test_read_hf_token_returns_value(tmp_home):
    """``read_hf_token(profile_name)`` returns the stored value."""
    _seed_default_profile()
    creds.set_hf_token("default", "hf_read_xyz")
    assert creds.read_hf_token("default") == "hf_read_xyz"


def test_read_hf_token_none_when_unset(tmp_home):
    """``read_hf_token`` returns ``None`` when the profile has no hf_token field."""
    _seed_default_profile()
    # No setter call → no hf_token field.
    assert creds.read_hf_token("default") is None
    # Same when the profile doesn't exist at all.
    assert creds.read_hf_token("nonexistent") is None


@pytest.mark.skipif(os.name == "nt", reason="POSIX-only chmod")
def test_set_hf_token_preserves_0600_mode(tmp_home, cli_runner):
    """credentials.json mode is exactly 0o600 after the setter (Phase 28 invariant)."""
    _seed_default_profile()
    result = cli_runner.invoke(
        cli, ["auth", "tokens", "set", "hf", "hf_perms_check"]
    )
    assert result.exit_code == 0, result.output
    path = creds.credentials_path()
    mode = os.stat(path).st_mode & 0o777
    assert mode == 0o600, f"Expected 0600 after hf_token setter, got 0{mode:o}"
