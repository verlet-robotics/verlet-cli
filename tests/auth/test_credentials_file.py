"""CLIAUTH-08 — credentials.json file-permission contract."""

import os

import pytest

from verlet.auth import credentials as creds


@pytest.mark.skipif(os.name == "nt", reason="POSIX-only chmod")
def test_mode_0600(tmp_home):
    """After writing credentials.json, the file mode must be exactly 0600."""
    creds.upsert_profile(
        "ci",
        kind="pat",
        access_token="pat_a_b",
        api_url="https://client-api.verlet.co",
    )
    path = creds.credentials_path()
    mode = os.stat(path).st_mode & 0o777
    assert mode == 0o600, f"Expected 0600, got 0{mode:o}"


@pytest.mark.skipif(os.name == "nt", reason="POSIX-only permissions")
def test_warns_on_overpermissive(tmp_home, capsys):
    """Reading a credentials.json with mode 0644 must warn but not refuse."""
    creds.upsert_profile(
        "ci",
        kind="pat",
        access_token="pat_a_b",
        api_url="https://client-api.verlet.co",
    )
    path = creds.credentials_path()
    os.chmod(path, 0o644)
    creds.load_credentials()  # triggers the warning
    captured = capsys.readouterr()
    assert "0600" in captured.err
    assert "chmod 600" in captured.err
    assert ".verlet/credentials.json" in captured.err


def test_legacy_api_url_moves_to_client_api(tmp_home):
    """Profiles saved against api.verlet.co resolve to the client API; others are untouched."""
    creds.upsert_profile("old", kind="pat", access_token="t", api_url="https://api.verlet.co/")
    creds.upsert_profile("stg", kind="pat", access_token="t", api_url="https://staging-api.verlet.co")
    assert creds.get_profile("old")["api_url"] == "https://client-api.verlet.co"
    assert creds.get_profile("stg")["api_url"] == "https://staging-api.verlet.co"
