"""CLIAUTH-07 — ``verlet auth tokens create|list|revoke|show``.

Plan 28-03 (Wave 3) replaces the xfail bodies with real assertions:

  * test_create_persists_and_warns — POST /tokens mints a PAT; stdout shows
    plaintext + ``SAVE THIS NOW``; the active profile is upserted under
    ``kind=pat`` with the plaintext as ``access_token``.
  * test_list_no_plaintext — GET /tokens renders a Rich table; ``pat_`` does
    not appear anywhere in stdout (Research §1.1 invariant).
  * test_revoke_clears_local — DELETE /tokens/{id} succeeds; the local profile
    that held the matching ``pat_id`` is cleared.
  * test_invalid_scope_rejected — unknown ``--scope`` short-circuits client-side
    BEFORE any HTTP call (no respx_mock fixture used — proves it).
"""

import httpx

from verlet.auth import credentials as creds
from verlet.cli import cli

from tests.conftest import combined_output


def _seed_device_flow_default(tmp_home):
    """Seed a logged-in device_flow profile so AuthenticatedClient works."""
    creds.upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        # Far-future expiry so AuthenticatedClient skips the refresh path.
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
        active_namespace=None,
    )


def test_create_persists_and_warns(tmp_home, respx_mock, cli_runner):
    """Mint a PAT, assert plaintext-once warning, assert profile persistence."""
    _seed_device_flow_default(tmp_home)
    respx_mock.post("/api/platform/v1/auth/tokens").mock(
        return_value=httpx.Response(
            201,
            json={
                "id": "f5b1a83c-1234-4abc-9def-aaaaaaaaaaaa",
                "name": "ci-token",
                "scopes": ["read:datasets", "write:push"],
                "last_4": "Xkj9",
                "plaintext": "pat_a1b2c3d4e5f60718_secret_with_underscores_xyz",
                "created_at": "2026-05-07T12:34:56Z",
                "expires_at": None,
            },
        )
    )

    result = cli_runner.invoke(
        cli,
        [
            "auth",
            "tokens",
            "create",
            "--name",
            "ci-token",
            "--scope",
            "read:datasets",
            "--scope",
            "write:push",
        ],
    )
    assert result.exit_code == 0, result.output
    # Plaintext printed once
    assert "pat_a1b2c3d4e5f60718_secret_with_underscores_xyz" in result.output
    # SAVE THIS NOW warning
    assert "SAVE THIS NOW" in result.output
    # Persisted to active profile (default) under kind=pat
    profile = creds.get_profile("default")
    assert profile is not None
    assert profile["kind"] == "pat"
    assert (
        profile["access_token"]
        == "pat_a1b2c3d4e5f60718_secret_with_underscores_xyz"
    )
    assert profile["pat_id"] == "f5b1a83c-1234-4abc-9def-aaaaaaaaaaaa"
    assert profile["name"] == "ci-token"
    assert profile["last_4"] == "Xkj9"


def test_list_no_plaintext(tmp_home, respx_mock, cli_runner):
    """List endpoint must never echo plaintext (Research §1.1 invariant)."""
    _seed_device_flow_default(tmp_home)
    respx_mock.get("/api/platform/v1/auth/tokens").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": "f5b1a83c-1234-4abc-9def-aaaaaaaaaaaa",
                    "name": "ci-token",
                    "scopes": ["read:datasets", "write:push"],
                    "last_4": "Xkj9",
                    "expires_at": None,
                    "last_used_at": None,
                    "created_at": "2026-05-07T12:34:56Z",
                    "revoked_at": None,
                }
            ],
        )
    )
    result = cli_runner.invoke(cli, ["auth", "tokens", "list"])
    assert result.exit_code == 0, result.output
    assert "ci-token" in result.output
    assert "Xkj9" in result.output
    # Plaintext format is pat_<lookup>_<secret>; assert NEITHER prefix nor
    # any plaintext token appears in list output (Research §1.1 invariant).
    assert "pat_" not in result.output


def test_revoke_clears_local(tmp_home, respx_mock, cli_runner):
    """Revoking the PAT that backs the active profile clears the profile."""
    # Seed a profile that holds a PAT with the id we will revoke.
    creds.upsert_profile(
        "default",
        kind="pat",
        api_url="https://api.verlet.co",
        access_token="pat_a_b",
        pat_id="f5b1a83c-1234-4abc-9def-aaaaaaaaaaaa",
        name="ci-token",
        scopes=["read:datasets"],
        last_4="Xkj9",
        created_at="2026-05-07T12:34:56Z",
        expires_at=None,
    )
    respx_mock.get("/api/platform/v1/auth/tokens").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "id": "f5b1a83c-1234-4abc-9def-aaaaaaaaaaaa",
                    "name": "ci-token",
                    "scopes": ["read:datasets"],
                    "last_4": "Xkj9",
                    "expires_at": None,
                    "last_used_at": None,
                    "created_at": "2026-05-07T12:34:56Z",
                    "revoked_at": None,
                }
            ],
        )
    )
    respx_mock.delete(
        "/api/platform/v1/auth/tokens/f5b1a83c-1234-4abc-9def-aaaaaaaaaaaa"
    ).mock(return_value=httpx.Response(204))

    result = cli_runner.invoke(cli, ["auth", "tokens", "revoke", "ci-token"])
    assert result.exit_code == 0, result.output
    # Local profile cleared because pat_id matched.
    assert creds.get_profile("default") is None


def test_invalid_scope_rejected(tmp_home, cli_runner):
    """Unknown --scope short-circuits client-side; no HTTP call is made."""
    # No need to seed — validation happens before any HTTP / profile load.
    result = cli_runner.invoke(
        cli,
        [
            "auth",
            "tokens",
            "create",
            "--name",
            "x",
            "--scope",
            "read:datasets",
            "--scope",
            "foo",
        ],
    )
    assert result.exit_code != 0
    # UsageError prints to stderr; combined_output concatenates stdout+stderr
    # robustly across Click versions (8.1.x mixes, 8.2+ separates).
    combined = combined_output(result)
    assert "Invalid scope 'foo'" in combined
    assert "read:catalog" in combined  # all 7 listed
    assert "write:tokens" in combined
