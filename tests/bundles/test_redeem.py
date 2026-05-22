"""Plan 30-07 Task 2 — `verlet bundles redeem <code>` (CLIBUNDLE-02).

Seven behavior tests:

  * test_redeem_writes_bundle_grant_profile — POSTs the redemption code,
    saves the returned token to credentials.json with kind='bundle_grant'
    + bundle_slug + expires_at, prints success line.
  * test_redeem_idempotent_overwrites_existing — re-redeeming the same code
    overwrites the prior profile entry (server returns 200 again per
    D-BUNDLE2; CLI overwrites local state).
  * test_redeem_410_prints_detail_to_stderr_exit1 — server 410 Gone → CLI
    surfaces the server detail on stderr + exits 1.
  * test_redeem_404_prints_invalid_code_exit1 — server 404 → "Invalid code"
    surfaced on stderr + exit 1.
  * test_auth_status_recognizes_bundle_grant — after redeem, `verlet auth
    status` shows the new profile with kind=bundle_grant + bundle_slug.
  * test_redeem_targets_named_profile — `--profile staging` writes to the
    named profile, not the active default.
  * test_credentials_file_mode_remains_0600 — POSIX file mode after redeem
    is exactly 0o600 (Phase 28 invariant).
"""
from __future__ import annotations

import os

import httpx
import pytest

from verlet.auth import credentials as creds
from verlet.cli import cli

from tests.conftest import combined_output


REDEEM_PATH = "/api/platform/v1/bundles/redeem"

# Far-future and near-future expiries for two distinct test surfaces.
FAR_FUTURE = "2099-01-01T00:00:00+00:00"
NEAR_FUTURE = "2099-01-02T00:00:00+00:00"


def test_redeem_writes_bundle_grant_profile(tmp_home, cli_runner, respx_mock):
    """Successful redeem persists kind=bundle_grant with bundle_slug + expires_at."""
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "bundle.jwt.token",
                "expires_at": FAR_FUTURE,
                "bundle_slug": "stanford-egocentric-2024",
                "kind": "bundle_grant",
            },
        )
    )

    result = cli_runner.invoke(cli, ["bundles", "redeem", "ABCD-1234"])
    assert result.exit_code == 0, (result.output, result.stderr)

    profile = creds.get_profile("default")
    assert profile is not None, "redeem must write the default profile"
    assert profile["kind"] == "bundle_grant"
    assert profile["access_token"] == "bundle.jwt.token"
    assert profile["expires_at"] == FAR_FUTURE
    assert profile["bundle_slug"] == "stanford-egocentric-2024"

    assert "stanford-egocentric-2024" in result.output
    assert FAR_FUTURE in result.output


def test_redeem_idempotent_overwrites_existing(tmp_home, cli_runner, respx_mock):
    """Re-redeeming overwrites the local profile entry idempotently (D-BUNDLE2)."""
    # First redeem.
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "first.token",
                "expires_at": FAR_FUTURE,
                "bundle_slug": "stanford-egocentric-2024",
                "kind": "bundle_grant",
            },
        )
    )
    r1 = cli_runner.invoke(cli, ["bundles", "redeem", "ABCD-1234"])
    assert r1.exit_code == 0, (r1.output, r1.stderr)
    assert creds.get_profile("default")["access_token"] == "first.token"

    # Server reissues a fresh token for the same code (D-BUNDLE2 idempotent).
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "second.token",
                "expires_at": NEAR_FUTURE,
                "bundle_slug": "stanford-egocentric-2024",
                "kind": "bundle_grant",
            },
        )
    )
    r2 = cli_runner.invoke(cli, ["bundles", "redeem", "ABCD-1234"])
    assert r2.exit_code == 0, (r2.output, r2.stderr)

    profile = creds.get_profile("default")
    assert profile["access_token"] == "second.token", "second redeem must overwrite"
    assert profile["expires_at"] == NEAR_FUTURE
    # Still bundle_grant + same slug.
    assert profile["kind"] == "bundle_grant"
    assert profile["bundle_slug"] == "stanford-egocentric-2024"


def test_redeem_410_prints_detail_to_stderr_exit1(tmp_home, cli_runner, respx_mock):
    """Server 410 Gone → CLI surfaces the detail on stderr + exits 1."""
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(
            410, json={"detail": "This code has expired"}
        )
    )

    result = cli_runner.invoke(cli, ["bundles", "redeem", "ABCD-1234"])
    assert result.exit_code != 0, (result.output, result.stderr)
    assert "This code has expired" in combined_output(result)
    # No profile written on failure.
    assert creds.get_profile("default") is None


def test_redeem_404_prints_invalid_code_exit1(tmp_home, cli_runner, respx_mock):
    """Server 404 → "Invalid code" on stderr + exit 1."""
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(404, json={"detail": "Invalid code"})
    )

    result = cli_runner.invoke(cli, ["bundles", "redeem", "NOPE"])
    assert result.exit_code != 0
    assert "Invalid code" in combined_output(result)
    assert creds.get_profile("default") is None


def test_auth_status_recognizes_bundle_grant(tmp_home, cli_runner, respx_mock):
    """After redeem, `verlet auth status` shows the new profile correctly."""
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "bundle.jwt.token",
                "expires_at": FAR_FUTURE,
                "bundle_slug": "stanford-egocentric-2024",
                "kind": "bundle_grant",
            },
        )
    )
    r1 = cli_runner.invoke(cli, ["bundles", "redeem", "ABCD-1234"])
    assert r1.exit_code == 0, (r1.output, r1.stderr)

    r2 = cli_runner.invoke(cli, ["auth", "status"])
    assert r2.exit_code == 0, (r2.output, r2.stderr)
    # Status output must mention the bundle slug + the kind discriminator.
    assert "stanford-egocentric-2024" in r2.output
    assert "bundle_grant" in r2.output


def test_redeem_targets_named_profile(tmp_home, cli_runner, respx_mock):
    """`--profile staging` writes to the named profile, not the default."""
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "staging.bundle.token",
                "expires_at": FAR_FUTURE,
                "bundle_slug": "stanford-egocentric-2024",
                "kind": "bundle_grant",
            },
        )
    )

    result = cli_runner.invoke(
        cli, ["--profile", "staging", "bundles", "redeem", "ABCD-1234"]
    )
    assert result.exit_code == 0, (result.output, result.stderr)

    # default unchanged
    assert creds.get_profile("default") is None
    # staging populated
    staging = creds.get_profile("staging")
    assert staging is not None
    assert staging["kind"] == "bundle_grant"
    assert staging["access_token"] == "staging.bundle.token"


@pytest.mark.skipif(os.name == "nt", reason="POSIX-only chmod")
def test_credentials_file_mode_remains_0600(tmp_home, cli_runner, respx_mock):
    """credentials.json mode is exactly 0o600 after redeem (Phase 28 invariant)."""
    respx_mock.post(f"https://api.verlet.co{REDEEM_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "bundle.jwt.token",
                "expires_at": FAR_FUTURE,
                "bundle_slug": "stanford-egocentric-2024",
                "kind": "bundle_grant",
            },
        )
    )
    result = cli_runner.invoke(cli, ["bundles", "redeem", "ABCD-1234"])
    assert result.exit_code == 0, (result.output, result.stderr)

    path = creds.credentials_path()
    mode = os.stat(path).st_mode & 0o777
    assert mode == 0o600, f"Expected 0600 after redeem, got 0{mode:o}"
