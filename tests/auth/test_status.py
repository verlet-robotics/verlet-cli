"""CLIAUTH-09 — ``verlet auth status``.

Plan 28-04 (Wave 4) replaces the Wave 0 xfail stubs with real assertions on
the per-kind output rendering (device_flow / pat / showcase_access_code) and
the machine-readable ``--json`` output.
"""

import json
from datetime import datetime, timedelta, timezone

import httpx

from verlet.auth import credentials as creds
from verlet.cli import cli

from tests.conftest import combined_output


def _far_future_iso() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()


def test_device_flow_status(tmp_home, respx_mock, cli_runner):
    creds.upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value-very-long-string-XYZ9",
        refresh_token="rt.value",
        expires_at=_far_future_iso(),
        identity={
            "id": "u1",
            "account_id": "0e64a8ab-1234",
            "email": "jane@example.com",
            "display_name": "Jane Doe",
            "slug": "jane",
        },
        active_namespace={
            "account_id": "0e64a8ab-1234",
            "type": "user",
            "slug": "jane",
            "display_name": "Jane Doe",
            "role": "owner",
        },
        issued_at="2026-05-07T12:00:00+00:00",
    )
    result = cli_runner.invoke(cli, ["auth", "status"])
    assert result.exit_code == 0, result.output
    assert "Profile: default" in result.output
    assert "kind=device_flow" in result.output
    assert "Jane Doe" in result.output
    assert "jane@example.com" in result.output
    # Token is masked, full plaintext NOT printed
    assert "jwt.access.value-very-long-string-XYZ9" not in result.output
    # last 4 of access_token IS shown
    assert "XYZ9" in result.output


def test_pat_status(tmp_home, respx_mock, cli_runner):
    creds.upsert_profile(
        "ci",
        kind="pat",
        api_url="https://api.verlet.co",
        access_token="pat_a1b2c3d4e5f60718_secret_xyz_Xkj9",
        pat_id="f5b1a83c-1234-4abc-9def-aaaaaaaaaaaa",
        name="ci-token",
        scopes=["read:datasets", "write:push"],
        last_4="Xkj9",
        created_at="2026-05-07T12:00:00+00:00",
        expires_at=None,
        identity={
            "id": "u1",
            "account_id": "0e64a8ab",
            "email": "jane@example.com",
            "display_name": "Jane Doe",
            "slug": "jane",
        },
        active_namespace={
            "account_id": "0e64a8ab",
            "type": "user",
            "slug": "jane",
            "display_name": "Jane Doe",
            "role": "owner",
        },
    )
    result = cli_runner.invoke(cli, ["--profile", "ci", "auth", "status"])
    assert result.exit_code == 0, result.output
    assert "kind=pat" in result.output
    assert "ci-token" in result.output
    assert "read:datasets" in result.output
    assert "write:push" in result.output
    assert "Xkj9" in result.output
    assert "Expires:     never" in result.output


def test_showcase_status(tmp_home, cli_runner):
    # Showcase status MUST NOT call /auth/me — no respx_mock fixture used.
    creds.upsert_profile(
        "showcase-old",
        kind="showcase_access_code",
        api_url="https://api.verlet.co",
        access_token="showcase-jwt-XYZ4",
        customer_name="Acme Robotics",
        expires_at=_far_future_iso(),
        issued_at="2026-05-07T12:00:00+00:00",
    )
    result = cli_runner.invoke(cli, ["--profile", "showcase-old", "auth", "status"])
    assert result.exit_code == 0, result.output
    assert "kind=showcase_access_code" in result.output
    assert "Acme Robotics" in result.output
    # Full token NOT printed; masked.
    assert "showcase-jwt-XYZ4" not in result.output


def test_fresh_showcase_token_not_flagged_expiring(tmp_home, cli_runner):
    """A freshly-issued showcase token (24h TTL) must NOT read as expiring soon.

    Regression: the near-expiry warning used a fixed 24h threshold, so a
    showcase code — which lives exactly 24h — was flagged the instant it
    was activated. The threshold now scales to the token's own lifetime.
    """
    now = datetime.now(timezone.utc)
    creds.upsert_profile(
        "showcase-fresh",
        kind="showcase_access_code",
        api_url="https://api.verlet.co",
        access_token="showcase-jwt-fresh-XYZ4",
        customer_name="Acme Robotics",
        issued_at=now.isoformat(),
        expires_at=(now + timedelta(hours=24)).isoformat(),
    )
    result = cli_runner.invoke(cli, ["--profile", "showcase-fresh", "auth", "status"])
    assert result.exit_code == 0, result.output
    assert "Expiring soon" not in result.output


def test_near_expiry_showcase_token_flagged(tmp_home, cli_runner):
    """A showcase token inside its final 10% IS flagged, with the showcase hint."""
    now = datetime.now(timezone.utc)
    creds.upsert_profile(
        "showcase-stale",
        kind="showcase_access_code",
        api_url="https://api.verlet.co",
        access_token="showcase-jwt-stale-XYZ4",
        customer_name="Acme Robotics",
        issued_at=(now - timedelta(hours=23)).isoformat(),
        expires_at=(now + timedelta(hours=1)).isoformat(),
    )
    result = cli_runner.invoke(cli, ["--profile", "showcase-stale", "auth", "status"])
    assert result.exit_code == 0, result.output
    assert "Expiring soon" in result.output
    # Kind-aware hint: showcase re-auths with --kind showcase, not plain login.
    assert "verlet auth login --kind showcase" in result.output


def test_json_output(tmp_home, cli_runner):
    creds.upsert_profile(
        "default",
        kind="pat",
        api_url="https://api.verlet.co",
        access_token="pat_a_b",
        pat_id="abcd",
        name="ci-token",
        scopes=["read:datasets"],
        last_4="b",
        created_at="2026-05-07T12:00:00+00:00",
        expires_at=None,
    )
    result = cli_runner.invoke(cli, ["auth", "status", "--json"])
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["profile"] == "default"
    assert parsed["kind"] == "pat"
    assert parsed["api_url"] == "https://api.verlet.co"
    assert parsed["expired"] is False
    assert parsed["scopes"] == ["read:datasets"]


def test_legacy_login_shim_deprecation_warning(tmp_home, respx_mock, cli_runner):
    """Plan 28-04 task 28-04-02 — legacy `verlet login` is now a deprecation shim
    that calls into showcase_login(). The deprecation hint goes to stderr; the
    new credentials.json profile is written under kind=showcase_access_code.
    """
    respx_mock.post("/api/v1/showcase/auth").mock(
        return_value=httpx.Response(
            200, json={"token": "showcase-jwt-XYZ", "customer_name": "Acme"}
        )
    )
    result = cli_runner.invoke(cli, ["login"], input="my-access-code\n")
    # Deprecation hint goes to stderr.
    combined = combined_output(result)
    assert "DEPRECATED" in combined
    assert "verlet auth login --kind showcase" in combined
    # Profile written.
    profile = creds.get_profile("default")
    assert profile is not None
    assert profile["kind"] == "showcase_access_code"
