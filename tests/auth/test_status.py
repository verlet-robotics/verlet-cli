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


def test_legacy_login_command_removed(tmp_home, cli_runner):
    """`verlet login` (legacy showcase shim) was removed in 0.9.0 — use
    `verlet auth login --kind showcase`. The bare name now resolves to
    Click's unknown-command error. Guards against accidental re-registration.
    """
    result = cli_runner.invoke(cli, ["login"])
    assert result.exit_code == 2
    assert "No such command" in result.output


# ---------------------------------------------------------------------------
# Near-expiry warning — Bug spotted hands-on: a flat 24h threshold fired
# on the first ``auth status`` after a fresh showcase login (their JWT TTL
# is 24h, so the user IS within 24h from minute one). Fixed by making the
# threshold kind-aware AND by surfacing the refresh command through the
# shared ``auth.expiry.refresh_command`` helper so the soon-hint matches
# the EXPIRED-hint's per-kind command text.
# ---------------------------------------------------------------------------


def _expires_in(seconds: int) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(seconds=seconds)
    ).isoformat()


def test_fresh_showcase_token_does_not_show_soon_warning(tmp_home, cli_runner):
    """A brand-new showcase JWT (24h TTL) must NOT trigger the soon-hint.
    The threshold for showcase is 2h, well below the 24h fresh-token mark.
    """
    creds.upsert_profile(
        "default",
        kind="showcase_access_code",
        api_url="https://api.verlet.co",
        access_token="sc.jwt.value",
        expires_at=_expires_in(23 * 3600 + 59 * 60),  # 23h 59m
        customer_name="Demo",
    )
    result = cli_runner.invoke(cli, ["auth", "status"])
    assert result.exit_code == 0, result.output
    assert "Expiring soon" not in result.output


def test_showcase_token_within_2h_shows_soon_warning_with_kind_flag(
    tmp_home, cli_runner
):
    """Inside the showcase per-kind window (under 2h), the soon-hint must
    fire AND must say ``--kind showcase`` so a showcase user following the
    advice doesn't get dumped into the device-flow login by mistake.
    """
    creds.upsert_profile(
        "default",
        kind="showcase_access_code",
        api_url="https://api.verlet.co",
        access_token="sc.jwt.value",
        expires_at=_expires_in(45 * 60),  # 45 minutes left
        customer_name="Demo",
    )
    result = cli_runner.invoke(cli, ["auth", "status"])
    assert result.exit_code == 0, result.output
    assert "Expiring soon" in result.output
    assert "verlet auth login --kind showcase" in result.output


def test_device_flow_within_1h_shows_soon_warning(tmp_home, cli_runner):
    """device_flow JWTs live 8h; warn under 1h. A fresh 8h token must NOT
    warn, but one with 30 minutes left must."""
    creds.upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="df.jwt.value",
        refresh_token="rt",
        expires_at=_expires_in(30 * 60),  # 30 minutes left
    )
    result = cli_runner.invoke(cli, ["auth", "status"])
    assert "Expiring soon" in result.output
    assert "verlet auth login" in result.output
    # device_flow refresh command must NOT carry --kind showcase.
    assert "--kind showcase" not in result.output


def test_device_flow_within_8h_no_warning_because_just_logged_in(
    tmp_home, cli_runner
):
    """A device_flow token at 4h remaining is fine (mid-lifetime). The
    flat 24h threshold would have wrongly warned here; the 1h per-kind
    threshold leaves the user alone."""
    creds.upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="df.jwt.value",
        refresh_token="rt",
        expires_at=_expires_in(4 * 3600),
    )
    result = cli_runner.invoke(cli, ["auth", "status"])
    assert "Expiring soon" not in result.output


def test_pat_within_24h_shows_soon_warning_with_tokens_create(
    tmp_home, cli_runner
):
    """PATs use the longer 24h warning window (they can live for weeks).
    The hint must point at ``auth tokens create`` — minting a new PAT,
    NOT running interactive login."""
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
        expires_at=_expires_in(12 * 3600),  # 12h left
    )
    result = cli_runner.invoke(cli, ["auth", "status"])
    assert "Expiring soon" in result.output
    assert "verlet auth tokens create" in result.output
