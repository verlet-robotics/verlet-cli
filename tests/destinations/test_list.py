"""Tests for `verlet destinations list`."""
from __future__ import annotations

import json

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

DEST_PATH = "/api/platform/v1/downloads/destinations"
FAR_FUTURE = "2099-01-01T00:00:00+00:00"


def _seed() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at=FAR_FUTURE,
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _sample_dest(name: str = "my-s3") -> dict:
    return {
        "id": "00000000-0000-0000-0000-0000000000d1",
        "account_id": "acc-1",
        "name": name,
        "provider": "aws_s3",
        "auth_kind": "deeplink",
        "bucket": "my-bucket",
        "prefix": "exports/",
        "region": "us-east-1",
        "created_at": "2026-05-01T00:00:00+00:00",
        "updated_at": "2026-05-01T00:00:00+00:00",
    }


def test_list_renders_table(tmp_home, cli_runner, respx_mock):
    """`destinations list` GETs /destinations with the Bearer header."""
    _seed()
    route = respx_mock.get(f"https://api.verlet.co{DEST_PATH}").mock(
        return_value=httpx.Response(200, json=[_sample_dest()])
    )

    result = cli_runner.invoke(cli, ["destinations", "list"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    assert route.calls.last.request.headers.get("Authorization") == (
        "Bearer jwt.access.value"
    )
    assert "my-s3" in result.output


def test_list_empty_message(tmp_home, cli_runner, respx_mock):
    """An empty destination list shows the dim 'add one' hint."""
    _seed()
    respx_mock.get(f"https://api.verlet.co{DEST_PATH}").mock(
        return_value=httpx.Response(200, json=[])
    )

    result = cli_runner.invoke(cli, ["destinations", "list"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "No cloud destinations" in result.output


def test_list_json_flag(tmp_home, cli_runner, respx_mock):
    """`--json` emits the raw destinations list."""
    _seed()
    respx_mock.get(f"https://api.verlet.co{DEST_PATH}").mock(
        return_value=httpx.Response(200, json=[_sample_dest()])
    )

    result = cli_runner.invoke(cli, ["destinations", "list", "--json"])
    assert result.exit_code == 0, (result.output, result.stderr)
    parsed = json.loads(result.output)
    assert parsed[0]["name"] == "my-s3"


def test_list_unauthenticated(tmp_home, cli_runner, respx_mock):
    """No profile → friendly auth error, no HTTP, no traceback."""
    result = cli_runner.invoke(cli, ["destinations", "list"])
    assert result.exit_code != 0
    assert "Not authenticated" in (result.output + (result.stderr or ""))
    assert "Traceback" not in result.output


def test_list_401_friendly_error(tmp_home, cli_runner, respx_mock):
    """A backend 401 surfaces as a friendly error, not a traceback."""
    _seed()
    respx_mock.get(f"https://api.verlet.co{DEST_PATH}").mock(
        return_value=httpx.Response(401, json={"detail": "Token expired"})
    )

    result = cli_runner.invoke(cli, ["destinations", "list"])
    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert "Token expired" in (result.output + (result.stderr or ""))
