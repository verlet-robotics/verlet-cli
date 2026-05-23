"""Tests for `verlet destinations rm`."""
from __future__ import annotations

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

BASE = "https://api.verlet.co/api/platform/v1/downloads/destinations"
DEST_ID = "00000000-0000-0000-0000-0000000000d1"
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


def _dest(name: str = "my-s3") -> dict:
    return {
        "id": DEST_ID,
        "account_id": "acc-1",
        "name": name,
        "provider": "aws_s3",
        "auth_kind": "deeplink",
        "bucket": "b",
        "prefix": None,
        "region": None,
        "created_at": "2026-05-01T00:00:00+00:00",
        "updated_at": "2026-05-01T00:00:00+00:00",
    }


def test_rm_by_name_resolves_then_deletes(tmp_home, cli_runner, respx_mock):
    """`rm <name> --yes` resolves the name via GET then DELETEs by id."""
    _seed()
    respx_mock.get(BASE).mock(return_value=httpx.Response(200, json=[_dest()]))
    delete = respx_mock.delete(f"{BASE}/{DEST_ID}").mock(
        return_value=httpx.Response(204)
    )

    result = cli_runner.invoke(cli, ["destinations", "rm", "my-s3", "--yes"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert delete.called


def test_rm_by_uuid_skips_lookup(tmp_home, cli_runner, respx_mock):
    """A UUID arg DELETEs directly — no GET /destinations lookup."""
    _seed()
    listing = respx_mock.get(BASE).mock(
        return_value=httpx.Response(200, json=[_dest()])
    )
    delete = respx_mock.delete(f"{BASE}/{DEST_ID}").mock(
        return_value=httpx.Response(204)
    )

    result = cli_runner.invoke(cli, ["destinations", "rm", DEST_ID, "--yes"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert delete.called
    assert not listing.called


def test_rm_unknown_name_errors_no_delete(tmp_home, cli_runner, respx_mock):
    """An unknown name errors and never issues a DELETE."""
    _seed()
    respx_mock.get(BASE).mock(return_value=httpx.Response(200, json=[_dest()]))
    delete = respx_mock.delete(f"{BASE}/{DEST_ID}").mock(
        return_value=httpx.Response(204)
    )

    result = cli_runner.invoke(cli, ["destinations", "rm", "ghost", "--yes"])
    assert result.exit_code != 0
    assert "No saved destination named 'ghost'" in (
        result.output + (result.stderr or "")
    )
    assert not delete.called


def test_rm_confirmation_abort(tmp_home, cli_runner, respx_mock):
    """Declining the confirmation prompt aborts without a DELETE."""
    _seed()
    respx_mock.get(BASE).mock(return_value=httpx.Response(200, json=[_dest()]))
    delete = respx_mock.delete(f"{BASE}/{DEST_ID}").mock(
        return_value=httpx.Response(204)
    )

    result = cli_runner.invoke(
        cli, ["destinations", "rm", "my-s3"], input="n\n"
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "Aborted" in result.output
    assert not delete.called
