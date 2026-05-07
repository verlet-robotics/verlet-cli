"""Plan 30-08 Task 1 — `verlet bundles list` (CLIBUNDLE-03).

Six behavior tests:

  * test_list_calls_authenticated_bundles_endpoint — `verlet bundles list`
    calls GET /api/platform/v1/bundles with the Bearer header from the active
    profile and renders a Rich table.
  * test_list_all_passes_include_inactive — `--all` adds
    `?include_inactive=true` and renders the status column with mixed values.
  * test_list_json_outputs_valid_json — `--json` emits a parseable list to
    stdout.
  * test_list_empty_message — empty `items` array shows the "No bundles in
    your account." dim message.
  * test_list_401_prints_verbatim_auth_error — 401 → stderr contains the
    exact `"not authenticated; run verlet auth login"` line and exit != 0.
  * test_list_status_color_coding — status column wraps active/expired/
    revoked values in green/yellow/red Text styles.
"""
from __future__ import annotations

import json

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.bundles._render import STATUS_STYLES, bundles_list_table
from verlet.cli import cli


BUNDLES_LIST_PATH = "/api/platform/v1/bundles"

FAR_FUTURE = "2099-01-01T00:00:00+00:00"
PAST = "2020-01-01T00:00:00+00:00"


def _seed_default_profile() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at=FAR_FUTURE,
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _sample_active_item(slug: str = "stanford-egocentric-2024") -> dict:
    return {
        "bundle_id": "00000000-0000-0000-0000-000000000001",
        "bundle_slug": slug,
        "bundle_name": "Stanford Egocentric 2024",
        "kind": "research",
        "expires_at": FAR_FUTURE,
        "dataset_count": 12,
        "total_size_bytes": 5_000_000_000,
        "total_hours": 142.5,
        "license": "CC-BY-4.0",
        "citation": "Lee et al., 2024",
        "status": "active",
    }


def test_list_calls_authenticated_bundles_endpoint(
    tmp_home, cli_runner, respx_mock,
):
    """`verlet bundles list` GETs /api/platform/v1/bundles with Authorization."""
    _seed_default_profile()

    route = respx_mock.get(
        f"https://api.verlet.co{BUNDLES_LIST_PATH}"
    ).mock(
        return_value=httpx.Response(
            200, json={"items": [_sample_active_item()]}
        )
    )

    result = cli_runner.invoke(cli, ["bundles", "list"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called

    # Authenticated: Authorization header sent.
    request = route.calls.last.request
    assert request.headers.get("Authorization") == "Bearer jwt.access.value", (
        f"expected Bearer header; got {request.headers.get('Authorization')!r}"
    )

    # Output contains the slug + status.
    assert "stanford-egocentric-2024" in result.output


def test_list_all_passes_include_inactive(tmp_home, cli_runner, respx_mock):
    """`--all` adds `?include_inactive=true` and renders mixed-status rows."""
    _seed_default_profile()

    route = respx_mock.get(
        f"https://api.verlet.co{BUNDLES_LIST_PATH}"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    _sample_active_item(),
                    {
                        **_sample_active_item("mit-pickplace"),
                        "expires_at": PAST,
                        "status": "expired",
                    },
                    {
                        **_sample_active_item("revoked-grant"),
                        "status": "revoked",
                    },
                ]
            },
        )
    )

    result = cli_runner.invoke(cli, ["bundles", "list", "--all"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called

    qs = dict(route.calls.last.request.url.params)
    assert qs.get("include_inactive") == "true", (
        f"expected include_inactive=true, got params={qs}"
    )

    # All three slugs surface in output.
    assert "stanford-egocentric-2024" in result.output
    assert "mit-pickplace" in result.output
    assert "revoked-grant" in result.output


def test_list_json_outputs_valid_json(tmp_home, cli_runner, respx_mock):
    """`--json` writes a parseable list of items to stdout."""
    _seed_default_profile()

    respx_mock.get(
        f"https://api.verlet.co{BUNDLES_LIST_PATH}"
    ).mock(
        return_value=httpx.Response(
            200, json={"items": [_sample_active_item()]}
        )
    )

    result = cli_runner.invoke(cli, ["bundles", "list", "--json"])
    assert result.exit_code == 0, (result.output, result.stderr)

    parsed = json.loads(result.output)
    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert parsed[0]["bundle_slug"] == "stanford-egocentric-2024"
    assert parsed[0]["dataset_count"] == 12


def test_list_empty_message(tmp_home, cli_runner, respx_mock):
    """Empty `items` array renders the "No bundles in your account." message."""
    _seed_default_profile()

    respx_mock.get(
        f"https://api.verlet.co{BUNDLES_LIST_PATH}"
    ).mock(return_value=httpx.Response(200, json={"items": []}))

    result = cli_runner.invoke(cli, ["bundles", "list"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "No bundles in your account." in result.output


def test_list_401_prints_verbatim_auth_error(tmp_home, cli_runner, respx_mock):
    """401 → stderr carries the exact `"not authenticated; run verlet auth login"`."""
    _seed_default_profile()

    respx_mock.get(
        f"https://api.verlet.co{BUNDLES_LIST_PATH}"
    ).mock(
        return_value=httpx.Response(
            401, json={"detail": "Token revoked or expired"}
        )
    )

    result = cli_runner.invoke(cli, ["bundles", "list"])
    assert result.exit_code != 0, (result.output, result.stderr)
    # Verbatim string — no prefix, no suffix.
    assert "not authenticated; run verlet auth login" in (
        result.stderr or ""
    ), result.stderr


def test_list_status_color_coding(tmp_home, cli_runner, respx_mock):
    """Status column color-codes: active=green, expired=yellow, revoked=red.

    Verifies via the renderer's STATUS_STYLES table + the constructed Rich
    Table cells. Asserting against ANSI in CliRunner output is brittle; here
    we hit the renderer directly, which is the contract surface.
    """
    items = [
        _sample_active_item(),
        {**_sample_active_item("mit-pickplace"), "status": "expired"},
        {**_sample_active_item("revoked-grant"), "status": "revoked"},
    ]
    table = bundles_list_table(items)

    # STATUS_STYLES presence — asserted as a module-level constant so future
    # drift surfaces in the unit test rather than via visual inspection.
    assert STATUS_STYLES.get("active") == "green"
    assert STATUS_STYLES.get("expired") == "yellow"
    assert STATUS_STYLES.get("revoked") == "red"

    # The Status column is the last column; sanity-check we built three rows.
    assert table.row_count == 3

    # Pull the last column's cells (Status) and check the styled Text values.
    status_col = table.columns[-1]
    status_cells = list(status_col.cells)
    assert len(status_cells) == 3

    from rich.text import Text
    expected = [("active", "green"), ("expired", "yellow"), ("revoked", "red")]
    for cell, (label, style) in zip(status_cells, expected):
        assert isinstance(cell, Text), (
            f"status cell must be Rich Text for color, got {type(cell)!r}"
        )
        assert str(cell) == label
        assert cell.style == style
