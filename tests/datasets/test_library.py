"""Tests for `verlet datasets library` (G-P1).

Coverage:
  * test_library_calls_authenticated_endpoint — GETs /downloads/library with
    the active profile's Bearer header and renders a Rich table.
  * test_library_renders_datasets_and_bundles — both sections surface.
  * test_library_empty_message — empty library shows the dim hint.
  * test_library_json_flag — `--json` emits the raw LibraryListResponse.
  * test_library_showcase_kind_rejected — showcase access codes are rejected
    pre-HTTP with a platform-account pointer.
  * test_library_unauthenticated — no profile → friendly auth error, no HTTP.
  * test_library_401_friendly_error — backend 401 surfaces without a traceback.
"""
from __future__ import annotations

import json

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

LIBRARY_PATH = "/api/platform/v1/downloads/library"
FAR_FUTURE = "2099-01-01T00:00:00+00:00"


def _seed_default_profile(kind: str = "device_flow") -> None:
    upsert_profile(
        "default",
        kind=kind,
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at=FAR_FUTURE,
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _sample_dataset(slug: str = "imitate-cube") -> dict:
    return {
        "purchase_id": "00000000-0000-0000-0000-0000000000a1",
        "catalog_dataset_id": "00000000-0000-0000-0000-0000000000b1",
        "dataset_slug": slug,
        "dataset_title": "Imitate Cube",
        "episode_count": 120,
        "total_hours": 8.5,
        "status": "completed",
        "total_price_cents": 12000,
        "currency": "USD",
        "license_type": "commercial",
        "variant": "processed",
        "purchased_at": "2026-04-15T12:00:00+00:00",
        "available_formats": ["lerobot-v2", "hdf5"],
    }


def _sample_bundle(slug: str = "stanford-egocentric-2024") -> dict:
    return {
        "purchase_id": "00000000-0000-0000-0000-0000000000c1",
        "bundle_slug": slug,
        "bundle_name": "Stanford Egocentric 2024",
        "bundle_version": 1,
        "license_tier": "research",
        "expires_at": FAR_FUTURE,
        "total_hours": 142.5,
        "dataset_count": 12,
        "contained_dataset_slugs": ["a", "b"],
    }


def test_library_calls_authenticated_endpoint(tmp_home, cli_runner, respx_mock):
    """`verlet datasets library` GETs /downloads/library with Authorization."""
    _seed_default_profile()
    route = respx_mock.get(f"https://api.verlet.co{LIBRARY_PATH}").mock(
        return_value=httpx.Response(
            200, json={"datasets": [_sample_dataset()], "count": 1, "bundles": []}
        )
    )

    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    assert route.calls.last.request.headers.get("Authorization") == (
        "Bearer jwt.access.value"
    )
    assert "imitate-cube" in result.output


def test_library_renders_datasets_and_bundles(tmp_home, cli_runner, respx_mock):
    """Both the purchased-datasets and bundles sections surface."""
    _seed_default_profile()
    respx_mock.get(f"https://api.verlet.co{LIBRARY_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "datasets": [_sample_dataset()],
                "count": 1,
                "bundles": [_sample_bundle()],
            },
        )
    )

    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "imitate-cube" in result.output
    assert "stanford-egocentric-2024" in result.output


def test_library_empty_message(tmp_home, cli_runner, respx_mock):
    """An empty library renders the dim 'browse the catalog' hint."""
    _seed_default_profile()
    respx_mock.get(f"https://api.verlet.co{LIBRARY_PATH}").mock(
        return_value=httpx.Response(
            200, json={"datasets": [], "count": 0, "bundles": []}
        )
    )

    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "Your library is empty" in result.output


def test_library_json_flag(tmp_home, cli_runner, respx_mock):
    """`--json` emits the raw LibraryListResponse body."""
    _seed_default_profile()
    body = {"datasets": [_sample_dataset()], "count": 1, "bundles": []}
    respx_mock.get(f"https://api.verlet.co{LIBRARY_PATH}").mock(
        return_value=httpx.Response(200, json=body)
    )

    result = cli_runner.invoke(cli, ["datasets", "library", "--json"])
    assert result.exit_code == 0, (result.output, result.stderr)
    parsed = json.loads(result.output)
    assert parsed["count"] == 1
    assert parsed["datasets"][0]["dataset_slug"] == "imitate-cube"


def test_library_showcase_kind_rejected(tmp_home, cli_runner, respx_mock):
    """Showcase access codes are rejected pre-HTTP with a platform pointer."""
    _seed_default_profile(kind="showcase_access_code")

    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code != 0
    assert "platform-account feature" in (result.output + (result.stderr or ""))
    # No HTTP call was made — respx would have raised on an unmocked request.


def test_library_unauthenticated(tmp_home, cli_runner, respx_mock):
    """No active profile → friendly auth error, no HTTP, no traceback."""
    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code != 0
    assert "Not authenticated" in (result.output + (result.stderr or ""))
    assert "Traceback" not in result.output


def test_library_401_friendly_error(tmp_home, cli_runner, respx_mock):
    """A backend 401 surfaces as a friendly error, not a Python traceback."""
    _seed_default_profile()
    respx_mock.get(f"https://api.verlet.co{LIBRARY_PATH}").mock(
        return_value=httpx.Response(401, json={"detail": "Token expired"})
    )

    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert "Token expired" in (result.output + (result.stderr or ""))
