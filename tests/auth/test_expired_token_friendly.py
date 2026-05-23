"""Friendly handling of expired non-refreshable profiles.

Two integration paths, both end-to-end through the Click runner:

* Authed-required commands fail fast pre-HTTP with the per-kind refresh
  command, not the backend's bare ``Invalid or expired …`` body.
* Anonymous-OK catalog browse (``datasets list``/``info``/``episodes``…)
  drops the dead Bearer header and continues against the public surface,
  emitting a one-line stderr hint so the user knows to refresh.

Device-flow profiles are exempt — their refresh path
(``AuthenticatedClient._refresh_if_needed``) handles expiry transparently.
"""
from __future__ import annotations

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli
from verlet.datasets import _api as catalog_api

PAST = "2020-01-01T00:00:00+00:00"
FAR_FUTURE = "2099-01-01T00:00:00+00:00"


def _seed(kind: str, expires_at: str = PAST) -> None:
    upsert_profile(
        "default",
        kind=kind,
        api_url="https://api.verlet.co",
        access_token="dead.jwt.value",
        refresh_token="dead-rt",
        expires_at=expires_at,
    )


# ---------------------------------------------------------------------------
# AuthenticatedClient — authed-required surfaces.
# ---------------------------------------------------------------------------


def test_library_with_expired_showcase_says_refresh_kind_showcase(
    tmp_home, cli_runner, respx_mock
):
    """Showcase-kind expired → friendly error pointing at --kind showcase,
    BUT showcase is also rejected pre-HTTP because library is platform-only.
    The credential-kind guard fires first, which is correct — a showcase user
    shouldn't be told to refresh showcase when the feature isn't theirs."""
    _seed(kind="showcase_access_code")
    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code != 0
    # The kind-mismatch guard takes precedence over the expiry guard here.
    assert "platform-account feature" in (result.output + (result.stderr or ""))


def test_library_with_expired_pat_says_create_pat(tmp_home, cli_runner, respx_mock):
    """PAT-kind expired → friendly error pointing at `auth tokens create`."""
    _seed(kind="pat")
    result = cli_runner.invoke(cli, ["datasets", "library"])
    assert result.exit_code != 0
    out = result.output + (result.stderr or "")
    assert "expired" in out.lower()
    assert "verlet auth tokens create" in out
    # No HTTP fired — respx would raise on an unmocked request.


def test_destinations_list_with_expired_pat_says_refresh(
    tmp_home, cli_runner, respx_mock
):
    """Every AuthenticatedClient-backed command gets the same treatment."""
    _seed(kind="pat")
    result = cli_runner.invoke(cli, ["destinations", "list"])
    assert result.exit_code != 0
    out = result.output + (result.stderr or "")
    assert "expired" in out.lower()
    assert "verlet auth tokens create" in out


def test_showcase_stats_with_expired_showcase_says_refresh_kind_showcase(
    tmp_home, cli_runner, respx_mock
):
    """Showcase stats requires showcase-kind; the expiry guard fires AFTER
    the kind guard accepts, so the user sees the showcase refresh command."""
    _seed(kind="showcase_access_code")
    result = cli_runner.invoke(cli, ["showcase", "stats"])
    assert result.exit_code != 0
    out = result.output + (result.stderr or "")
    assert "expired" in out.lower()
    assert "verlet auth login --kind showcase" in out


def test_authenticated_client_skips_expiry_check_for_device_flow(tmp_home):
    """Device-flow profiles have a refresh path — they must NOT be rejected
    by the pre-flight check; the refresh runs on the next request instead."""
    _seed(kind="device_flow")
    # Should construct without raising. The refresh logic only fires on
    # request(), not on __init__.
    from verlet.api_client import AuthenticatedClient

    client = AuthenticatedClient("default")
    try:
        # Profile is loaded, headers carry the (dead) Bearer — the refresh
        # round trip is what surfaces the dead session, not the ctor.
        assert "Authorization" in client.headers()
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Catalog browse anonymous-OK fallback.
# ---------------------------------------------------------------------------


def test_catalog_list_with_expired_pat_falls_back_to_anonymous(
    tmp_home, cli_runner, respx_mock
):
    """An expired PAT must NOT break public catalog browse.

    The dead Bearer is dropped, the request goes anonymous (no
    Authorization header), and a one-line stderr hint tells the user how
    to refresh.
    """
    # Reset the per-process dedup set so we see the warning in this test.
    catalog_api._EXPIRY_HINT_EMITTED.clear()
    _seed(kind="pat")
    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"items": [], "total": 0, "page": 1, "page_size": 20},
        )
    )

    result = cli_runner.invoke(cli, ["datasets", "list"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    # The Bearer must NOT have been sent — anonymous fallback.
    assert "Authorization" not in route.calls.last.request.headers
    # Stderr (mixed into result.output under CliRunner) carries the hint.
    assert "expired" in result.output.lower() or "expired" in (
        result.stderr or ""
    ).lower()
    assert "verlet auth tokens create" in result.output + (result.stderr or "")


def test_catalog_browse_emits_expiry_hint_once_per_process(
    tmp_home, cli_runner, respx_mock
):
    """The dedup set keeps the stderr stream clean on commands that fire
    multiple catalog requests."""
    catalog_api._EXPIRY_HINT_EMITTED.clear()
    _seed(kind="pat")
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"items": [], "total": 0, "page": 1, "page_size": 20},
        )
    )
    # First call: hint emitted, default profile recorded in the dedup set.
    cli_runner.invoke(cli, ["datasets", "list"])
    assert "default" in catalog_api._EXPIRY_HINT_EMITTED
    # Second call: no second hint should be queued — the helper short-circuits.
    catalog_api._api_url_and_headers("default")
    # Set is unchanged — still only one entry.
    assert catalog_api._EXPIRY_HINT_EMITTED == {"default"}


def test_catalog_browse_with_unexpired_pat_keeps_bearer(
    tmp_home, cli_runner, respx_mock
):
    """A live PAT continues to authenticate normally — the expiry path is
    only triggered when ``expires_at`` is already in the past."""
    catalog_api._EXPIRY_HINT_EMITTED.clear()
    _seed(kind="pat", expires_at=FAR_FUTURE)
    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"items": [], "total": 0, "page": 1, "page_size": 20},
        )
    )

    result = cli_runner.invoke(cli, ["datasets", "list"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert (
        route.calls.last.request.headers.get("Authorization")
        == "Bearer dead.jwt.value"
    )
