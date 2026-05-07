"""Plan 30-10 Task 2 — opt-in telemetry via User-Agent header (CLIDIST-05).

Tests cover the privacy contract:

  * Default = OFF: ``~/.verlet/config.json`` absent or telemetry_enabled
    not exactly ``True`` -> User-Agent is bare ``verlet-cli/<version>``.
  * Pitfall 7 type strictness: only Python ``True`` (JSON ``true``) flips
    the flag. ``null`` / ``"true"`` / ``"false"`` / missing -> still OFF.
  * Privacy invariant: enabled User-Agent contains ONLY cli_version,
    python_version, os/arch — never command names, paths, dataset slugs,
    identities, or argv.
  * AuthenticatedClient injects User-Agent on every request.
  * Anonymous bundles browse calls ALSO include User-Agent (D-DIST1
    "every request").
  * `verlet config telemetry status|enable|disable` toggles the flag and
    persists via 0o600 mode on the file (Pitfall 7 file-system invariant).
"""
from __future__ import annotations

import json
import os
import stat

import httpx
import pytest

from verlet.cli import cli


# ---------------------------------------------------------------------------
# build_user_agent + telemetry_enabled — pure unit tests
# ---------------------------------------------------------------------------


def test_build_user_agent_disabled_is_bare(tmp_home):
    from verlet.telemetry import build_user_agent

    ua = build_user_agent("0.8.0", telemetry_enabled=False)
    assert ua == "verlet-cli/0.8.0"


def test_build_user_agent_enabled_includes_python_os_arch(tmp_home):
    import platform as _plat
    import sys as _sys

    from verlet.telemetry import build_user_agent

    ua = build_user_agent("0.8.0", telemetry_enabled=True)
    py = ".".join(map(str, _sys.version_info[:2]))
    plat = _plat.system().lower()
    arch = _plat.machine().lower()
    assert ua == f"verlet-cli/0.8.0 (python/{py} {plat}/{arch})"
    # Privacy invariant: NO command names, paths, slugs, identities.
    forbidden = (
        "datasets", "download", "push", "bundles", "info", "list",
        "redeem", "ego", "segment", "/", "verlet@", "user@",
    )
    for needle in forbidden:
        # "verlet-cli" prefix is allowed; ban inside the parens.
        # The simplest assertion: the inside-parens body has no slashes
        # (path) and no command verbs.
        if needle in ("/",):
            # Three slashes are expected in the normal UA shape:
            # verlet-cli/<v>, python/<py>, <os>/<arch>. More than that
            # would indicate a leaked filesystem path.
            assert ua.count("/") == 3, (
                f"UA has unexpected slash density (paths leak?): {ua!r}"
            )
            continue
        body = ua[ua.index("(") + 1 : ua.index(")")]
        assert needle not in body, (
            f"{needle!r} must NOT appear in telemetry UA body: {body!r}"
        )


def test_telemetry_enabled_default_off_when_no_config_file(tmp_home):
    from verlet.telemetry import telemetry_enabled

    # Fresh tmp_home; no ~/.verlet/config.json present.
    assert telemetry_enabled() is False


def test_telemetry_enabled_off_when_key_missing(tmp_home):
    from verlet.telemetry import telemetry_enabled

    cfg = tmp_home / ".verlet" / "config.json"
    cfg.write_text(json.dumps({"unrelated_key": "value"}))
    assert telemetry_enabled() is False


@pytest.mark.parametrize(
    "raw_value",
    [None, "true", "false", "True", "False", "1", 0, 1, [], {}, "yes"],
)
def test_telemetry_enabled_pitfall_7_type_strictness(tmp_home, raw_value):
    """Pitfall 7: ONLY Python bool ``True`` flips the flag. No other type."""
    from verlet.telemetry import telemetry_enabled

    cfg = tmp_home / ".verlet" / "config.json"
    cfg.write_text(json.dumps({"telemetry_enabled": raw_value}))
    assert telemetry_enabled() is False, (
        f"value {raw_value!r} must NOT enable telemetry (Pitfall 7)"
    )


def test_telemetry_enabled_only_when_exact_true_bool(tmp_home):
    from verlet.telemetry import telemetry_enabled

    cfg = tmp_home / ".verlet" / "config.json"
    cfg.write_text(json.dumps({"telemetry_enabled": True}))
    assert telemetry_enabled() is True


# ---------------------------------------------------------------------------
# verlet config telemetry status|enable|disable
# ---------------------------------------------------------------------------


def test_config_telemetry_status_default_disabled(tmp_home, cli_runner):
    result = cli_runner.invoke(cli, ["config", "telemetry", "status"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "disabled" in result.output.lower()


def test_config_telemetry_enable_writes_flag_and_status_reflects(
    tmp_home, cli_runner
):
    result = cli_runner.invoke(cli, ["config", "telemetry", "enable"])
    assert result.exit_code == 0, (result.output, result.stderr)

    cfg = tmp_home / ".verlet" / "config.json"
    assert cfg.exists()
    data = json.loads(cfg.read_text())
    assert data.get("telemetry_enabled") is True

    # Status now reports enabled.
    result2 = cli_runner.invoke(cli, ["config", "telemetry", "status"])
    assert result2.exit_code == 0
    assert "enabled" in result2.output.lower()
    assert "disabled" not in result2.output.lower()


def test_config_telemetry_enable_file_mode_is_0600(tmp_home, cli_runner):
    """Pitfall 7 filesystem invariant: config.json mode is exactly 0600."""
    result = cli_runner.invoke(cli, ["config", "telemetry", "enable"])
    assert result.exit_code == 0
    cfg = tmp_home / ".verlet" / "config.json"
    mode = stat.S_IMODE(os.stat(cfg).st_mode)
    assert mode == 0o600, f"expected 0o600, got 0o{mode:o}"


def test_config_telemetry_disable_clears_flag(tmp_home, cli_runner):
    # Enable first.
    cli_runner.invoke(cli, ["config", "telemetry", "enable"])
    cfg = tmp_home / ".verlet" / "config.json"
    assert json.loads(cfg.read_text()).get("telemetry_enabled") is True

    # Disable.
    result = cli_runner.invoke(cli, ["config", "telemetry", "disable"])
    assert result.exit_code == 0
    data = json.loads(cfg.read_text())
    assert data.get("telemetry_enabled") is not True

    # Status reflects.
    result2 = cli_runner.invoke(cli, ["config", "telemetry", "status"])
    assert result2.exit_code == 0
    assert "disabled" in result2.output.lower()


# ---------------------------------------------------------------------------
# AuthenticatedClient User-Agent injection
# ---------------------------------------------------------------------------


def test_authenticated_client_includes_user_agent_header(tmp_home, respx_mock):
    """Every AuthenticatedClient request carries a User-Agent header."""
    from verlet.api_client import AuthenticatedClient
    from verlet.auth.credentials import upsert_profile

    # Set up a PAT profile so AuthenticatedClient resolves cleanly.
    upsert_profile(
        "default",
        kind="pat",
        api_url="https://api.verlet.co",
        access_token="pat_lookup_secret",
        identity={"id": "u1", "email": "test@example.com"},
    )

    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/auth/me"
    ).mock(return_value=httpx.Response(200, json={"id": "u1"}))

    client = AuthenticatedClient("default")
    try:
        client.get("/api/platform/v1/auth/me")
    finally:
        client.close()

    assert route.called
    request = route.calls.last.request
    ua = request.headers.get("User-Agent", "")
    assert ua.startswith("verlet-cli/"), (
        f"expected verlet-cli/<v> User-Agent, got {ua!r}"
    )


def test_anonymous_bundles_browse_includes_user_agent(tmp_home, respx_mock):
    """Anonymous bundles browse also sends User-Agent (D-DIST1: every request)."""
    from verlet.bundles._api import fetch_bundles_browse
    import asyncio

    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/research-bundles"
    ).mock(return_value=httpx.Response(200, json={"items": []}))

    asyncio.run(fetch_bundles_browse(limit=10))

    assert route.called
    request = route.calls.last.request
    ua = request.headers.get("User-Agent", "")
    assert ua.startswith("verlet-cli/"), (
        f"anonymous browse must send User-Agent header, got {ua!r}"
    )
    # Anonymous: no Authorization header.
    assert "Authorization" not in request.headers


def test_user_agent_privacy_payload_no_command_or_path(tmp_home, respx_mock):
    """Privacy contract: UA never contains command names, paths, slugs, identities."""
    from verlet.bundles._api import fetch_bundles_browse
    import asyncio

    # Enable telemetry to exercise the rich UA path.
    cfg = tmp_home / ".verlet" / "config.json"
    cfg.write_text(json.dumps({"telemetry_enabled": True}))

    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/research-bundles"
    ).mock(return_value=httpx.Response(200, json={"items": []}))

    asyncio.run(fetch_bundles_browse(limit=10))

    request = route.calls.last.request
    ua = request.headers.get("User-Agent", "")
    # Forbidden tokens — no command names, no slugs, no paths, no identities.
    forbidden = [
        "browse", "redeem", "list", "info", "download", "push",
        "datasets", "bundles", "ego", "segment",
        "stanford-egocentric", "mit-pickplace", "ABCD-1234",
        "test@example.com", "u1@", "/home/", "/Users/",
        "--limit", "--profile", "--json",
    ]
    body = ua[ua.index("(") + 1 : ua.index(")")] if "(" in ua else ua
    for tok in forbidden:
        assert tok not in body, (
            f"forbidden token {tok!r} leaked into UA: {ua!r}"
        )
