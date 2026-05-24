"""Tests for `verlet showcase stats` (G-S3)."""
from __future__ import annotations

import json

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

STATS_URL = "https://api.verlet.co/api/v1/showcase/operation-stats"
FAR_FUTURE = "2099-01-01T00:00:00+00:00"

_STATS = {
    "rigs_deployed": 42,
    "ego_rigs_deployed": 18,
    "teleop_rigs_deployed": 24,
    "active_operators_7d": 31,
    "total_episodes_7d": 5400,
    "total_duration_secs_7d": 360000,
    "qc_pass_rate_7d": 0.964,
    "generated_at": "2026-05-22T00:00:00+00:00",
}


def _seed(kind: str = "showcase_access_code") -> None:
    upsert_profile(
        "default",
        kind=kind,
        api_url="https://api.verlet.co",
        access_token="showcase.jwt.value",
        expires_at=FAR_FUTURE,
    )


def test_stats_renders(tmp_home, cli_runner, respx_mock):
    """`showcase stats` GETs operation-stats with the Bearer header."""
    _seed()
    route = respx_mock.get(STATS_URL).mock(
        return_value=httpx.Response(200, json=_STATS)
    )

    result = cli_runner.invoke(cli, ["showcase", "stats"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    assert route.calls.last.request.headers.get("Authorization") == (
        "Bearer showcase.jwt.value"
    )
    assert "42" in result.output  # rigs_deployed
    assert "96.4%" in result.output  # qc pass rate


def test_stats_json_flag(tmp_home, cli_runner, respx_mock):
    """`--json` emits the raw OperationStatsResponse body."""
    _seed()
    respx_mock.get(STATS_URL).mock(
        return_value=httpx.Response(200, json=_STATS)
    )

    result = cli_runner.invoke(cli, ["showcase", "stats", "--json"])
    assert result.exit_code == 0, (result.output, result.stderr)
    parsed = json.loads(result.output)
    assert parsed["rigs_deployed"] == 42


def test_stats_rejects_non_showcase_profile(tmp_home, cli_runner, respx_mock):
    """A platform (device_flow) profile is rejected pre-HTTP."""
    _seed(kind="device_flow")

    result = cli_runner.invoke(cli, ["showcase", "stats"])
    assert result.exit_code != 0
    assert "showcase access code" in (result.output + (result.stderr or ""))
    # No HTTP call — respx would raise on an unmocked request.


def test_stats_unauthenticated(tmp_home, cli_runner, respx_mock):
    """No profile → friendly auth error, no traceback."""
    result = cli_runner.invoke(cli, ["showcase", "stats"])
    assert result.exit_code != 0
    assert "Not authenticated" in (result.output + (result.stderr or ""))
    assert "Traceback" not in result.output
