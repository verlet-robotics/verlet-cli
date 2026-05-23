"""Tests for `verlet datasets quality` / `analytics` (G-P6)."""
from __future__ import annotations

import json

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

QC_URL = (
    "https://api.verlet.co/api/platform/v1/catalog/datasets/"
    "imitate-cube/qc-distributions"
)
ANALYTICS_URL = (
    "https://api.verlet.co/api/platform/v1/catalog/datasets/imitate-cube/analytics"
)


def _seed() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


_QC_BODY = {
    "distributions": {
        "alignment_score": {
            "mean": 0.92,
            "std": 0.04,
            "min": 0.81,
            "max": 0.99,
            "values": [0.9, 0.95],
        },
        "mean_jerk": {
            "mean": 1.2,
            "std": 0.3,
            "min": 0.5,
            "max": 2.1,
            "values": [1.0, 1.4],
        },
    }
}

_ANALYTICS_BODY = {
    "episode_count": 120,
    "episodes_with_qc": 118,
    "qc_status_counts": {"passed": 110, "flagged": 8},
    "duration": {
        "stats": {
            "mean": 12.0,
            "std": 2.0,
            "min": 8.0,
            "max": 20.0,
            "median": 11.5,
            "cv": 0.16,
            "count": 120,
        },
        "histogram": [],
    },
    "integrity_checks": [],
}


def test_quality_renders_distributions(tmp_home, cli_runner, respx_mock):
    """`datasets quality` renders one row per QC check."""
    route = respx_mock.get(QC_URL).mock(
        return_value=httpx.Response(200, json=_QC_BODY)
    )
    result = cli_runner.invoke(cli, ["datasets", "quality", "imitate-cube"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    assert "alignment_score" in result.output
    assert "mean_jerk" in result.output
    # Anonymous — no Bearer header.
    assert "Authorization" not in route.calls.last.request.headers


def test_quality_empty(tmp_home, cli_runner, respx_mock):
    """No distributions → the dim 'not available' message."""
    respx_mock.get(QC_URL).mock(
        return_value=httpx.Response(200, json={"distributions": {}})
    )
    result = cli_runner.invoke(cli, ["datasets", "quality", "imitate-cube"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "No QC distributions" in result.output


def test_quality_json_flag(tmp_home, cli_runner, respx_mock):
    """`--json` emits the raw distributions body."""
    respx_mock.get(QC_URL).mock(
        return_value=httpx.Response(200, json=_QC_BODY)
    )
    result = cli_runner.invoke(
        cli, ["datasets", "quality", "imitate-cube", "--json"]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    parsed = json.loads(result.output)
    assert "alignment_score" in parsed["distributions"]


def test_analytics_renders(tmp_home, cli_runner, respx_mock):
    """`datasets analytics` renders summary + QC status + metric tables."""
    route = respx_mock.get(ANALYTICS_URL).mock(
        return_value=httpx.Response(200, json=_ANALYTICS_BODY)
    )
    result = cli_runner.invoke(cli, ["datasets", "analytics", "imitate-cube"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    assert "120" in result.output  # episode_count
    assert "passed" in result.output
    assert "duration" in result.output


def test_analytics_authenticated_sends_bearer(tmp_home, cli_runner, respx_mock):
    """With an active profile the analytics call carries the Bearer header."""
    _seed()
    route = respx_mock.get(ANALYTICS_URL).mock(
        return_value=httpx.Response(200, json=_ANALYTICS_BODY)
    )
    result = cli_runner.invoke(cli, ["datasets", "analytics", "imitate-cube"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.calls.last.request.headers.get("Authorization") == (
        "Bearer jwt.access.value"
    )


def test_analytics_json_flag(tmp_home, cli_runner, respx_mock):
    """`--json` emits the raw analytics body."""
    respx_mock.get(ANALYTICS_URL).mock(
        return_value=httpx.Response(200, json=_ANALYTICS_BODY)
    )
    result = cli_runner.invoke(
        cli, ["datasets", "analytics", "imitate-cube", "--json"]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    parsed = json.loads(result.output)
    assert parsed["episode_count"] == 120
