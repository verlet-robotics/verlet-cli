"""Tests for `verlet datasets episodes` / `segments` (G-P7)."""
from __future__ import annotations

import json

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

EPISODES_URL = (
    "https://api.verlet.co/api/platform/v1/catalog/datasets/imitate-cube/episodes"
)
SEGMENTS_URL = (
    "https://api.verlet.co/api/platform/v1/catalog/datasets/cooking-ego/segments"
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


def _episode(idx: int) -> dict:
    return {
        "id": f"ep-{idx}",
        "dataset_index": idx,
        "duration_secs": 12.5,
        "frame_count": 375,
        "qc_status": "passed",
        "is_free_sample": idx == 0,
        "thumbnail_url": None,
    }


def _segment(idx: int) -> dict:
    return {
        "id": f"seg-{idx}",
        "dataset_index": idx,
        "thumbnail_url": None,
        "name": f"segment {idx}",
        "duration_s": 8.0,
        "hand_coverage": 0.92,
        "mean_hand_confidence": 0.81,
        "category": "cooking",
        "subcategory": "chopping",
        "has_depth": True,
        "is_free_sample": False,
    }


def test_episodes_renders_and_sends_pagination(tmp_home, cli_runner, respx_mock):
    """`datasets episodes` sends page/page_size and renders the index column."""
    route = respx_mock.get(EPISODES_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [_episode(0), _episode(1)],
                "total": 2,
                "page": 1,
                "page_size": 20,
            },
        )
    )

    result = cli_runner.invoke(
        cli, ["datasets", "episodes", "imitate-cube", "--limit", "20"]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    params = dict(route.calls.last.request.url.params)
    assert params.get("page") == "1"
    assert params.get("page_size") == "20"
    # Anonymous path — no Authorization header.
    assert "Authorization" not in route.calls.last.request.headers


def test_episodes_authenticated_sends_bearer(tmp_home, cli_runner, respx_mock):
    """With an active profile the episodes call carries the Bearer header."""
    _seed()
    route = respx_mock.get(EPISODES_URL).mock(
        return_value=httpx.Response(
            200, json={"items": [_episode(0)], "total": 1, "page": 1, "page_size": 20}
        )
    )

    result = cli_runner.invoke(cli, ["datasets", "episodes", "imitate-cube"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.calls.last.request.headers.get("Authorization") == (
        "Bearer jwt.access.value"
    )


def test_episodes_empty(tmp_home, cli_runner, respx_mock):
    """An empty episode list renders the dim 'no episodes' message."""
    respx_mock.get(EPISODES_URL).mock(
        return_value=httpx.Response(
            200, json={"items": [], "total": 0, "page": 1, "page_size": 20}
        )
    )
    result = cli_runner.invoke(cli, ["datasets", "episodes", "imitate-cube"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "No episodes found" in result.output


def test_episodes_json_flag(tmp_home, cli_runner, respx_mock):
    """`--json` emits the raw PaginatedResponse body."""
    respx_mock.get(EPISODES_URL).mock(
        return_value=httpx.Response(
            200, json={"items": [_episode(3)], "total": 1, "page": 1, "page_size": 20}
        )
    )
    result = cli_runner.invoke(
        cli, ["datasets", "episodes", "imitate-cube", "--json"]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    parsed = json.loads(result.output)
    assert parsed["items"][0]["dataset_index"] == 3


def test_episodes_page_footer_when_truncated(tmp_home, cli_runner, respx_mock):
    """When more pages remain, a truncation footer is shown."""
    respx_mock.get(EPISODES_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [_episode(i) for i in range(20)],
                "total": 95,
                "page": 1,
                "page_size": 20,
            },
        )
    )
    result = cli_runner.invoke(cli, ["datasets", "episodes", "imitate-cube"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "showing 20 of 95" in result.output


def test_segments_renders(tmp_home, cli_runner, respx_mock):
    """`datasets segments` renders an ego dataset's segments."""
    respx_mock.get(SEGMENTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [_segment(0), _segment(1)],
                "total": 2,
                "page": 1,
                "page_size": 20,
            },
        )
    )
    result = cli_runner.invoke(cli, ["datasets", "segments", "cooking-ego"])
    assert result.exit_code == 0, (result.output, result.stderr)
    # The ID column is no-wrap, so it survives narrow-terminal rendering.
    assert "seg-0" in result.output
    assert "cooking" in result.output


def test_segments_empty_for_teleop(tmp_home, cli_runner, respx_mock):
    """A teleop dataset returns an empty segment list → ego-only hint."""
    respx_mock.get(SEGMENTS_URL).mock(
        return_value=httpx.Response(
            200, json={"items": [], "total": 0, "page": 1, "page_size": 20}
        )
    )
    result = cli_runner.invoke(cli, ["datasets", "segments", "cooking-ego"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "ego-only" in result.output
