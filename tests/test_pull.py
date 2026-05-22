"""Tests for ``verlet pull <slug>`` — branch on profile kind, render errors,
build the download plan correctly."""
from __future__ import annotations

import json

import pytest

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_showcase_profile() -> None:
    upsert_profile(
        "default",
        kind="showcase_access_code",
        api_url="https://api.verlet.co",
        access_token="showcase.jwt.token",
        customer_name="Acme Robotics",
        expires_at="2099-01-01T00:00:00+00:00",
    )


def _seed_device_flow_profile() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="platform.jwt.token",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
    )


def _showcase_manifest(slug: str = "acme-ds-1") -> dict:
    return {
        "dataset_title": "Acme Dataset 1",
        "dataset_slug": slug,
        "format": "lerobot-v2",
        "variant": "processed",
        "scope": "samples",
        "episodes": [
            {
                "episode_index": 0,
                "parquet_url": "https://signed.example/ep0.parquet",
                "video_urls": [
                    {"camera": "cam0", "url": "https://signed.example/ep0/cam0.mp4"}
                ],
                "meta_urls": [
                    {"filename": "info.json", "url": "https://signed.example/info.json"}
                ],
            }
        ],
        "quota_remaining": {"bytes": None, "episodes": 4},
    }


# ---------------------------------------------------------------------------
# Profile branching
# ---------------------------------------------------------------------------


def test_pull_no_profile_errors_with_login_hint(cli_runner, tmp_home):
    result = cli_runner.invoke(cli, ["pull", "any-slug"])
    assert result.exit_code != 0
    out = (result.stderr or "") + (result.output or "")
    assert "auth login" in out.lower()


def test_pull_showcase_profile_hits_showcase_endpoint_dry_run(
    cli_runner, respx_mock, tmp_home
):
    _seed_showcase_profile()
    slug = "acme-ds-1"
    route = respx_mock.get(
        f"https://api.verlet.co/api/v1/showcase/datasets/{slug}/download",
    ).respond(200, json=_showcase_manifest(slug))

    result = cli_runner.invoke(
        cli,
        ["pull", slug, "--variant", "processed", "--scope", "samples", "--dry-run"],
    )
    assert result.exit_code == 0, result.output + (result.stderr or "")
    assert route.called
    # Verify the request carried the showcase bearer header + query params.
    req = route.calls.last.request
    assert req.headers["Authorization"] == "Bearer showcase.jwt.token"
    assert "variant=processed" in str(req.url)
    assert "scope=samples" in str(req.url)
    # Plan output mentions the slug.
    assert slug in result.output


def test_pull_device_flow_routes_to_platform_endpoint(
    cli_runner, respx_mock, tmp_home
):
    _seed_device_flow_profile()
    slug = "public-ds"
    # Manifest shape: same field names as showcase, no variant/scope.
    manifest = {
        "dataset_title": "Public Dataset",
        "dataset_slug": slug,
        "format": "lerobot-v2",
        "episodes": [
            {
                "episode_index": 0,
                "parquet_url": "https://signed.example/pub/ep0.parquet",
                "video_urls": [],
                "meta_urls": [],
            }
        ],
    }
    route = respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}/samples/download",
    ).respond(200, json=manifest)
    # The showcase route MUST NOT be hit for a device-flow profile.
    showcase_route = respx_mock.get(
        f"https://api.verlet.co/api/v1/showcase/datasets/{slug}/download",
    ).respond(200, json=manifest)

    result = cli_runner.invoke(
        cli,
        ["pull", slug, "--scope", "samples", "--dry-run"],
    )
    assert result.exit_code == 0, result.output + (result.stderr or "")
    assert route.called
    assert not showcase_route.called


def test_pull_device_flow_full_scope_errors_clearly(cli_runner, tmp_home):
    _seed_device_flow_profile()
    result = cli_runner.invoke(cli, ["pull", "x", "--scope", "full"])
    assert result.exit_code != 0
    out = (result.stderr or "") + (result.output or "")
    # Should point user at platform purchase flow.
    assert "verlet.co/catalog" in out or "samples" in out.lower()


# ---------------------------------------------------------------------------
# Error rendering
# ---------------------------------------------------------------------------


def test_pull_404_renders_no_access_message(cli_runner, respx_mock, tmp_home):
    _seed_showcase_profile()
    slug = "hidden-ds"
    respx_mock.get(
        f"https://api.verlet.co/api/v1/showcase/datasets/{slug}/download",
    ).respond(404, json={"detail": "Dataset not found"})

    result = cli_runner.invoke(cli, ["pull", slug, "--dry-run"])
    assert result.exit_code != 0
    out = (result.stderr or "") + (result.output or "")
    assert "No access" in out


def test_pull_429_renders_rate_limit_message(cli_runner, respx_mock, tmp_home):
    _seed_showcase_profile()
    slug = "quota-ds"
    respx_mock.get(
        f"https://api.verlet.co/api/v1/showcase/datasets/{slug}/download",
    ).respond(429, json={"detail": "Episode quota exhausted"})

    result = cli_runner.invoke(cli, ["pull", slug, "--dry-run"])
    assert result.exit_code != 0
    out = (result.stderr or "") + (result.output or "")
    assert "Rate-limited" in out or "quota" in out.lower()


# ---------------------------------------------------------------------------
# Plan layout
# ---------------------------------------------------------------------------


def test_plan_items_layout_groups_files_by_episode(tmp_home):
    """Smoke check the internal planner: parquet + video go under
    <output>/<slug>/episode_NNNNNN/, meta goes under <output>/<slug>/meta/."""
    from pathlib import Path

    from verlet.pull.commands import _plan_items

    items = _plan_items(
        "acme-ds-1",
        Path("/tmp/out"),
        _showcase_manifest(),
    )
    paths = [str(it.local_path) for it in items]
    assert any(p.endswith("/acme-ds-1/episode_000000/episode_000000.parquet") for p in paths)
    assert any(p.endswith("/acme-ds-1/episode_000000/videos/cam0.mp4") for p in paths)
    assert any(p.endswith("/acme-ds-1/meta/info.json") for p in paths)


def test_plan_items_dedupes_meta_across_episodes(tmp_home):
    """If two episodes carry the same meta file (the backend stamps a
    dataset-global meta on each episode), the plan must emit it once."""
    from pathlib import Path

    from verlet.pull.commands import _plan_items

    manifest = _showcase_manifest()
    # Duplicate episode-0's meta on episode-1.
    manifest["episodes"].append(
        {
            "episode_index": 1,
            "parquet_url": "https://signed.example/ep1.parquet",
            "video_urls": [],
            "meta_urls": manifest["episodes"][0]["meta_urls"],
        }
    )
    items = _plan_items("acme-ds-1", Path("/tmp/out"), manifest)
    info_jsons = [it for it in items if it.local_path.name == "info.json"]
    assert len(info_jsons) == 1
