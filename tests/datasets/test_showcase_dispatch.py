"""Credential-kind dispatch: showcase access codes route `verlet datasets`
to the gated `/api/v1/showcase/datasets/*` surface; platform accounts keep
hitting `/api/platform/*`.

Also covers the removal of `verlet ego` and the showcase-only flag rejection
on `datasets download`. All assertions use ``result.output`` / ``exit_code``
and respx call inspection — never ``result.stderr``.
"""
from __future__ import annotations

from verlet.auth.credentials import upsert_profile


def _seed_showcase_profile(_tmp_home, *, token: str = "sc-t0k3n") -> None:
    upsert_profile(
        "default",
        kind="showcase_access_code",
        access_token=token,
        api_url="https://api.verlet.co",
    )


def _seed_platform_profile(_tmp_home, *, token: str = "pf-t0k3n") -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        access_token=token,
        api_url="https://api.verlet.co",
    )


def _paths(respx_mock) -> list[str]:
    return [c.request.url.path for c in respx_mock.calls]


def test_list_showcase_credential_hits_gated_endpoint(
    cli_runner, respx_mock, tmp_home
):
    """A showcase access code routes `datasets list` to /api/v1/showcase/datasets."""
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    respx_mock.get("https://api.verlet.co/api/v1/showcase/datasets").respond(
        200,
        json={
            "datasets": [
                {
                    "id": "00000000-0000-0000-0000-0000000000aa",
                    "slug": "granted-ego-ds",
                    "title": "Granted Ego DS",
                    "modality": "ego",
                    "variants_available": ["raw"],
                    "episode_count": 3,
                    "total_hours": 1.0,
                }
            ],
            "total": 1,
        },
    )

    result = cli_runner.invoke(cli, ["datasets", "list"])
    assert result.exit_code == 0, result.output

    paths = _paths(respx_mock)
    assert "/api/v1/showcase/datasets" in paths, paths
    assert not any(p.startswith("/api/platform/") for p in paths), paths
    assert "granted-ego-ds" in result.output


def test_list_platform_credential_hits_platform_endpoint(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response
):
    """A device-flow account still routes `datasets list` to /api/platform/*."""
    from verlet.cli import cli

    _seed_platform_profile(tmp_home)
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets"
    ).respond(200, json=mock_catalog_list_response)

    result = cli_runner.invoke(cli, ["datasets", "list"])
    assert result.exit_code == 0, result.output

    paths = _paths(respx_mock)
    assert "/api/platform/v1/catalog/datasets" in paths, paths
    assert not any(p.startswith("/api/v1/showcase/") for p in paths), paths


def test_info_showcase_shows_grants_no_segment_ids(
    cli_runner, respx_mock, tmp_home
):
    """`datasets info` under a showcase code renders grants, never segment UUIDs."""
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    leak_uuid = "deadbeef-0000-0000-0000-000000000000"
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds"
    ).respond(
        200,
        json={
            "id": "00000000-0000-0000-0000-0000000000aa",
            "slug": "granted-ego-ds",
            "title": "Granted Ego DS",
            "modality": "ego",
            "segment_count": 12,
            "variants_available": ["raw", "processed"],
            # A leaky payload: even if the backend ever included segment IDs,
            # the showcase renderer must not print them.
            "segments": [{"id": leak_uuid}],
            "effective_grants": [
                {"variant": "raw", "scope": "full", "quota_remaining": None},
            ],
        },
    )

    result = cli_runner.invoke(cli, ["datasets", "info", "granted-ego-ds"])
    assert result.exit_code == 0, result.output
    assert "/api/v1/showcase/datasets/granted-ego-ds" in _paths(respx_mock)
    assert leak_uuid not in result.output
    # Grants table is rendered.
    assert "raw" in result.output and "full" in result.output


def test_info_showcase_renders_quota_and_expiry(cli_runner, respx_mock, tmp_home):
    """`datasets info` surfaces a grant's remaining quota + expiry (G-S1).

    A prospect must be able to see 'downloads used vs quota' and 'valid until'
    before a 429 surprises them. The data already ships on the showcase detail
    response — this asserts the renderer surfaces it.
    """
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds"
    ).respond(
        200,
        json={
            "id": "00000000-0000-0000-0000-0000000000aa",
            "slug": "granted-ego-ds",
            "title": "Granted Ego DS",
            "modality": "ego",
            "segment_count": 12,
            "variants_available": ["processed"],
            "effective_grants": [
                {
                    "variant": "processed",
                    "scope": "samples",
                    "expires_at": "2026-12-31T00:00:00+00:00",
                    "quota_remaining": {"bytes": 5_000_000_000, "episodes": 40},
                },
            ],
        },
    )

    result = cli_runner.invoke(cli, ["datasets", "info", "granted-ego-ds"])
    assert result.exit_code == 0, result.output
    # Quota: bytes pass through format_bytes; episodes render as "<n> units".
    assert "5.0 GB" in result.output
    assert "40 units" in result.output
    # Grant expiry surfaces verbatim (date portion is enough).
    assert "2026-12-31" in result.output


def test_download_showcase_rejects_platform_flags(cli_runner, tmp_home):
    """Showcase credentials cannot use --format/--detach/--episode-ids etc."""
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    result = cli_runner.invoke(
        cli, ["datasets", "download", "granted-ego-ds", "--format", "hdf5"]
    )
    assert result.exit_code != 0
    assert "showcase" in result.output.lower()


def test_download_showcase_hits_gated_download_endpoint(
    cli_runner, respx_mock, tmp_home
):
    """Showcase `datasets download` routes to the gated download endpoint.

    Ego datasets require ``--variant``; the pre-flight modality check
    that enforces this also fetches the detail endpoint, so both routes
    are mocked.
    """
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds"
    ).respond(
        200,
        json={
            "id": "granted-ego-ds",
            "slug": "granted-ego-ds",
            "title": "Granted Ego DS",
            "modality": "ego",
            "task_type": "ego",
            "robot_embodiment": "human-ego",
            "episode_count": 10,
            "total_hours": 1.0,
            "effective_grants": [
                {"variant": "raw", "scope": "full", "expires_at": None,
                 "quota_remaining": None}
            ],
        },
    )
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds/download"
    ).respond(
        200,
        json={
            "dataset_title": "Granted Ego DS",
            "dataset_slug": "granted-ego-ds",
            "format": "ego-segments-raw",
            "modality": "ego",
            "variant": "raw",
            "scope": "full",
            "episodes": [],
            "segments": [
                {
                    "segment_id": "seg-1",
                    "dataset_index": 0,
                    "duration_s": 10.0,
                    "is_free_sample": False,
                    "files": [
                        {
                            "role": "rgb",
                            "key": "segments/ep/seg-1/rgb.mp4",
                            "url": "https://signed.example/rgb.mp4",
                        }
                    ],
                }
            ],
            "quota_remaining": None,
        },
    )

    result = cli_runner.invoke(
        cli,
        [
            "datasets", "download", "granted-ego-ds",
            "--variant", "raw", "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (
        "/api/v1/showcase/datasets/granted-ego-ds/download" in _paths(respx_mock)
    )


def test_download_showcase_limit_forwarded_as_query_param(
    cli_runner, respx_mock, tmp_home
):
    """``--limit N`` is sent to the gated download endpoint as ``?limit=N``."""
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds"
    ).respond(
        200,
        json={
            "id": "granted-ego-ds",
            "slug": "granted-ego-ds",
            "title": "Granted Ego DS",
            "modality": "ego",
            "task_type": "ego",
            "robot_embodiment": "human-ego",
            "episode_count": 10,
            "total_hours": 1.0,
            "effective_grants": [
                {"variant": "raw", "scope": "full", "expires_at": None,
                 "quota_remaining": None}
            ],
        },
    )
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds/download"
    ).respond(
        200,
        json={
            "dataset_title": "Granted Ego DS",
            "dataset_slug": "granted-ego-ds",
            "format": "ego-segments-raw",
            "modality": "ego",
            "variant": "raw",
            "scope": "full",
            "episodes": [],
            "segments": [],
            "quota_remaining": None,
        },
    )

    result = cli_runner.invoke(
        cli,
        [
            "datasets", "download", "granted-ego-ds",
            "--variant", "raw", "--limit", "3", "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    dl_calls = [
        c for c in respx_mock.calls
        if c.request.url.path.endswith("/granted-ego-ds/download")
    ]
    assert dl_calls, "download endpoint was not called"
    assert b"limit=3" in dl_calls[-1].request.url.query


def test_download_limit_rejected_for_platform_account(cli_runner, tmp_home):
    """``--limit`` is showcase-only; a platform account is told to use
    --episode-ids instead. Fails pre-HTTP, so no mock is needed."""
    from verlet.cli import cli

    _seed_platform_profile(tmp_home)
    result = cli_runner.invoke(
        cli, ["datasets", "download", "some-ds", "--limit", "5"]
    )
    assert result.exit_code != 0
    out = result.output.lower()
    assert "--limit" in out and "showcase" in out


def test_download_showcase_limit_zero_rejected(cli_runner, tmp_home):
    """``--limit 0`` is rejected by click's IntRange(min=1) before any HTTP."""
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    result = cli_runner.invoke(
        cli, ["datasets", "download", "granted-ego-ds", "--limit", "0"]
    )
    assert result.exit_code != 0


def test_download_showcase_ego_without_variant_friendly_error(
    cli_runner, respx_mock, tmp_home
):
    """An ego showcase dataset called with no ``--variant`` must NOT relay
    the backend's raw Pydantic 422 ``[{'type': 'missing', ...}]`` body —
    the CLI catches it pre-HTTP via a modality detail fetch and surfaces
    a usage error listing the variants the caller's grants actually cover.
    """
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds"
    ).respond(
        200,
        json={
            "id": "granted-ego-ds",
            "slug": "granted-ego-ds",
            "title": "Granted Ego DS",
            # No explicit modality — exercises the resolve_modality fallback.
            "task_type": "ego",
            "robot_embodiment": "human-ego",
            "episode_count": 10,
            "total_hours": 1.0,
            "effective_grants": [
                {"variant": "raw", "scope": "samples"},
                {"variant": "raw", "scope": "full"},
            ],
        },
    )
    # Download endpoint NOT mocked — if the CLI reaches it, respx raises
    # and we know the pre-flight check failed.

    result = cli_runner.invoke(
        cli, ["datasets", "download", "granted-ego-ds"]
    )
    assert result.exit_code == 2, result.output
    out = result.output + (result.stderr or "")
    assert "--variant is required" in out
    assert "raw" in out
    # The raw Pydantic-422 leak must NOT appear.
    assert "Pydantic" not in out
    assert "'type': 'missing'" not in out


def test_ego_command_removed(cli_runner, tmp_home):
    """`verlet ego …` was fully removed in 0.9.0 (the migration-hint stub is
    gone too) — the bare name now resolves to Click's unknown-command error.
    """
    from verlet.cli import cli

    result = cli_runner.invoke(cli, ["ego", "list"])
    assert result.exit_code == 2
    assert "No such command" in result.output


def test_download_showcase_reuses_cached_manifest_on_rerun(
    cli_runner, respx_mock, tmp_home, tmp_path
):
    """A second run against the same output dir must not re-fetch the manifest
    while its URLs are valid: every fetch consumes grant quota and, on a
    billable grant, serves fewer episodes the second time."""
    from datetime import datetime, timedelta, timezone
    from unittest.mock import patch

    from verlet.cli import cli
    from verlet.download import DownloadResult

    _seed_showcase_profile(tmp_home)
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/teleop-ds"
    ).respond(
        200,
        json={
            "id": "teleop-ds", "slug": "teleop-ds", "title": "Teleop DS",
            "modality": "teleop", "task_type": "fold", "robot_embodiment": "yam",
            "episode_count": 1, "total_hours": 0.1, "effective_grants": [],
        },
    )
    manifest_route = respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/teleop-ds/download"
    ).respond(
        200,
        json={
            "dataset_title": "Teleop DS", "dataset_slug": "teleop-ds",
            "format": "lerobot-v2", "modality": "teleop", "variant": "processed",
            "scope": "full",
            "episodes": [{
                "episode_index": 0,
                "parquet_url": "https://signed.example/ep0.parquet",
                "video_urls": [{"camera": "cam_high", "url": "https://signed.example/h.mp4"}],
                "meta_urls": [],
            }],
            "segments": [],
            "meta": {"info": {"fps": 30}, "task": "fold"},
            "urls_expire_at": (datetime.now(timezone.utc) + timedelta(days=6)).isoformat(),
        },
    )

    async def fake_download_resolved(items, parallel, skip_existing):  # noqa: ARG001
        return DownloadResult(downloaded=0, skipped=len(items), failed=0)

    args = ["datasets", "download", "teleop-ds", "-o", str(tmp_path), "-y"]
    with (
        patch("verlet.datasets.commands.download_resolved", side_effect=fake_download_resolved),
        patch("verlet.datasets.commands.check_license_accepted", return_value=True),
    ):
        first = cli_runner.invoke(cli, args)
        second = cli_runner.invoke(cli, args)

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert manifest_route.call_count == 1
    assert "Reusing cached download manifest" in second.output
    assert (tmp_path / ".teleop-ds.verlet-manifest.json").exists()
