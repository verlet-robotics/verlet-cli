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
    """Showcase `datasets download` routes to the gated download endpoint."""
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
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
        cli, ["datasets", "download", "granted-ego-ds", "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert (
        "/api/v1/showcase/datasets/granted-ego-ds/download" in _paths(respx_mock)
    )


def test_ego_command_removed(cli_runner, tmp_home):
    """`verlet ego …` was fully removed in 0.9.0 (the migration-hint stub is
    gone too) — the bare name now resolves to Click's unknown-command error.
    """
    from verlet.cli import cli

    result = cli_runner.invoke(cli, ["ego", "list"])
    assert result.exit_code == 2
    assert "No such command" in result.output
