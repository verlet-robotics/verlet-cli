"""CLIDATA-06: verlet datasets download. Real assertions over respx mocks.

The download command does the following over the wire:
  1. Fetch /catalog/datasets/{slug} (modality discriminator).
  2. Fetch the modality-correct manifest endpoint.
  3. Hand the signed-URL items to ``download_resolved``.

Tests stub steps 1+2 with respx and (where the actual byte download matters)
monkeypatch ``download_resolved`` so we don't need a real R2 server. Pre-flight
gates (modality flag matrix, auth, --format hint) short-circuit BEFORE the
manifest fetch — those tests assert that the manifest endpoint was never
called.
"""
from __future__ import annotations

from unittest.mock import patch

from verlet.auth.credentials import upsert_profile
from verlet.download import DownloadResult


def _seed_default_profile(_tmp_home, *, token: str = "t0k3n") -> None:
    """Write a credentials.json profile under the test's isolated HOME."""
    upsert_profile(
        "default",
        kind="device_flow",
        access_token=token,
        api_url="https://api.verlet.co",
    )


def _arm_detail(slug: str = "pick-and-place-yam-v3") -> dict:
    """Arm row marker per ``is_ego_row``: ``data_tiers`` lacks ``raw``."""
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "slug": slug,
        "title": "Pick and Place YAM v3",
        "data_tiers": ["processed"],
        "available_variants": ["processed"],
    }


def _ego_detail(slug: str = "kitchen-cooking-aria-spring-2026") -> dict:
    """Ego row marker per ``is_ego_row``: ``data_tiers`` contains ``raw``."""
    return {
        "id": "00000000-0000-0000-0000-000000000002",
        "slug": slug,
        "title": "Kitchen Cooking Aria",
        "data_tiers": ["raw", "processed"],
        "available_variants": ["raw", "processed"],
    }


def _manifest_paths(respx_mock) -> list[str]:
    """All URL paths hit so far — used to assert which endpoints fired."""
    return [c.request.url.path for c in respx_mock.calls]


def test_arm_dispatch(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """Arm catalog row → `/downloads/{slug}/manifest`; ego endpoint NOT hit."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=_arm_detail("pick-and-place-yam-v3"))
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pick-and-place-yam-v3/manifest",
    ).respond(200, json=mock_arm_manifest_response)

    result = cli_runner.invoke(
        cli, ["datasets", "download", "pick-and-place-yam-v3", "--dry-run"],
    )
    assert result.exit_code == 0, result.output

    paths = _manifest_paths(respx_mock)
    assert (
        "/api/platform/v1/downloads/pick-and-place-yam-v3/manifest" in paths
    ), paths
    assert not any("/downloads/ego/" in p for p in paths), paths


def test_ego_dispatch(
    cli_runner, respx_mock, tmp_home, mock_ego_manifest_response,
):
    """Ego catalog row → `/downloads/ego/datasets/{slug}/manifest?variant=…`."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    slug = "kitchen-cooking-aria-spring-2026"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_ego_detail(slug))
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/ego/datasets/{slug}/manifest",
    ).respond(200, json=mock_ego_manifest_response)

    result = cli_runner.invoke(
        cli,
        ["datasets", "download", slug, "--variant", "raw", "--dry-run"],
    )
    assert result.exit_code == 0, result.output

    # Verify the ego variant manifest endpoint was hit + the arm manifest path
    # was NOT touched.
    paths = _manifest_paths(respx_mock)
    assert (
        f"/api/platform/v1/downloads/ego/datasets/{slug}/manifest" in paths
    ), paths
    assert f"/api/platform/v1/downloads/{slug}/manifest" not in paths, paths

    # Verify the variant query param made it to the request.
    manifest_calls = [
        c for c in respx_mock.calls
        if c.request.url.path.startswith(
            f"/api/platform/v1/downloads/ego/datasets/{slug}/manifest"
        )
    ]
    assert manifest_calls, "ego manifest endpoint should have fired"
    assert manifest_calls[-1].request.url.params.get("variant") == "raw"


def test_variant_rejected_on_arm(cli_runner, respx_mock, tmp_home):
    """`--variant` on an arm row → pre-flight error (D-MOD2). Manifest NOT hit."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=_arm_detail("pick-and-place-yam-v3"))

    result = cli_runner.invoke(
        cli,
        [
            "datasets",
            "download",
            "pick-and-place-yam-v3",
            "--variant",
            "raw",
            "--dry-run",
        ],
    )
    # click.UsageError → exit 2.
    assert result.exit_code == 2, result.output
    assert "--variant is ego-only" in result.output, result.output

    paths = _manifest_paths(respx_mock)
    # Detail endpoint is allowed (modality detection); manifest must NOT fire.
    assert all("/manifest" not in p for p in paths), paths


def test_variant_required_on_ego(cli_runner, respx_mock, tmp_home):
    """Ego row without `--variant` → pre-flight error (D-MOD2). Manifest NOT hit."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    slug = "kitchen-cooking-aria-spring-2026"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_ego_detail(slug))

    result = cli_runner.invoke(
        cli, ["datasets", "download", slug, "--dry-run"],
    )
    assert result.exit_code == 2, result.output
    assert "--variant is required for ego" in result.output, result.output

    paths = _manifest_paths(respx_mock)
    assert all("/manifest" not in p for p in paths), paths


def test_non_native_format_phase_30_hint(cli_runner, respx_mock, tmp_home):
    """`--format hdf5` (non-native) → Phase-30 hint + clean exit (status 0).

    CONTEXT.md Discretion §"--format flag in v1": exit cleanly (status 0)
    rather than as a flag-misuse error. The download command short-circuits
    before any HTTP fires — neither the catalog detail nor the manifest is
    touched, since the format check happens BEFORE modality resolution.
    """
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    # No respx route registered for the catalog detail endpoint — if the
    # command fires it, respx will raise "no matching route" and the test
    # fails. Asserting "no calls" below is the explicit contract.
    result = cli_runner.invoke(
        cli,
        [
            "datasets",
            "download",
            "pick-and-place-yam-v3",
            "--format",
            "hdf5",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Phase 30 conversion engine" in result.output, result.output

    # Zero HTTP calls — pre-flight gate fires before modality detection.
    assert len(respx_mock.calls) == 0, (
        f"Phase 30 hint must short-circuit before any HTTP, "
        f"got {len(respx_mock.calls)} calls"
    )


def test_resume_skips_existing(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """`--resume` flag → ``download_resolved`` invoked with skip_existing=True."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=_arm_detail("pick-and-place-yam-v3"))
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pick-and-place-yam-v3/manifest",
    ).respond(200, json=mock_arm_manifest_response)

    fake_result = DownloadResult(downloaded=0, skipped=3, failed=0)

    async def fake_download_resolved(items, parallel, skip_existing):  # noqa: ARG001
        fake_download_resolved.last_skip_existing = skip_existing
        return fake_result

    fake_download_resolved.last_skip_existing = None

    with patch(
        "verlet.datasets.commands.download_resolved",
        side_effect=fake_download_resolved,
    ):
        with patch(
            "verlet.datasets.commands.check_license_accepted",
            return_value=True,
        ):
            result = cli_runner.invoke(
                cli,
                ["datasets", "download", "pick-and-place-yam-v3", "--resume"],
            )

    assert result.exit_code == 0, result.output
    assert fake_download_resolved.last_skip_existing is True


def test_force_overrides_resume(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """`--force` → ``download_resolved`` invoked with skip_existing=False."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=_arm_detail("pick-and-place-yam-v3"))
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pick-and-place-yam-v3/manifest",
    ).respond(200, json=mock_arm_manifest_response)

    fake_result = DownloadResult(downloaded=3, skipped=0, failed=0)

    async def fake_download_resolved(items, parallel, skip_existing):  # noqa: ARG001
        fake_download_resolved.last_skip_existing = skip_existing
        return fake_result

    fake_download_resolved.last_skip_existing = None

    with patch(
        "verlet.datasets.commands.download_resolved",
        side_effect=fake_download_resolved,
    ):
        with patch(
            "verlet.datasets.commands.check_license_accepted",
            return_value=True,
        ):
            with patch(
                "verlet.datasets.commands.write_license_file",
            ):
                result = cli_runner.invoke(
                    cli,
                    [
                        "datasets",
                        "download",
                        "pick-and-place-yam-v3",
                        "--force",
                    ],
                )

    assert result.exit_code == 0, result.output
    assert fake_download_resolved.last_skip_existing is False


def test_dry_run_no_writes(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """`--dry-run` → ``download_resolved`` NOT called; output lists planned files."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=_arm_detail("pick-and-place-yam-v3"))
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pick-and-place-yam-v3/manifest",
    ).respond(200, json=mock_arm_manifest_response)

    with patch(
        "verlet.datasets.commands.download_resolved",
    ) as mock_dl:
        result = cli_runner.invoke(
            cli,
            ["datasets", "download", "pick-and-place-yam-v3", "--dry-run"],
        )

    assert result.exit_code == 0, result.output
    assert mock_dl.call_count == 0, (
        "download_resolved must not be called on --dry-run"
    )
    assert "Would download" in result.output, result.output


def test_partial_failure_exits_nonzero(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """Any failed file → SystemExit(1) (ROADMAP §29 SC3)."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=_arm_detail("pick-and-place-yam-v3"))
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pick-and-place-yam-v3/manifest",
    ).respond(200, json=mock_arm_manifest_response)

    fake_result = DownloadResult(downloaded=2, skipped=0, failed=1)

    async def fake_download_resolved(items, parallel, skip_existing):  # noqa: ARG001
        return fake_result

    with patch(
        "verlet.datasets.commands.download_resolved",
        side_effect=fake_download_resolved,
    ):
        with patch(
            "verlet.datasets.commands.check_license_accepted",
            return_value=True,
        ):
            with patch("verlet.datasets.commands.write_license_file"):
                result = cli_runner.invoke(
                    cli,
                    ["datasets", "download", "pick-and-place-yam-v3"],
                )

    assert result.exit_code == 1, result.output
    assert "1 failed" in result.output, result.output


def test_unauthenticated_early_exit(cli_runner, respx_mock, tmp_home):
    """No active profile → fail-fast pre-flight (D-MOD4); zero HTTP calls."""
    from verlet.cli import cli

    # tmp_home is empty → no credentials.json → require_profile raises.
    result = cli_runner.invoke(
        cli, ["datasets", "download", "any-slug"],
    )

    # ClickException → exit 1.
    assert result.exit_code == 1, result.output
    assert "Not authenticated" in result.output, result.output
    assert "verlet auth login" in result.output, result.output

    # No HTTP fired — pre-flight gate before any network work.
    assert len(respx_mock.calls) == 0, (
        f"unauthenticated exit must short-circuit before any HTTP, "
        f"got {len(respx_mock.calls)} calls"
    )


def test_format_lerobot_v2_happy_path(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """`--format lerobot-v2` (native) → manifest fetched with `?format=lerobot-v2`."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=_arm_detail("pick-and-place-yam-v3"))
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pick-and-place-yam-v3/manifest",
    ).respond(200, json=mock_arm_manifest_response)

    fake_result = DownloadResult(downloaded=3, skipped=0, failed=0)

    async def fake_download_resolved(items, parallel, skip_existing):  # noqa: ARG001
        return fake_result

    with patch(
        "verlet.datasets.commands.download_resolved",
        side_effect=fake_download_resolved,
    ):
        with patch(
            "verlet.datasets.commands.check_license_accepted",
            return_value=True,
        ):
            with patch("verlet.datasets.commands.write_license_file"):
                result = cli_runner.invoke(
                    cli,
                    [
                        "datasets",
                        "download",
                        "pick-and-place-yam-v3",
                        "--format",
                        "lerobot-v2",
                    ],
                )

    assert result.exit_code == 0, result.output

    # Verify the manifest request had ?format=lerobot-v2.
    manifest_calls = [
        c for c in respx_mock.calls
        if c.request.url.path
        == "/api/platform/v1/downloads/pick-and-place-yam-v3/manifest"
    ]
    assert manifest_calls, "manifest endpoint should have fired"
    assert manifest_calls[-1].request.url.params.get("format") == "lerobot-v2"
