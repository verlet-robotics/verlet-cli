"""Plan 30-09 Task 1 — `verlet bundles download <id>` (CLIBUNDLE-05).

Seven behavior tests:

  * test_download_variant_raw_zero_network_call_exits_2 — Click parse-time
    rejection of ``--variant raw`` with verbatim D-BUNDLE3 error string and
    ZERO HTTP calls (asserted via empty respx route registry).
  * test_download_fans_out_per_dataset_manifests — bundle detail fetch +
    per-dataset arm-manifest fetches + download to ``<out>/<slug>/...``.
  * test_download_format_applied_to_all_datasets — ``--format hdf5`` adds
    ``?format=hdf5`` to every per-dataset manifest call.
  * test_download_400_on_one_dataset_fails_fast_no_partial_writes — D-BUNDLE3
    fail-fast: any 400 from one dataset's manifest call aborts the whole
    bundle download with NO files written for any dataset (verified by
    asserting only the first dataset's manifest was attempted before exit).
  * test_download_writes_bundle_manifest_json_at_root — ``bundle_manifest.json``
    is written at ``<out>/bundle_manifest.json`` summarizing slugs + format
    + total size (D-BUNDLE4).
  * test_download_custom_out_dir_overrides_default — ``--out custom-dir``
    overrides default ``./<bundle_id>/`` location.
  * test_download_disk_layout_per_dataset_subdir — per-dataset subdir uses
    the dataset's slug (D-BUNDLE4).
"""
from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import pytest

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli


BUNDLE_DETAIL_PATH_FMT = "/api/platform/v1/bundles/{bundle_id}"
ARM_MANIFEST_PATH_FMT = "/api/platform/v1/downloads/{slug}/manifest"

FAR_FUTURE = "2099-01-01T00:00:00+00:00"


def _seed_default_profile() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at=FAR_FUTURE,
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _bundle_detail(
    bundle_id: str = "stanford-egocentric-2024",
    *,
    datasets: list[dict] | None = None,
) -> dict:
    if datasets is None:
        datasets = [
            {
                "slug": "kitchen-pickplace",
                "name": "Kitchen pick-place",
                "episode_count": 50,
                "available_formats": ["lerobot-v2", "hdf5"],
                "size_bytes": 2_500_000_000,
            },
            {
                "slug": "tabletop-stack",
                "name": "Tabletop stacking",
                "episode_count": 30,
                "available_formats": ["lerobot-v2", "hdf5"],
                "size_bytes": 1_200_000_000,
            },
        ]
    return {
        "bundle_id": bundle_id,
        "bundle_slug": "stanford-egocentric-2024",
        "bundle_name": "Stanford Egocentric 2024",
        "kind": "research",
        "expires_at": FAR_FUTURE,
        "license": "CC-BY-4.0",
        "citation": "Lee et al., 2024",
        "datasets": datasets,
    }


def _arm_manifest_200(slug: str) -> dict:
    """Sample DownloadManifest body (200 response). Files list is empty so
    the test never has to mock an actual chunked download."""
    return {
        "dataset_id": "00000000-0000-0000-0000-000000000001",
        "dataset_slug": slug,
        "format": "lerobot-v2",
        "files": [],
    }


@pytest.fixture(autouse=True)
def _no_op_download():
    """Stub ``download_resolved`` so tests never spin up real httpx streams.

    Returns a minimal DownloadResult-like object so the bundles command's
    bookkeeping path is exercised without hitting disk for chunked bytes.
    """
    from verlet.download import DownloadResult

    async def _fake_download(items, parallel=8, skip_existing=True):
        # Touch each local_path so the bundle_manifest.json check that the
        # file landed somewhere is exercisable from the bundles command.
        for it in items:
            it.local_path.parent.mkdir(parents=True, exist_ok=True)
            it.local_path.write_bytes(b"")
        return DownloadResult(downloaded=len(items), skipped=0, failed=0)

    with patch("verlet.bundles.commands.download_resolved", _fake_download):
        yield


# ---------------------------------------------------------------------------
# Test 1: --variant raw zero-network rejection (D-BUNDLE3)
# ---------------------------------------------------------------------------


def test_download_variant_raw_zero_network_call_exits_2(
    tmp_home, cli_runner, respx_mock,
):
    """`--variant raw` exits 2 with verbatim D-BUNDLE3 error + NO HTTP calls."""
    _seed_default_profile()

    # Register a route that would catch any unintended HTTP call. If the
    # validator works correctly, this route is never called.
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/bundles/any-bundle"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail()))

    result = cli_runner.invoke(
        cli, ["bundles", "download", "any-bundle", "--variant", "raw"]
    )
    assert result.exit_code == 2, (result.exit_code, result.output, result.stderr)
    assert (
        "bundles are processed-only; --variant raw is not allowed"
        in (result.stderr or "")
    ), result.stderr
    # Critical: ZERO network calls before the bail-out.
    assert len(respx_mock.calls) == 0, (
        f"--variant raw must reject pre-network; got "
        f"{len(respx_mock.calls)} HTTP calls"
    )


# ---------------------------------------------------------------------------
# Test 2: fan-out + per-dataset disk layout
# ---------------------------------------------------------------------------


def test_download_fans_out_per_dataset_manifests(
    tmp_home, cli_runner, respx_mock,
):
    """Bundle detail GET + per-dataset manifest GETs + per-slug subdirs."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))

    kitchen_route = respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(200, json=_arm_manifest_200("kitchen-pickplace"))
    )
    tabletop_route = respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='tabletop-stack')}"
    ).mock(
        return_value=httpx.Response(200, json=_arm_manifest_200("tabletop-stack"))
    )

    out_dir = tmp_home / "bundle-out"
    result = cli_runner.invoke(
        cli, ["bundles", "download", bundle_id, "-o", str(out_dir)]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert kitchen_route.called, "kitchen-pickplace manifest not fetched"
    assert tabletop_route.called, "tabletop-stack manifest not fetched"

    # D-BUNDLE4 disk layout: per-dataset subdirs at the bundle root.
    assert (out_dir / "kitchen-pickplace").is_dir(), list(out_dir.iterdir())
    assert (out_dir / "tabletop-stack").is_dir(), list(out_dir.iterdir())


# ---------------------------------------------------------------------------
# Test 3: --format applied to all bundle datasets
# ---------------------------------------------------------------------------


def test_download_format_applied_to_all_datasets(
    tmp_home, cli_runner, respx_mock,
):
    """`--format hdf5` adds `?format=hdf5` to every per-dataset manifest call."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))

    kitchen_route = respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(
            200, json={**_arm_manifest_200("kitchen-pickplace"), "format": "hdf5"}
        )
    )
    tabletop_route = respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='tabletop-stack')}"
    ).mock(
        return_value=httpx.Response(
            200, json={**_arm_manifest_200("tabletop-stack"), "format": "hdf5"}
        )
    )

    out_dir = tmp_home / "bundle-fmt"
    result = cli_runner.invoke(
        cli,
        ["bundles", "download", bundle_id, "--format", "hdf5", "-o", str(out_dir)],
    )
    assert result.exit_code == 0, (result.output, result.stderr)

    # Every per-dataset manifest call carries ?format=hdf5.
    for route in (kitchen_route, tabletop_route):
        assert route.called
        qs = dict(route.calls.last.request.url.params)
        assert qs.get("format") == "hdf5", (
            f"expected ?format=hdf5 on {route.pattern}, got params={qs}"
        )


# ---------------------------------------------------------------------------
# Test 4: 400 on one dataset → fail-fast, no partial writes
# ---------------------------------------------------------------------------


def test_download_400_on_one_dataset_fails_fast_no_partial_writes(
    tmp_home, cli_runner, respx_mock,
):
    """First dataset returns 400 → bundle download exits 1 with detail; the
    second dataset's manifest is never fetched (D-BUNDLE3 fail-fast)."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))

    kitchen_route = respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(
            400,
            json={"detail": "format hdf5 not supported for raw-only dataset kitchen-pickplace"},
        )
    )
    tabletop_route = respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='tabletop-stack')}"
    ).mock(
        return_value=httpx.Response(200, json=_arm_manifest_200("tabletop-stack"))
    )

    out_dir = tmp_home / "bundle-fail-fast"
    result = cli_runner.invoke(
        cli,
        ["bundles", "download", bundle_id, "--format", "hdf5", "-o", str(out_dir)],
    )
    assert result.exit_code != 0, (result.output, result.stderr)
    # The first dataset's manifest WAS attempted...
    assert kitchen_route.called
    # ...but the second's WAS NOT — fail-fast aborts before fanning further.
    assert not tabletop_route.called, (
        "fail-fast violated: tabletop-stack manifest fetched after "
        "kitchen-pickplace 400"
    )
    # Stderr surfaces the failing dataset slug + server detail verbatim.
    assert "kitchen-pickplace" in (result.stderr or ""), result.stderr
    assert (
        "format hdf5 not supported for raw-only dataset kitchen-pickplace"
        in (result.stderr or "")
    ), result.stderr
    # No bundle_manifest.json written when the run aborts (no partial writes).
    assert not (out_dir / "bundle_manifest.json").exists()


# ---------------------------------------------------------------------------
# Test 5: bundle_manifest.json at root summarizing slugs + format + total size
# ---------------------------------------------------------------------------


def test_download_writes_bundle_manifest_json_at_root(
    tmp_home, cli_runner, respx_mock,
):
    """`<out>/bundle_manifest.json` summarizes the bundle download (D-BUNDLE4)."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(200, json=_arm_manifest_200("kitchen-pickplace"))
    )
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='tabletop-stack')}"
    ).mock(
        return_value=httpx.Response(200, json=_arm_manifest_200("tabletop-stack"))
    )

    out_dir = tmp_home / "bundle-root-json"
    result = cli_runner.invoke(
        cli, ["bundles", "download", bundle_id, "-o", str(out_dir)]
    )
    assert result.exit_code == 0, (result.output, result.stderr)

    summary_path = out_dir / "bundle_manifest.json"
    assert summary_path.exists(), list(out_dir.iterdir())

    parsed = json.loads(summary_path.read_text())
    assert parsed["bundle_id"] == bundle_id
    assert parsed["bundle_slug"] == "stanford-egocentric-2024"
    # `format` is None when no --format flag is supplied (native).
    assert parsed.get("format") is None
    slugs = [d["slug"] for d in parsed["datasets"]]
    assert sorted(slugs) == sorted(["kitchen-pickplace", "tabletop-stack"])


# ---------------------------------------------------------------------------
# Test 6: --out overrides default ./<bundle_id>/
# ---------------------------------------------------------------------------


def test_download_custom_out_dir_overrides_default(
    tmp_home, cli_runner, respx_mock,
):
    """`-o custom-dir` writes to the provided path, not ./<bundle_id>/."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_bundle_detail(
                bundle_id,
                datasets=[
                    {
                        "slug": "single-ds",
                        "name": "Solo dataset",
                        "episode_count": 10,
                        "available_formats": ["lerobot-v2"],
                        "size_bytes": 1_000_000,
                    }
                ],
            ),
        )
    )
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='single-ds')}"
    ).mock(return_value=httpx.Response(200, json=_arm_manifest_200("single-ds")))

    custom = tmp_home / "my-custom-out"
    result = cli_runner.invoke(
        cli, ["bundles", "download", bundle_id, "-o", str(custom)]
    )
    assert result.exit_code == 0, (result.output, result.stderr)

    assert custom.is_dir()
    assert (custom / "bundle_manifest.json").exists()
    assert (custom / "single-ds").is_dir()


# ---------------------------------------------------------------------------
# Test 7: per-dataset slug subdir in the disk layout (D-BUNDLE4 spec)
# ---------------------------------------------------------------------------


def test_download_disk_layout_per_dataset_subdir(
    tmp_home, cli_runner, respx_mock,
):
    """`<out>/<dataset_slug>/...` is the D-BUNDLE4 disk layout."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(200, json=_arm_manifest_200("kitchen-pickplace"))
    )
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='tabletop-stack')}"
    ).mock(
        return_value=httpx.Response(200, json=_arm_manifest_200("tabletop-stack"))
    )

    out_dir = tmp_home / "bundle-layout"
    result = cli_runner.invoke(
        cli, ["bundles", "download", bundle_id, "-o", str(out_dir)]
    )
    assert result.exit_code == 0, (result.output, result.stderr)

    # D-BUNDLE4: each dataset slug becomes a sibling subdir at the bundle
    # root, never nested under one another.
    children = sorted(p.name for p in out_dir.iterdir() if p.is_dir())
    assert children == ["kitchen-pickplace", "tabletop-stack"], children
