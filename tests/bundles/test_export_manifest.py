"""Plan 30-09 Task 2 — `verlet bundles export-manifest <id>` (CLIBUNDLE-06).

Five behavior tests:

  * test_export_writes_portable_json — JSON contains bundle_id, bundle_slug,
    expires_at, datasets[*].files[*].{path,url,size_bytes,checksum_sha256}.
  * test_export_default_path_uses_bundle_id — without --out, writes to
    ./<bundle_id>-manifest.json.
  * test_export_format_query_param_threaded_through — `--format hdf5` adds
    ?format=hdf5 to every per-dataset manifest call AND records the format
    at the top level of the output JSON.
  * test_export_404_unknown_bundle_exits_1 — bundle 404 routes through
    fetch_bundle_detail's BUNDLE_NOT_FOUND_MSG verbatim.
  * test_export_exported_at_iso_parseable — exported_at field round-trips
    through datetime.fromisoformat (catches accidental non-ISO formatting).
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

from tests.conftest import combined_output


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


def _bundle_detail(bundle_id: str = "stanford-egocentric-2024") -> dict:
    return {
        "bundle_id": bundle_id,
        "bundle_slug": "stanford-egocentric-2024",
        "bundle_name": "Stanford Egocentric 2024",
        "kind": "research",
        "expires_at": FAR_FUTURE,
        "license": "CC-BY-4.0",
        "citation": "Lee et al., 2024",
        "datasets": [
            {
                "slug": "kitchen-pickplace",
                "name": "Kitchen pick-place",
                "episode_count": 50,
                "available_formats": ["lerobot-v2", "hdf5"],
                "size_bytes": 2_500_000_000,
            }
        ],
    }


def _arm_manifest_with_files(slug: str, *, fmt: str = "lerobot-v2") -> dict:
    """Sample manifest body with one file row carrying url+checksum+size."""
    return {
        "dataset_id": "00000000-0000-0000-0000-000000000001",
        "dataset_slug": slug,
        "format": fmt,
        "files": [
            {
                "path": "data/chunk-000/episode_000000.parquet",
                "url": "https://r2.signed.example/data/episode_000000.parquet?sig=abc",
                "size_bytes": 1_048_576,
                "checksum_sha256": "0" * 64,
            }
        ],
    }


# ---------------------------------------------------------------------------
# Test 1: portable JSON shape
# ---------------------------------------------------------------------------


def test_export_writes_portable_json(tmp_home, cli_runner, respx_mock):
    """JSON output carries bundle_id, slug, expires_at, per-dataset files."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(
            200, json=_arm_manifest_with_files("kitchen-pickplace")
        )
    )

    out_path = tmp_home / "manifest.json"
    result = cli_runner.invoke(
        cli, ["bundles", "export-manifest", bundle_id, "--out", str(out_path)]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert out_path.exists()

    parsed = json.loads(out_path.read_text())
    assert parsed["bundle_id"] == bundle_id
    assert parsed["bundle_slug"] == "stanford-egocentric-2024"
    assert parsed["expires_at"] == FAR_FUTURE
    assert isinstance(parsed["datasets"], list)
    assert len(parsed["datasets"]) == 1

    ds = parsed["datasets"][0]
    assert ds["slug"] == "kitchen-pickplace"
    files = ds["files"]
    assert len(files) == 1
    f = files[0]
    # Per-file portable manifest shape: path + url + size + checksum.
    assert f["path"] == "data/chunk-000/episode_000000.parquet"
    assert f["url"].startswith("https://r2.signed.example/")
    assert f["size_bytes"] == 1_048_576
    assert f["checksum_sha256"] == "0" * 64


# ---------------------------------------------------------------------------
# Test 2: default --out path uses ./<bundle_id>-manifest.json
# ---------------------------------------------------------------------------


def test_export_default_path_uses_bundle_id(
    tmp_home, cli_runner, respx_mock, monkeypatch,
):
    """Without --out, writes to ./<bundle_id>-manifest.json in cwd."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(
            200, json=_arm_manifest_with_files("kitchen-pickplace")
        )
    )

    # Ensure the relative ./<bundle_id>-manifest.json lands inside tmp_home,
    # not somewhere on the developer's machine.
    monkeypatch.chdir(tmp_home)

    result = cli_runner.invoke(cli, ["bundles", "export-manifest", bundle_id])
    assert result.exit_code == 0, (result.output, result.stderr)

    expected = Path(f"./{bundle_id}-manifest.json")
    assert expected.exists(), list(tmp_home.iterdir())


# ---------------------------------------------------------------------------
# Test 3: --format threads through to per-dataset manifest + JSON top level
# ---------------------------------------------------------------------------


def test_export_format_query_param_threaded_through(
    tmp_home, cli_runner, respx_mock,
):
    """`--format hdf5` flows to per-dataset manifest call AND output JSON."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))
    manifest_route = respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(
            200,
            json=_arm_manifest_with_files("kitchen-pickplace", fmt="hdf5"),
        )
    )

    out_path = tmp_home / "manifest-hdf5.json"
    result = cli_runner.invoke(
        cli,
        [
            "bundles", "export-manifest", bundle_id,
            "--format", "hdf5", "--out", str(out_path),
        ],
    )
    assert result.exit_code == 0, (result.output, result.stderr)

    # The per-dataset manifest call carries ?format=hdf5.
    qs = dict(manifest_route.calls.last.request.url.params)
    assert qs.get("format") == "hdf5", qs

    parsed = json.loads(out_path.read_text())
    assert parsed["format"] == "hdf5"


# ---------------------------------------------------------------------------
# Test 4: 404 unknown bundle exits 1 with verbatim "bundle not found"
# ---------------------------------------------------------------------------


def test_export_404_unknown_bundle_exits_1(
    tmp_home, cli_runner, respx_mock,
):
    """Bundle 404 propagates through fetch_bundle_detail's stderr path."""
    _seed_default_profile()
    bundle_id = "definitely-not-a-real-bundle"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(404, json={"detail": "Not Found"}))

    out_path = tmp_home / "would-not-write.json"
    result = cli_runner.invoke(
        cli, ["bundles", "export-manifest", bundle_id, "--out", str(out_path)]
    )
    assert result.exit_code != 0, (result.output, result.stderr)
    assert "bundle not found" in combined_output(result)
    # Nothing written on the failure path.
    assert not out_path.exists()


# ---------------------------------------------------------------------------
# Test 5: exported_at is parseable ISO-8601
# ---------------------------------------------------------------------------


def test_export_exported_at_iso_parseable(
    tmp_home, cli_runner, respx_mock,
):
    """`exported_at` round-trips through datetime.fromisoformat()."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_bundle_detail(bundle_id)))
    respx_mock.get(
        f"https://api.verlet.co{ARM_MANIFEST_PATH_FMT.format(slug='kitchen-pickplace')}"
    ).mock(
        return_value=httpx.Response(
            200, json=_arm_manifest_with_files("kitchen-pickplace")
        )
    )

    out_path = tmp_home / "iso-check.json"
    result = cli_runner.invoke(
        cli, ["bundles", "export-manifest", bundle_id, "--out", str(out_path)]
    )
    assert result.exit_code == 0, (result.output, result.stderr)

    parsed = json.loads(out_path.read_text())
    exported_at = parsed.get("exported_at")
    assert exported_at, parsed

    # ISO-8601 round-trip — accept Z or +00:00 timezone suffix.
    cleaned = exported_at.replace("Z", "+00:00")
    parsed_dt = datetime.fromisoformat(cleaned)
    # The exported_at timestamp should be reasonable -- not in the far future
    # or the distant past.
    assert parsed_dt.year >= 2024, parsed_dt
