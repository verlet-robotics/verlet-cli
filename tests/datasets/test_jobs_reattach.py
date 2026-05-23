"""Plan 30-06 Task 2 — `verlet datasets jobs <job_id>` reattach + listing-deferred.

Five behavior tests for CLIDATA-07 SC4 (single-job reattach):

  * test_jobs_reattach_processing_then_completed — poll loop drives to completion
  * test_jobs_reattach_already_completed_downloads_immediately — short-circuit
  * test_jobs_reattach_failed_prints_error_and_exits_nonzero — D-FORMAT3
  * test_jobs_no_id_lists_account_jobs / _slug_lists_dataset_conversions /
    _no_id_empty_account — the G-P5 conversion-job listing
  * test_jobs_reattach_unknown_id_404 — 404 → "job not found" + exit 1

The "no id" test asserts ZERO HTTP calls — the listing endpoint isn't
implemented server-side (verified during planning), so the bare invocation
prints a deferred-feature notice and exits 0 without any network work.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import httpx

from verlet.auth.credentials import upsert_profile


def _seed_default_profile() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


async def _async_noop_sleep(_secs):
    return None


def _stub_arm_manifest_files() -> list[dict]:
    """Single-file manifest body — kept tiny so download_resolved is fast."""
    return [
        {
            "path": "meta/info.json",
            "url": "https://r2.verlet.co/signed/info.json?token=ghi",
            "size_bytes": 2_048,
        },
    ]


def _stub_arm_manifest(slug: str = "pick-and-place-yam-v3") -> dict:
    return {
        "dataset_id": "00000000-0000-0000-0000-000000000001",
        "dataset_slug": slug,
        "format": "hdf5",
        "files": _stub_arm_manifest_files(),
    }


def test_jobs_reattach_processing_then_completed(
    cli_runner, respx_mock, tmp_home, monkeypatch,
):
    """`verlet datasets jobs <job_id>` — server returns processing then
    completed; CLI runs the poll loop, downloads the manifest, exits 0."""
    from verlet.cli import cli

    _seed_default_profile()
    job_id = "job-reattach-1"
    manifest = _stub_arm_manifest()

    # Stub the R2 download so download_resolved doesn't hit the real network.
    respx_mock.get(
        "https://r2.verlet.co/signed/info.json?token=ghi"
    ).respond(200, content=b"{}")

    # First poll = processing, second = completed.
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/jobs/{job_id}",
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "job_id": job_id,
                    "status": "processing",
                    "progress": {"current_episode": 1, "total_episodes": 3},
                    "manifest": None,
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
            httpx.Response(
                200,
                json={
                    "job_id": job_id,
                    "status": "completed",
                    "progress": None,
                    "manifest": manifest,
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
        ]
    )

    # Output to a tmp directory we can write to.
    out = tmp_home / "out"
    monkeypatch.chdir(tmp_home)

    with patch(
        "verlet.datasets.convert.asyncio.sleep",
        new=_async_noop_sleep,
    ):
        result = cli_runner.invoke(
            cli,
            [
                "datasets",
                "jobs",
                job_id,
                "--quiet",
                "-o",
                str(out),
            ],
        )

    assert result.exit_code == 0, result.output

    # Job poll endpoint hit ≥2 times (processing → completed).
    poll_calls = [
        c for c in respx_mock.calls
        if c.request.url.path
        == f"/api/platform/v1/downloads/jobs/{job_id}"
    ]
    assert len(poll_calls) >= 2, f"expected ≥2 polls, got {len(poll_calls)}"


def test_jobs_reattach_already_completed_downloads_immediately(
    cli_runner, respx_mock, tmp_home, monkeypatch,
):
    """`verlet datasets jobs <job_id>` against an already-completed job — the
    CLI downloads the inlined manifest immediately (no further polling)."""
    from verlet.cli import cli

    _seed_default_profile()
    job_id = "job-reattach-done"
    manifest = _stub_arm_manifest()

    respx_mock.get(
        "https://r2.verlet.co/signed/info.json?token=ghi"
    ).respond(200, content=b"{}")

    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/jobs/{job_id}",
    ).respond(
        200,
        json={
            "job_id": job_id,
            "status": "completed",
            "progress": None,
            "manifest": manifest,
            "error_message": None,
            "failed_stage": None,
        },
    )

    out = tmp_home / "out"
    monkeypatch.chdir(tmp_home)

    result = cli_runner.invoke(
        cli, ["datasets", "jobs", job_id, "--quiet", "-o", str(out)],
    )

    assert result.exit_code == 0, result.output

    # Exactly ONE call to the jobs endpoint (no follow-up polling).
    poll_calls = [
        c for c in respx_mock.calls
        if c.request.url.path
        == f"/api/platform/v1/downloads/jobs/{job_id}"
    ]
    assert len(poll_calls) == 1, (
        f"completed-shortcut should hit jobs endpoint exactly once, "
        f"got {len(poll_calls)}"
    )


def test_jobs_reattach_failed_prints_error_and_exits_nonzero(
    cli_runner, respx_mock, tmp_home,
):
    """`verlet datasets jobs <job_id>` against a failed job — prints verbatim
    error + failed_stage on stderr and exits non-zero (D-FORMAT3)."""
    from verlet.cli import cli

    _seed_default_profile()
    job_id = "job-reattach-failed"

    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/jobs/{job_id}",
    ).respond(
        200,
        json={
            "job_id": job_id,
            "status": "failed",
            "progress": None,
            "manifest": None,
            "error_message": "ffmpeg returned non-zero",
            "failed_stage": "convert",
        },
    )

    result = cli_runner.invoke(
        cli, ["datasets", "jobs", job_id, "--quiet"],
    )

    assert result.exit_code != 0, result.output
    combined = (result.output or "") + (result.stderr or "")
    # Verbatim D-FORMAT3 message format:
    # "conversion failed: <error_message> (stage: <failed_stage>)"
    assert "conversion failed: ffmpeg returned non-zero" in combined, combined
    assert "stage: convert" in combined, combined


def _conversion(fmt: str = "hdf5", status: str = "completed") -> dict:
    """A DatasetConversionResponse-shaped row."""
    return {
        "id": f"conv-{fmt}",
        "catalog_dataset_id": "cd-1",
        "source_format": "lerobot-v2",
        "target_format": fmt,
        "target_format_version": "1.0",
        "status": status,
        "total_size_bytes": 1_500_000_000,
        "total_episodes": 10,
        "current_episode": 10,
        "error_message": None,
        "created_at": "2026-05-10T00:00:00+00:00",
        "completed_at": "2026-05-10T01:00:00+00:00",
    }


def test_jobs_no_id_lists_account_jobs(cli_runner, respx_mock, tmp_home):
    """`verlet datasets jobs` (no argument) lists every account conversion job."""
    from verlet.cli import cli

    _seed_default_profile()
    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs"
    ).respond(200, json=[_conversion("hdf5"), _conversion("rlds")])

    result = cli_runner.invoke(cli, ["datasets", "jobs"])
    assert result.exit_code == 0, result.output
    assert route.called
    assert "conv-hdf5" in result.output
    assert "conv-rlds" in result.output


def test_jobs_slug_lists_dataset_conversions(cli_runner, respx_mock, tmp_home):
    """`verlet datasets jobs --slug <ds>` lists that dataset's conversion jobs."""
    from verlet.cli import cli

    _seed_default_profile()
    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/imitate-cube/conversions"
    ).respond(200, json=[_conversion("hdf5")])

    result = cli_runner.invoke(
        cli, ["datasets", "jobs", "--slug", "imitate-cube"]
    )
    assert result.exit_code == 0, result.output
    assert route.called
    assert "conv-hdf5" in result.output


def test_jobs_no_id_empty_account(cli_runner, respx_mock, tmp_home):
    """No conversion jobs → the dim 'no jobs' message, exit 0."""
    from verlet.cli import cli

    _seed_default_profile()
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs"
    ).respond(200, json=[])

    result = cli_runner.invoke(cli, ["datasets", "jobs"])
    assert result.exit_code == 0, result.output
    assert "No conversion jobs" in result.output


def test_jobs_reattach_unknown_id_404(cli_runner, respx_mock, tmp_home):
    """Unknown job_id — server returns 404; CLI prints "job not found" and
    exits 1."""
    from verlet.cli import cli

    _seed_default_profile()
    job_id = "does-not-exist"

    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/jobs/{job_id}",
    ).respond(404, json={"detail": "Job not found"})

    result = cli_runner.invoke(
        cli, ["datasets", "jobs", job_id, "--quiet"],
    )
    assert result.exit_code != 0, result.output
    combined = (result.output or "") + (result.stderr or "")
    assert "job not found" in combined.lower(), combined
