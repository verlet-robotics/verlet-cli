"""CLIDATA-07 (Plan 30-04): --format flag + 202 + job-id polling loop.

Tests are split into two halves:

* **Pure-function tests** for ``validate_format`` and ``poll_conversion_job``
  (Task 1) — exercise ``verlet/datasets/convert.py`` in isolation.
* **End-to-end command tests** for ``verlet datasets download --format <fmt>``
  (Task 2) — drive ``verlet/datasets/commands.py`` through Click's CliRunner
  with respx-mocked manifest + jobs endpoints.

Both halves stay in this single file because they share the same fixtures
and the verbatim error wording from D-FORMAT3 lives in one place
(``conversion failed: ...``). The Phase 27 ``JobPollResponse`` shape used by
the polling tests is documented in ``30-RESEARCH.md`` "Pattern 1".
"""
from __future__ import annotations

from unittest.mock import patch

import click
import httpx
import pytest

from verlet.auth.credentials import upsert_profile


# ---------------------------------------------------------------------------
# Task 1: validate_format
# ---------------------------------------------------------------------------


def test_validate_format_accepts_hdf5():
    from verlet.datasets.convert import validate_format

    assert validate_format("hdf5") == "hdf5"


def test_validate_format_accepts_all_eight_formats():
    from verlet.datasets.convert import SUPPORTED_FORMATS, validate_format

    expected = (
        "lerobot-v2",
        "lerobot-v3",
        "hdf5",
        "zarr",
        "rlds",
        "rosbag",
        "robodm",
        "egomimic",
    )
    assert SUPPORTED_FORMATS == expected
    for fmt in expected:
        assert validate_format(fmt) == fmt


def test_validate_format_none_returns_none():
    from verlet.datasets.convert import validate_format

    # None means "no format flag passed" → native, no conversion.
    assert validate_format(None) is None


def test_validate_format_rejects_unknown_with_message():
    from verlet.datasets.convert import validate_format

    with pytest.raises(click.BadParameter) as exc:
        validate_format("invalid-format")
    msg = str(exc.value)
    assert "must be one of" in msg
    # All 8 formats listed verbatim.
    for fmt in (
        "lerobot-v2",
        "lerobot-v3",
        "hdf5",
        "zarr",
        "rlds",
        "rosbag",
        "robodm",
        "egomimic",
    ):
        assert fmt in msg, f"{fmt} missing from BadParameter message"


def test_validate_format_reexported_from_validation_module():
    """``_validation.py`` re-exports ``validate_format`` + ``SUPPORTED_FORMATS``
    so callers can import the whole flag-matrix surface from one place
    (Phase 29 convention)."""
    from verlet.datasets._validation import (  # noqa: F401
        SUPPORTED_FORMATS as REEXPORTED,
    )
    from verlet.datasets._validation import validate_format as reexported_fn
    from verlet.datasets.convert import (
        SUPPORTED_FORMATS as CANONICAL,
    )
    from verlet.datasets.convert import validate_format as canonical_fn

    assert reexported_fn is canonical_fn
    assert REEXPORTED == CANONICAL


# ---------------------------------------------------------------------------
# Task 1: poll_conversion_job
# ---------------------------------------------------------------------------


def _seed_default_profile(_tmp_home, *, token: str = "t0k3n") -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        access_token=token,
        api_url="https://api.verlet.co",
    )


def test_poll_conversion_job_processing_then_completed_returns_manifest(
    respx_mock, tmp_home,
):
    """Two polls: first ``processing``, second ``completed`` — returns the
    inlined manifest dict from the completed body."""
    from verlet.api_client import AuthenticatedClient
    from verlet.datasets.convert import poll_conversion_job

    _seed_default_profile(tmp_home)

    manifest_body = {
        "dataset_id": "00000000-0000-0000-0000-000000000001",
        "dataset_slug": "pick-and-place-yam-v3",
        "format": "hdf5",
        "files": [],
    }
    # respx returns each .respond(...) call in registration order.
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs/job-1",
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "job_id": "job-1",
                    "status": "processing",
                    "progress": {
                        "current_episode": 1,
                        "total_episodes": 5,
                    },
                    "manifest": None,
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
            httpx.Response(
                200,
                json={
                    "job_id": "job-1",
                    "status": "completed",
                    "progress": None,
                    "manifest": manifest_body,
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
        ]
    )

    client = AuthenticatedClient("default")
    try:
        # Stub asyncio.sleep to avoid actually sleeping 3s.
        with patch("verlet.datasets.convert.asyncio.sleep") as sleep_mock:
            import asyncio as _asyncio

            async def _noop(_secs):
                return None

            sleep_mock.side_effect = _noop
            result = _asyncio.run(
                poll_conversion_job(client, "job-1", verbose=False, quiet=True)
            )
    finally:
        client.close()

    assert result == manifest_body
    # Sleep was invoked exactly once (between the two polls).
    assert sleep_mock.call_count == 1


def test_poll_conversion_job_failed_writes_stderr_and_exits_nonzero(
    respx_mock, tmp_home, capsys,
):
    """``status=="failed"`` → verbatim ``error_message`` + ``failed_stage`` to
    stderr (D-FORMAT3) and SystemExit(1)."""
    from verlet.api_client import AuthenticatedClient
    from verlet.datasets.convert import poll_conversion_job

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs/job-2",
    ).respond(
        200,
        json={
            "job_id": "job-2",
            "status": "failed",
            "progress": None,
            "manifest": None,
            "error_message": "ffmpeg returned non-zero",
            "failed_stage": "convert",
        },
    )

    client = AuthenticatedClient("default")
    try:
        import asyncio as _asyncio

        with pytest.raises(SystemExit) as exc:
            _asyncio.run(
                poll_conversion_job(
                    client, "job-2", verbose=False, quiet=True
                )
            )
    finally:
        client.close()

    assert exc.value.code == 1
    captured = capsys.readouterr()
    # D-FORMAT3: verbatim error + failed_stage on stderr.
    assert (
        "conversion failed: ffmpeg returned non-zero (stage: convert)"
        in captured.err
    )


def test_poll_conversion_job_polls_with_3s_interval(respx_mock, tmp_home):
    """Default poll cadence is ``POLL_INTERVAL_SECONDS`` (3.0)."""
    from verlet.api_client import AuthenticatedClient
    from verlet.datasets.convert import POLL_INTERVAL_SECONDS, poll_conversion_job

    _seed_default_profile(tmp_home)

    assert POLL_INTERVAL_SECONDS == 3.0  # Locked in module constant.

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs/job-3",
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "job_id": "job-3",
                    "status": "processing",
                    "progress": {"current_episode": 0, "total_episodes": 1},
                    "manifest": None,
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
            httpx.Response(
                200,
                json={
                    "job_id": "job-3",
                    "status": "completed",
                    "progress": None,
                    "manifest": {"files": []},
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
        ]
    )

    client = AuthenticatedClient("default")
    try:
        with patch("verlet.datasets.convert.asyncio.sleep") as sleep_mock:
            async def _noop(_secs):
                return None

            sleep_mock.side_effect = _noop
            import asyncio as _asyncio

            _asyncio.run(
                poll_conversion_job(client, "job-3", verbose=False, quiet=True)
            )
    finally:
        client.close()

    # Assert sleep called with the locked POLL_INTERVAL_SECONDS constant.
    assert sleep_mock.call_args.args[0] == POLL_INTERVAL_SECONDS


def test_poll_conversion_job_verbose_streams_log_lines_to_stderr(
    respx_mock, tmp_home, capsys,
):
    """``verbose=True`` → server ``progress.log_lines`` echoed to stderr."""
    from verlet.api_client import AuthenticatedClient
    from verlet.datasets.convert import poll_conversion_job

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs/job-4",
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "job_id": "job-4",
                    "status": "processing",
                    "progress": {
                        "current_episode": 2,
                        "total_episodes": 4,
                        "log_lines": ["stage start", "stage end"],
                    },
                    "manifest": None,
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
            httpx.Response(
                200,
                json={
                    "job_id": "job-4",
                    "status": "completed",
                    "progress": None,
                    "manifest": {"files": []},
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
        ]
    )

    client = AuthenticatedClient("default")
    try:
        with patch("verlet.datasets.convert.asyncio.sleep") as sleep_mock:
            async def _noop(_secs):
                return None

            sleep_mock.side_effect = _noop
            import asyncio as _asyncio

            _asyncio.run(
                poll_conversion_job(client, "job-4", verbose=True, quiet=True)
            )
    finally:
        client.close()

    captured = capsys.readouterr()
    # Both server log lines surfaced on stderr (with [server] prefix).
    assert "[server] stage start" in captured.err
    assert "[server] stage end" in captured.err


def test_poll_conversion_job_failed_without_failed_stage(
    respx_mock, tmp_home, capsys,
):
    """``failed_stage=None`` → just ``error_message`` (no `(stage: ...)` suffix)."""
    from verlet.api_client import AuthenticatedClient
    from verlet.datasets.convert import poll_conversion_job

    _seed_default_profile(tmp_home)

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs/job-5",
    ).respond(
        200,
        json={
            "job_id": "job-5",
            "status": "failed",
            "progress": None,
            "manifest": None,
            "error_message": "internal error",
            "failed_stage": None,
        },
    )

    client = AuthenticatedClient("default")
    try:
        import asyncio as _asyncio

        with pytest.raises(SystemExit) as exc:
            _asyncio.run(
                poll_conversion_job(
                    client, "job-5", verbose=False, quiet=True
                )
            )
    finally:
        client.close()

    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "conversion failed: internal error" in captured.err
    # No stage suffix when failed_stage is None.
    assert "(stage:" not in captured.err


# ---------------------------------------------------------------------------
# Task 2: --format flag wired into ``verlet datasets download``
# ---------------------------------------------------------------------------


def _arm_detail(slug: str = "pick-and-place-yam-v3") -> dict:
    """Mirror tests/datasets/test_download.py's helper for arm rows."""
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "slug": slug,
        "title": "Pick and Place YAM v3",
        "data_tiers": ["processed"],
        "available_variants": ["processed"],
    }


def test_download_format_hdf5_polls_and_completes(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """``verlet datasets download <slug> --format hdf5`` — manifest endpoint
    returns 202 + ``{job_id}``; CLI polls ``/downloads/jobs/<id>`` until
    completed; download driver receives the inlined manifest."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    slug = "pick-and-place-yam-v3"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_arm_detail(slug))

    # Manifest endpoint returns 202 + job_id (Manifest202Response).
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/manifest",
    ).respond(
        202,
        json={
            "job_id": "job-7",
            "status": "processing",
            "poll_url": "/api/platform/v1/downloads/jobs/job-7",
        },
    )

    # Job poll: one processing → completed with inlined manifest.
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/jobs/job-7",
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "job_id": "job-7",
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
                    "job_id": "job-7",
                    "status": "completed",
                    "progress": None,
                    "manifest": mock_arm_manifest_response,
                    "error_message": None,
                    "failed_stage": None,
                },
            ),
        ]
    )

    with patch(
        "verlet.datasets.convert.asyncio.sleep",
        new=_async_noop_sleep,
    ):
        result = cli_runner.invoke(
            cli,
            [
                "datasets",
                "download",
                slug,
                "--format",
                "hdf5",
                "--dry-run",  # short-circuit before download_resolved
                "--quiet",  # suppress Rich progress in test output
            ],
        )

    assert result.exit_code == 0, result.output

    # Manifest endpoint hit with ?format=hdf5.
    manifest_calls = [
        c for c in respx_mock.calls
        if c.request.url.path
        == f"/api/platform/v1/downloads/{slug}/manifest"
    ]
    assert manifest_calls, "manifest endpoint should have fired"
    assert manifest_calls[-1].request.url.params.get("format") == "hdf5"

    # Job poll endpoint hit at least twice (processing → completed).
    poll_calls = [
        c for c in respx_mock.calls
        if c.request.url.path
        == "/api/platform/v1/downloads/jobs/job-7"
    ]
    assert len(poll_calls) >= 2, (
        f"expected ≥2 poll calls, got {len(poll_calls)}"
    )


async def _async_noop_sleep(_secs):
    """Patch target for ``asyncio.sleep`` so polling tests stay instant."""
    return None


def test_download_no_format_native_200_no_poll(
    cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """``verlet datasets download <slug>`` (no --format) — manifest 200 ↦
    inline path; CLI does NOT poll ``/downloads/jobs/...`` at all."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    slug = "pick-and-place-yam-v3"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_arm_detail(slug))
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/manifest",
    ).respond(200, json=mock_arm_manifest_response)

    result = cli_runner.invoke(
        cli, ["datasets", "download", slug, "--dry-run"],
    )
    assert result.exit_code == 0, result.output

    # NO calls to /downloads/jobs/ — the 200 path stayed inline.
    poll_calls = [
        c for c in respx_mock.calls
        if "/downloads/jobs/" in c.request.url.path
    ]
    assert poll_calls == [], (
        f"native (no --format) path must not poll jobs endpoint; "
        f"got {[c.request.url.path for c in poll_calls]}"
    )


def test_download_format_invalid_rejected_pre_http(
    cli_runner, respx_mock, tmp_home,
):
    """Unknown ``--format`` value → click.BadParameter (exit 2). NO HTTP fires."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    result = cli_runner.invoke(
        cli,
        [
            "datasets",
            "download",
            "any-slug",
            "--format",
            "definitely-not-a-format",
            "--dry-run",
        ],
    )
    # click.BadParameter → exit 2.
    assert result.exit_code == 2, result.output
    # The validator's message is part of stderr/output.
    output = (result.output or "") + (result.stderr or "")
    assert "must be one of" in output, output

    # Zero HTTP calls — validator fires before any network work.
    assert len(respx_mock.calls) == 0, (
        f"invalid --format must short-circuit before HTTP, "
        f"got {len(respx_mock.calls)} calls"
    )


@pytest.mark.parametrize(
    "fmt",
    [
        "lerobot-v2",
        "lerobot-v3",
        "hdf5",
        "zarr",
        "rlds",
        "rosbag",
        "robodm",
        "egomimic",
    ],
)
def test_download_all_eight_formats_send_format_param(
    fmt, cli_runner, respx_mock, tmp_home, mock_arm_manifest_response,
):
    """All 8 supported formats reach the manifest endpoint with
    ``?format=<fmt>``. Native (lerobot-v2) returns 200, others 202+poll."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home)

    slug = "pick-and-place-yam-v3"
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{slug}",
    ).respond(200, json=_arm_detail(slug))

    if fmt == "lerobot-v2":
        # Native — 200 + manifest, no polling.
        respx_mock.get(
            f"https://api.verlet.co/api/platform/v1/downloads/{slug}/manifest",
        ).respond(200, json=mock_arm_manifest_response)
        result = cli_runner.invoke(
            cli,
            [
                "datasets",
                "download",
                slug,
                "--format",
                fmt,
                "--dry-run",
            ],
        )
    else:
        # Non-native — 202 + job_id, then a single completed poll.
        respx_mock.get(
            f"https://api.verlet.co/api/platform/v1/downloads/{slug}/manifest",
        ).respond(
            202,
            json={
                "job_id": f"job-{fmt}",
                "status": "processing",
                "poll_url": f"/api/platform/v1/downloads/jobs/job-{fmt}",
            },
        )
        respx_mock.get(
            f"https://api.verlet.co/api/platform/v1/downloads/jobs/job-{fmt}",
        ).respond(
            200,
            json={
                "job_id": f"job-{fmt}",
                "status": "completed",
                "progress": None,
                "manifest": mock_arm_manifest_response,
                "error_message": None,
                "failed_stage": None,
            },
        )
        with patch(
            "verlet.datasets.convert.asyncio.sleep",
            new=_async_noop_sleep,
        ):
            result = cli_runner.invoke(
                cli,
                [
                    "datasets",
                    "download",
                    slug,
                    "--format",
                    fmt,
                    "--dry-run",
                    "--quiet",
                ],
            )

    assert result.exit_code == 0, f"format {fmt} failed: {result.output}"

    manifest_calls = [
        c for c in respx_mock.calls
        if c.request.url.path
        == f"/api/platform/v1/downloads/{slug}/manifest"
    ]
    assert manifest_calls, f"manifest endpoint missing for {fmt}"
    assert manifest_calls[-1].request.url.params.get("format") == fmt


def test_download_format_help_documents_option():
    """``verlet datasets download --help`` text references ``--format``."""
    from click.testing import CliRunner

    from verlet.cli import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["datasets", "download", "--help"])
    assert result.exit_code == 0, result.output
    assert "--format" in result.output, result.output
