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
