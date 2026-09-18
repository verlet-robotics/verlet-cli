"""verlet.download: atomic writes, size check, retries, optional files."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
import respx

import verlet.download as dl
from verlet.download import DownloadPlanItem, download_resolved


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(dl, "RETRY_BACKOFF_SECONDS", (0.0, 0.0, 0.0))


def _run(items):
    return asyncio.run(download_resolved(items, parallel=2, skip_existing=True))


@respx.mock
def test_transient_error_then_success_leaves_no_part_file(tmp_path: Path):
    route = respx.get("https://r2.test/a.bin")
    route.side_effect = [httpx.Response(503), httpx.Response(200, content=b"hello")]
    dest = tmp_path / "a.bin"

    result = _run([DownloadPlanItem(url="https://r2.test/a.bin", local_path=dest)])

    assert (result.downloaded, result.failed) == (1, 0)
    assert dest.read_bytes() == b"hello"
    assert not list(tmp_path.glob("*.part"))
    assert route.call_count == 2


@respx.mock
def test_truncated_body_is_a_failure_not_a_file(tmp_path: Path):
    respx.get("https://r2.test/b.bin").mock(
        return_value=httpx.Response(200, content=b"abc", headers={"content-length": "10"})
    )
    dest = tmp_path / "b.bin"

    result = _run([DownloadPlanItem(url="https://r2.test/b.bin", local_path=dest)])

    assert result.failed == 1 and result.downloaded == 0
    assert not dest.exists() and not list(tmp_path.glob("*.part"))
    assert "truncated" in result.failures[0][1]


@respx.mock
def test_optional_404_counts_as_skipped(tmp_path: Path):
    respx.get("https://r2.test/cal.json").mock(return_value=httpx.Response(404))
    item = DownloadPlanItem(
        url="https://r2.test/cal.json", local_path=tmp_path / "cal.json", optional=True
    )

    result = _run([item])

    assert (result.downloaded, result.skipped, result.failed) == (0, 1, 0)


@respx.mock
def test_existing_final_file_is_skipped_without_a_request(tmp_path: Path):
    route = respx.get("https://r2.test/c.bin").mock(return_value=httpx.Response(200, content=b"x"))
    dest = tmp_path / "c.bin"
    dest.write_bytes(b"already here")

    result = _run([DownloadPlanItem(url="https://r2.test/c.bin", local_path=dest)])

    assert result.skipped == 1 and route.call_count == 0
