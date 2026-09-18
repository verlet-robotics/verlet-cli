"""Shared download engine for ego and teleop datasets.

Every file goes through :func:`download_file`, which is where the robustness
lives: bytes stream into ``<name>.part``, the byte count is checked against
``Content-Length``, and only then is the file renamed into place — so a file
under its final name is complete by construction and a re-run can skip it on
``exists()`` alone. Transient failures (transport errors, 5xx, 429) retry
with backoff; 403/404 do not.
"""
import asyncio
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse

import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

CHUNK_SIZE = 1024 * 256
DEFAULT_TIMEOUT = httpx.Timeout(300.0, connect=30.0)
RETRY_BACKOFF_SECONDS = (1.0, 4.0, 16.0)  # 3 retries after the first attempt


class OptionalMissing(Exception):
    """An optional file 404'd — counts as skipped, not failed."""


class Expired(Exception):
    """A presigned URL was refused (403) — the manifest has expired."""


@dataclass
class DownloadPlanItem:
    """A pre-resolved download: URL already presigned, local path fixed.

    ``optional`` marks files the server presigns without an existence check
    (per-episode calibration); a 404 on one of those is "absent", not an error.
    """

    url: str
    local_path: Path
    optional: bool = False


@dataclass
class DownloadResult:
    """Summary of a download_files run. Printed to the user by callers."""

    downloaded: int
    skipped: int
    failed: int
    failures: list[tuple[Path, str]] = field(default_factory=list)


PresignFn = Callable[[str], Awaitable[str]]


def _apply_url_extension(local_path: Path, url: str) -> Path:
    """If local_path has no suffix, inherit the extension from the presigned URL.

    Ego logical keys like ``{segment_id}/overlay`` have no extension; the real
    object in R2 is ``overlay.mp4`` (or ``.rrd``, etc). Without this, files
    land on disk as extensionless blobs and video players reject them.
    """
    if local_path.suffix:
        return local_path
    url_name = Path(urlparse(url).path).name
    if "." not in url_name:
        return local_path
    ext = "." + url_name.rsplit(".", 1)[-1]
    return local_path.with_name(local_path.name + ext)


def _should_skip(local_path: Path, skip_existing: bool) -> bool:
    # A file under its final name was renamed there only after its byte count
    # matched Content-Length, so existence is proof of completeness.
    return skip_existing and local_path.exists()


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or code >= 500
    return False


async def _stream_once(client: httpx.AsyncClient, url: str, dest: Path) -> int:
    part = dest.with_name(dest.name + ".part")
    written = 0
    async with client.stream("GET", url) as resp:
        resp.raise_for_status()
        expected = resp.headers.get("content-length")
        with open(part, "wb") as f:
            async for chunk in resp.aiter_bytes(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                written += len(chunk)
    if expected is not None and written != int(expected):
        part.unlink(missing_ok=True)
        raise httpx.TransportError(
            f"truncated: got {written} of {expected} bytes for {dest.name}"
        )
    os.replace(part, dest)
    return written


async def download_file(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
    *,
    optional: bool = False,
) -> int:
    """Download one presigned URL to ``dest`` atomically. Returns bytes written.

    Retries transport errors / 5xx / 429 with backoff. Raises
    :class:`OptionalMissing` on a 404 of an optional file and
    :class:`Expired` on 403; anything else propagates after the last retry.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt, delay in enumerate((*RETRY_BACKOFF_SECONDS, None)):
        try:
            return await _stream_once(client, url, dest)
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            if code == 404 and optional:
                raise OptionalMissing(dest.name) from exc
            if code == 403:
                raise Expired(dest.name) from exc
            if delay is None or not _retryable(exc):
                raise
        except httpx.TransportError:
            if delay is None:
                raise
        await asyncio.sleep(delay)
    raise AssertionError("unreachable")


def _progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("files"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )


async def _run_plan(
    plan: list[tuple[Path, Callable[[], Awaitable[tuple[str, Path, bool]]]]],
    parallel: int,
    skip_existing: bool,
) -> DownloadResult:
    """Drive a list of ``(local_path, resolve)`` pairs; ``resolve`` yields the
    final ``(url, local_path, optional)`` once any presign step has run."""
    result = DownloadResult(0, 0, 0)
    semaphore = asyncio.Semaphore(parallel)
    expired = False

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        with _progress() as progress:
            overall = progress.add_task(f"Downloading {len(plan)} files", total=len(plan))

            async def one(local_path: Path, resolve) -> None:
                nonlocal expired
                async with semaphore:
                    try:
                        if _should_skip(local_path, skip_existing):
                            result.skipped += 1
                            return
                        url, resolved, optional = await resolve()
                        if _should_skip(resolved, skip_existing):
                            result.skipped += 1
                            return
                        await download_file(client, url, resolved, optional=optional)
                        result.downloaded += 1
                    except OptionalMissing:
                        result.skipped += 1
                    except Expired as exc:
                        expired = True
                        result.failed += 1
                        result.failures.append((local_path, str(exc)))
                    except Exception as exc:
                        result.failed += 1
                        result.failures.append((local_path, f"{type(exc).__name__}: {exc}"))
                    finally:
                        progress.advance(overall)

            await asyncio.gather(*(one(p, r) for p, r in plan), return_exceptions=True)

    if expired:
        Console().print(
            "[yellow]Some download URLs have expired.[/yellow] Re-run the same "
            "command to fetch a fresh manifest and resume; completed files are kept."
        )
    return result


async def download_files(
    keys: list[str],
    dest_dir: Path,
    presign_fn: PresignFn,
    strip_prefix: str = "",
    parallel: int = 8,
    dry_run: bool = False,
    skip_existing: bool = True,
) -> DownloadResult:
    """Download multiple S3 keys via a per-key presign callback.

    Used by the ego legacy-asset flow and teleop. Filenames are derived from
    the R2 key (minus `strip_prefix`); missing extensions are inherited from
    the presigned URL at resolution time. If you already have the URLs and
    want fixed filenames, use `download_resolved` instead.
    """
    if not keys:
        return DownloadResult(0, 0, 0)

    plan: list[tuple[str, Path]] = []
    for key in keys:
        relative = key
        if strip_prefix and key.startswith(strip_prefix):
            relative = key[len(strip_prefix):].lstrip("/")
        plan.append((key, dest_dir / relative))

    if dry_run:
        console = Console()
        console.print(f"\n[bold]Would download {len(plan)} files to {dest_dir}[/bold]\n")
        for key, local_path in plan[:20]:
            console.print(f"  {local_path}")
        if len(plan) > 20:
            console.print(f"  ... and {len(plan) - 20} more")
        return DownloadResult(0, 0, 0)

    def _resolver(key: str, local_path: Path):
        async def resolve() -> tuple[str, Path, bool]:
            url = await presign_fn(key)
            return url, _apply_url_extension(local_path, url), False

        return resolve

    return await _run_plan(
        [(p, _resolver(k, p)) for k, p in plan], parallel, skip_existing
    )


async def download_resolved(
    items: list[DownloadPlanItem],
    parallel: int = 8,
    skip_existing: bool = True,
) -> DownloadResult:
    """Download pre-resolved (URL, local_path) pairs.

    Used by the showcase / purchase manifest flows where the CLI already has
    every presigned URL and the final filename for each file.
    """
    if not items:
        return DownloadResult(0, 0, 0)

    def _resolver(item: DownloadPlanItem):
        async def resolve() -> tuple[str, Path, bool]:
            return item.url, item.local_path, item.optional

        return resolve

    return await _run_plan(
        [(i.local_path, _resolver(i)) for i in items], parallel, skip_existing
    )
