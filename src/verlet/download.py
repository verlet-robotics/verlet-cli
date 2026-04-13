"""Shared download engine for ego and teleop datasets."""
import asyncio
from dataclasses import dataclass
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


@dataclass
class DownloadPlanItem:
    """A pre-resolved download: URL already presigned, local path fixed.

    Used by the training-bundle flow which knows the final filename up front
    (`video.mp4`, `poses.hdf5`, `depth.mkv`) and already has all presigned
    URLs from a single `/training-bundle` call per segment. Bypasses the
    `presign_fn` round-trip path used by the legacy per-asset downloader.
    """

    url: str
    local_path: Path


@dataclass
class DownloadResult:
    """Summary of a download_files run. Printed to the user by callers."""

    downloaded: int
    skipped: int
    failed: int


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
    if not skip_existing:
        return False
    try:
        return local_path.exists() and local_path.stat().st_size > 0
    except OSError:
        return False


async def download_file(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
) -> int:
    """Download a single file from a presigned URL. Returns bytes written."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    async with client.stream("GET", url) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            async for chunk in resp.aiter_bytes(chunk_size=CHUNK_SIZE):
                f.write(chunk)
                written += len(chunk)
    return written


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
        console.print(
            f"\n[bold]Would download {len(plan)} files to {dest_dir}[/bold]\n"
        )
        for key, local_path in plan[:20]:
            console.print(f"  {local_path}")
        if len(plan) > 20:
            console.print(f"  ... and {len(plan) - 20} more")
        return DownloadResult(0, 0, 0)

    downloaded = 0
    skipped = 0
    failed = 0
    semaphore = asyncio.Semaphore(parallel)

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        with _progress() as progress:
            overall = progress.add_task(
                f"Downloading {len(plan)} files", total=len(plan)
            )

            async def one(key: str, local_path: Path) -> None:
                nonlocal downloaded, skipped, failed
                async with semaphore:
                    try:
                        # Cheap upfront skip — works when the destination
                        # filename is already set (no extension inference
                        # needed). Saves a presign round-trip on re-runs.
                        if _should_skip(local_path, skip_existing):
                            skipped += 1
                            return
                        url = await presign_fn(key)
                        resolved = _apply_url_extension(local_path, url)
                        if _should_skip(resolved, skip_existing):
                            skipped += 1
                            return
                        await download_file(client, url, resolved)
                        downloaded += 1
                    except Exception:
                        failed += 1
                    finally:
                        progress.advance(overall)

            await asyncio.gather(
                *(one(k, p) for k, p in plan), return_exceptions=True
            )

    return DownloadResult(downloaded, skipped, failed)


async def download_resolved(
    items: list[DownloadPlanItem],
    parallel: int = 8,
    skip_existing: bool = True,
) -> DownloadResult:
    """Download pre-resolved (URL, local_path) pairs.

    Used by the training-bundle flow where the CLI has already fetched
    presigned URLs in bulk and knows the final filename for each file
    (`video.mp4` / `depth.mkv` / `poses.hdf5`). No extension inference, no
    presign round-trips.
    """
    if not items:
        return DownloadResult(0, 0, 0)

    downloaded = 0
    skipped = 0
    failed = 0
    semaphore = asyncio.Semaphore(parallel)

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        with _progress() as progress:
            overall = progress.add_task(
                f"Downloading {len(items)} files", total=len(items)
            )

            async def one(item: DownloadPlanItem) -> None:
                nonlocal downloaded, skipped, failed
                async with semaphore:
                    try:
                        if _should_skip(item.local_path, skip_existing):
                            skipped += 1
                            return
                        await download_file(client, item.url, item.local_path)
                        downloaded += 1
                    except Exception:
                        failed += 1
                    finally:
                        progress.advance(overall)

            await asyncio.gather(
                *(one(i) for i in items), return_exceptions=True
            )

    return DownloadResult(downloaded, skipped, failed)
