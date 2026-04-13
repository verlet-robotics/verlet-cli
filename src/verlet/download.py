"""Shared download engine for ego and teleop datasets."""
import asyncio
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse

import httpx
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


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
            async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                f.write(chunk)
                written += len(chunk)
    return written


PresignFn = Callable[[str], Awaitable[str]]


async def download_files(
    keys: list[str],
    dest_dir: Path,
    presign_fn: PresignFn,
    strip_prefix: str = "",
    parallel: int = 8,
    dry_run: bool = False,
) -> int:
    """Download multiple files with progress display.

    Args:
        keys: S3 keys to download.
        dest_dir: Local directory root.
        presign_fn: Async function that takes a key and returns a presigned URL.
        strip_prefix: Prefix to strip from keys to compute relative local paths.
        parallel: Max concurrent downloads.
        dry_run: If True, print what would be downloaded without downloading.

    Returns:
        Number of files downloaded.
    """
    if not keys:
        return 0

    # Build download plan
    plan: list[tuple[str, Path]] = []
    for key in keys:
        relative = key
        if strip_prefix and key.startswith(strip_prefix):
            relative = key[len(strip_prefix):].lstrip("/")
        local_path = dest_dir / relative
        plan.append((key, local_path))

    if dry_run:
        from rich.console import Console
        console = Console()
        console.print(f"\n[bold]Would download {len(plan)} files to {dest_dir}[/bold]\n")
        for key, local_path in plan[:20]:
            console.print(f"  {local_path}")
        if len(plan) > 20:
            console.print(f"  ... and {len(plan) - 20} more")
        return 0

    semaphore = asyncio.Semaphore(parallel)
    downloaded = 0

    async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=30.0)) as client:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TextColumn("files"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            overall = progress.add_task(
                f"Downloading {len(plan)} files",
                total=len(plan),
            )

            async def download_one(key: str, local_path: Path) -> None:
                nonlocal downloaded
                async with semaphore:
                    url = await presign_fn(key)
                    resolved = _apply_url_extension(local_path, url)
                    try:
                        await download_file(client, url, resolved)
                        downloaded += 1
                    finally:
                        progress.advance(overall)

            tasks = [download_one(key, path) for key, path in plan]
            await asyncio.gather(*tasks, return_exceptions=True)

    return downloaded
