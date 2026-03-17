"""Async download engine with presigned URLs and Rich progress."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from verlet.auth import get_api_base, get_auth_headers
from verlet.display import console

CHUNK_SIZE = 256 * 1024  # 256 KB


async def _presign(
    client: httpx.AsyncClient,
    api_base: str,
    headers: dict[str, str],
    r2_key: str,
) -> str:
    """Get a presigned URL for an R2 key."""
    resp = await client.get(
        f"{api_base}/api/presign",
        params={"key": r2_key, "bucket": "output"},
        headers=headers,
    )
    if resp.status_code == 401:
        raise RuntimeError("Session expired. Run 'verlet login' to re-authenticate.")
    resp.raise_for_status()
    return resp.json()["url"]


async def _download_file(
    client: httpx.AsyncClient,
    api_base: str,
    auth_headers: dict[str, str],
    r2_key: str,
    local_path: Path,
    semaphore: asyncio.Semaphore,
    progress: Progress,
    overall_task: TaskID,
    retries: int = 3,
) -> bool:
    """Download a single file with retries and progress tracking."""
    tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")

    for attempt in range(1, retries + 1):
        try:
            async with semaphore:
                url = await _presign(client, api_base, auth_headers, r2_key)

                async with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    total = int(resp.headers.get("content-length", 0))

                    # Skip if local file matches remote size
                    if local_path.exists() and local_path.stat().st_size == total and total > 0:
                        progress.advance(overall_task, total)
                        return True

                    local_path.parent.mkdir(parents=True, exist_ok=True)

                    file_task = progress.add_task(
                        f"  {r2_key.rsplit('/', 1)[-1]}",
                        total=total or None,
                    )

                    with open(tmp_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                            f.write(chunk)
                            progress.advance(file_task, len(chunk))
                            progress.advance(overall_task, len(chunk))

                    progress.remove_task(file_task)

                # Atomic rename
                tmp_path.rename(local_path)
                return True

        except Exception as e:
            if tmp_path.exists():
                tmp_path.unlink()
            if attempt == retries:
                console.print(f"[red]Failed[/] {r2_key}: {e}")
                return False
            await asyncio.sleep(2 ** attempt)

    return False


async def download_files(
    file_plan: list[tuple[str, Path]],
    parallel: int = 8,
) -> tuple[int, int]:
    """Download a list of (r2_key, local_path) pairs concurrently.

    Returns (success_count, fail_count).
    """
    if not file_plan:
        console.print("Nothing to download.")
        return 0, 0

    # Check which files can be skipped (already downloaded with correct size)
    api_base = get_api_base()
    auth_headers = get_auth_headers()
    semaphore = asyncio.Semaphore(parallel)

    # Estimate total bytes (unknown until presign, so we track dynamically)
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        overall_task = progress.add_task("Downloading", total=0)

        # First pass: get total size by presigning all files
        # (done lazily during download to avoid expiry)

        async with httpx.AsyncClient(timeout=httpx.Timeout(60, connect=15)) as client:
            tasks = [
                _download_file(
                    client, api_base, auth_headers,
                    r2_key, local_path,
                    semaphore, progress, overall_task,
                )
                for r2_key, local_path in file_plan
            ]
            results = await asyncio.gather(*tasks)

    ok = sum(1 for r in results if r)
    fail = sum(1 for r in results if not r)
    return ok, fail
