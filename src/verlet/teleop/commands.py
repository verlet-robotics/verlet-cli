"""Teleop CLI commands."""
import asyncio
import re
from pathlib import Path

import click

from verlet.display import console, teleop_dataset_table, teleop_episode_table, format_bytes


@click.group("teleop")
def teleop_group():
    """Teleoperation robot data (LeRobot v2.1 format)."""
    pass


def _resolve_dataset_id(datasets: list[dict], ident: str) -> str:
    """Resolve a full UUID or unambiguous short prefix to a dataset id.

    Users see truncated 8-char IDs in `verlet teleop list`, so other commands
    accept any prefix.
    """
    matches = [d for d in datasets if d["id"].startswith(ident)]
    if not matches:
        raise click.ClickException(f"Dataset '{ident}' not found.")
    if len(matches) > 1:
        ids = ", ".join(m["id"][:12] for m in matches[:5])
        raise click.ClickException(
            f"Dataset prefix '{ident}' is ambiguous ({len(matches)} matches: {ids}...). "
            "Use a longer prefix."
        )
    return matches[0]["id"]


@teleop_group.command("list")
@click.option("--detailed", is_flag=True, help="Show episode-level breakdown")
def teleop_list(detailed: bool):
    """List available teleop datasets."""
    from verlet.teleop.catalog import fetch_teleop_catalog, fetch_teleop_dataset

    catalog = asyncio.run(fetch_teleop_catalog())
    datasets = catalog.get("datasets", [])

    if not datasets:
        console.print("[dim]No teleop datasets found.[/dim]")
        return

    table = teleop_dataset_table(datasets)
    console.print(table)
    console.print(f"\n[dim]{len(datasets)} dataset(s) total[/dim]")

    if detailed:
        for ds in datasets:
            console.print(f"\n[bold]{ds['task_name']}[/bold] ({ds['id'][:8]})")
            detail = asyncio.run(fetch_teleop_dataset(ds["id"]))
            episodes = detail.get("episodes", [])
            if episodes:
                ep_table = teleop_episode_table(episodes)
                console.print(ep_table)
            camera_names = detail.get("camera_names", [])
            if camera_names:
                console.print(f"  Cameras: {', '.join(camera_names)}")


@teleop_group.command("info")
@click.argument("dataset_id")
def teleop_info(dataset_id: str):
    """Show details for a specific teleop dataset.

    DATASET_ID may be a full UUID or any unambiguous prefix (e.g. the 8-char
    ID shown by `verlet teleop list`).
    """
    from verlet.teleop.catalog import (
        fetch_teleop_catalog,
        fetch_teleop_dataset,
        fetch_teleop_files,
    )

    catalog = asyncio.run(fetch_teleop_catalog())
    dataset_id = _resolve_dataset_id(catalog.get("datasets", []), dataset_id)

    detail = asyncio.run(fetch_teleop_dataset(dataset_id))
    ds = detail["dataset"]
    episodes = detail.get("episodes", [])
    camera_names = detail.get("camera_names", [])

    console.print(f"\n[bold]{ds['task_name']}[/bold]")
    if ds.get("task_description"):
        console.print(f"  {ds['task_description']}")
    console.print(f"  Episodes: {ds['total_episodes']}")
    console.print(f"  Frames:   {ds['total_frames']:,}")
    console.print(f"  Duration: {ds['total_duration_secs']:.1f}s")
    console.print(f"  Size:     {format_bytes(ds.get('total_bytes'))}")
    if camera_names:
        console.print(f"  Cameras:  {', '.join(camera_names)}")

    if episodes:
        console.print()
        ep_table = teleop_episode_table(episodes)
        console.print(ep_table)

    # File summary
    try:
        files_data = asyncio.run(fetch_teleop_files(dataset_id))
        keys = files_data.get("keys", [])
        if keys:
            parquet = sum(1 for k in keys if k.endswith(".parquet"))
            mp4 = sum(1 for k in keys if k.endswith(".mp4"))
            json_files = sum(1 for k in keys if k.endswith(".json") or k.endswith(".jsonl"))
            console.print(f"\n  Files: {len(keys)} total ({parquet} parquet, {mp4} video, {json_files} meta)")
    except Exception:
        pass


@teleop_group.command("download")
@click.option("-o", "--output", default="./verlet-data", help="Output directory")
@click.option("--task", "task_name", default=None, help="Filter by task name")
@click.option("--parallel", default=8, help="Max concurrent downloads")
@click.option(
    "--force",
    is_flag=True,
    help="Re-download files even when they already exist on disk",
)
@click.option("--dry-run", is_flag=True, help="Show what would be downloaded")
def teleop_download(
    output: str,
    task_name: str | None,
    parallel: int,
    force: bool,
    dry_run: bool,
):
    """Download teleop datasets in LeRobot v2.1 format."""
    from verlet.teleop.catalog import fetch_teleop_catalog, fetch_teleop_files, presign_teleop_file
    from verlet.download import download_files
    from verlet.license import check_license_accepted, prompt_license_acceptance, write_license_file

    if not dry_run and not check_license_accepted():
        if not prompt_license_acceptance():
            console.print("[dim]Download cancelled.[/dim]")
            return

    catalog = asyncio.run(fetch_teleop_catalog())
    datasets = catalog.get("datasets", [])

    if task_name:
        pattern = task_name.lower()
        datasets = [d for d in datasets if pattern in d["task_name"].lower()]

    if not datasets:
        console.print("[dim]No matching teleop datasets found.[/dim]")
        return

    console.print(f"[bold]Found {len(datasets)} dataset(s) to download[/bold]")

    dest_root = Path(output)
    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for ds in datasets:
        task_slug = re.sub(r"[^a-zA-Z0-9_-]", "-", ds["task_name"]).strip("-").lower()
        dataset_dir = dest_root / "teleop" / task_slug

        console.print(f"\n[bold]{ds['task_name']}[/bold] -> {dataset_dir}")

        files_data = asyncio.run(fetch_teleop_files(ds["id"]))
        keys = files_data.get("keys", [])
        prefix = files_data.get("prefix", "")

        if not keys:
            console.print("  [dim]No files found[/dim]")
            continue

        console.print(f"  {len(keys)} files")

        async def presign(key: str) -> str:
            return await presign_teleop_file(ds["id"], key)

        result = asyncio.run(
            download_files(
                keys=keys,
                dest_dir=dataset_dir,
                presign_fn=presign,
                strip_prefix=prefix,
                parallel=parallel,
                dry_run=dry_run,
                skip_existing=not force,
            )
        )
        total_downloaded += result.downloaded
        total_skipped += result.skipped
        total_failed += result.failed

        if not dry_run and result.downloaded > 0:
            write_license_file(dataset_dir)

    if not dry_run:
        summary = [f"[green]{total_downloaded}[/green] downloaded"]
        if total_skipped:
            summary.append(f"[dim]{total_skipped} already on disk[/dim]")
        if total_failed:
            summary.append(f"[red]{total_failed} failed[/red]")
        console.print(f"\n{', '.join(summary)} -> {dest_root}")
