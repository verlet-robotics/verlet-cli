"""Click commands for the ego modality."""

from __future__ import annotations

import asyncio
from pathlib import Path

import click

from verlet.display import category_table, console, format_duration, segment_table
from verlet.ego.catalog import (
    fetch_catalog,
    filter_categories,
    filter_segments,
    flatten_segments,
    segment_file_list,
    segment_r2_prefix,
)


@click.group("ego")
def ego_group():
    """EgoDex hand-pose data."""


@ego_group.command("list")
@click.option("--task", default=None, help="Filter by station/task name.")
@click.option("--category", default=None, help="Filter by category.")
@click.option("--detailed", is_flag=True, help="Show individual segments.")
def list_cmd(task: str | None, category: str | None, detailed: bool):
    """List available EgoDex data."""
    catalog = fetch_catalog()

    if detailed:
        segments = flatten_segments(catalog)
        segments = filter_segments(segments, task=task, category=category)
        if not segments:
            console.print("No segments found matching filters.")
            return
        console.print(segment_table(segments))
        console.print(f"\n[bold]{len(segments)}[/bold] segments, "
                      f"{format_duration(sum(s.get('durationSec', 0) for s in segments))} total")
    else:
        categories = catalog.get("categories", [])
        categories = filter_categories(categories, task=task, category=category)
        if not categories:
            console.print("No categories found matching filters.")
            return
        console.print(category_table(categories))
        total_segs = sum(c.get("segmentCount", 0) for c in categories)
        total_dur = sum(c.get("totalDurationSec", 0) for c in categories)
        console.print(f"\n[bold]{total_segs}[/bold] segments, {format_duration(total_dur)} total")


@ego_group.command("info")
@click.argument("segment_id")
def info_cmd(segment_id: str):
    """Show details for a specific segment."""
    catalog = fetch_catalog()
    segments = flatten_segments(catalog)

    seg = next((s for s in segments if s["id"] == segment_id), None)
    if not seg:
        raise click.ClickException(f"Segment '{segment_id}' not found.")

    console.print(f"[bold]Segment:[/bold] {seg['id']}")
    console.print(f"[bold]Station:[/bold] {seg.get('station', 'N/A')}")
    console.print(f"[bold]Category:[/bold] {seg.get('category', 'N/A')}")
    console.print(f"[bold]Subcategory:[/bold] {seg.get('subcategory', 'N/A')}")
    console.print(f"[bold]Duration:[/bold] {format_duration(seg.get('durationSec', 0))}")
    console.print(f"[bold]Time range:[/bold] {seg.get('startSec', 0):.1f}s — {seg.get('endSec', 0):.1f}s")

    cam = seg.get("cameraInfo")
    if cam:
        console.print(f"[bold]Camera:[/bold] fx={cam['fx']:.1f} fy={cam['fy']:.1f} "
                       f"ppx={cam['ppx']:.1f} ppy={cam['ppy']:.1f}")

    prefix = segment_r2_prefix(seg)
    console.print(f"[bold]R2 prefix:[/bold] {prefix}/")

    console.print("\n[bold]Files:[/bold]")
    for key in segment_file_list(seg):
        console.print(f"  {key}")


@ego_group.command("download")
@click.option("-o", "--output", default="./verlet-data", type=click.Path(), help="Output directory.")
@click.option("--task", default=None, help="Filter by station/task name.")
@click.option("--category", default=None, help="Filter by category.")
@click.option("--include", default=None, help="Comma-separated glob patterns for files to include.")
@click.option("--exclude", default=None, help="Comma-separated glob patterns for files to exclude.")
@click.option("--parallel", default=8, type=int, help="Number of concurrent downloads.")
@click.option("--dry-run", is_flag=True, help="Show download plan without downloading.")
def download_cmd(
    output: str,
    task: str | None,
    category: str | None,
    include: str | None,
    exclude: str | None,
    parallel: int,
    dry_run: bool,
):
    """Download EgoDex data segments."""
    catalog = fetch_catalog()
    segments = flatten_segments(catalog)
    segments = filter_segments(segments, task=task, category=category)

    if not segments:
        console.print("No segments found matching filters.")
        return

    include_pats = [p.strip() for p in include.split(",")] if include else None
    exclude_pats = [p.strip() for p in exclude.split(",")] if exclude else None

    # Build download plan: list of (r2_key, local_path)
    base_dir = Path(output) / "ego"
    file_plan: list[tuple[str, Path]] = []

    for seg in segments:
        keys = segment_file_list(seg, include=include_pats, exclude=exclude_pats)
        prefix = segment_r2_prefix(seg)
        for key in keys:
            # key is like "station-1/episode_042_seg5/hands.npz"
            rel = key  # already relative
            local_path = base_dir / rel
            file_plan.append((key, local_path))

    # Skip already-downloaded files (size check happens during download)
    existing = sum(1 for _, p in file_plan if p.exists())

    console.print(f"[bold]{len(segments)}[/bold] segments, "
                  f"[bold]{len(file_plan)}[/bold] files to download")
    if existing:
        console.print(f"  ({existing} files already exist locally — will verify sizes)")
    console.print(f"  Output: {base_dir.resolve()}")

    if dry_run:
        console.print("\n[bold]Download plan:[/bold]")
        for key, local in file_plan:
            status = "[green]exists[/]" if local.exists() else "[dim]pending[/]"
            console.print(f"  {status} {key}")
        return

    from verlet.ego.download import download_files

    ok, fail = asyncio.run(download_files(file_plan, parallel=parallel))

    console.print(f"\n[bold green]{ok}[/] downloaded", end="")
    if fail:
        console.print(f", [bold red]{fail}[/] failed", end="")
    console.print()
