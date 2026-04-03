"""Ego CLI commands."""
import asyncio
from pathlib import Path

import click

from verlet.display import console, ego_segment_table, format_duration


@click.group("ego")
def ego_group():
    """Egocentric hand tracking data."""
    pass


@ego_group.command("list")
@click.option("--category", default=None, help="Filter by category")
def ego_list(category: str | None):
    """List available ego segments."""
    from verlet.ego.catalog import fetch_ego_catalog

    catalog = asyncio.run(fetch_ego_catalog(category))
    segments = catalog.get("segments", [])

    if not segments:
        console.print("[dim]No segments found.[/dim]")
        return

    table = ego_segment_table(segments)
    console.print(table)

    total_dur = sum(s.get("duration_s", 0) for s in segments)
    console.print(f"\n[dim]{len(segments)} segment(s), {format_duration(total_dur)} total[/dim]")


@ego_group.command("info")
@click.argument("segment_id")
def ego_info(segment_id: str):
    """Show details for a specific ego segment."""
    from verlet.ego.catalog import fetch_ego_catalog

    catalog = asyncio.run(fetch_ego_catalog())
    segments = catalog.get("segments", [])

    seg = next((s for s in segments if s["id"] == segment_id), None)
    if not seg:
        raise click.ClickException(f"Segment '{segment_id}' not found.")

    console.print(f"\n[bold]Segment:[/bold]  {seg['id']}")
    console.print(f"[bold]Category:[/bold] {seg.get('category', '---')}")
    console.print(f"[bold]Subcat:[/bold]   {seg.get('subcategory', '---')}")
    console.print(f"[bold]Duration:[/bold] {format_duration(seg.get('duration_s', 0))}")
    if seg.get("description"):
        console.print(f"[bold]Desc:[/bold]     {seg['description']}")

    cam = seg.get("camera_info")
    if cam and isinstance(cam, dict):
        console.print(f"[bold]Camera:[/bold]   fx={cam.get('fx', '?')} fy={cam.get('fy', '?')} "
                       f"ppx={cam.get('ppx', '?')} ppy={cam.get('ppy', '?')}")

    assets = []
    if seg.get("has_overlay"):
        assets.append("overlay")
    if seg.get("has_rrd"):
        assets.append("rrd")
    if seg.get("has_egodex"):
        assets.append("egodex")
    console.print(f"[bold]Assets:[/bold]   {', '.join(assets) if assets else 'none'}")


@ego_group.command("download")
@click.option("-o", "--output", default="./verlet-data", help="Output directory")
@click.option("--category", default=None, help="Filter by category")
@click.option("--asset", "asset_types", multiple=True,
              type=click.Choice(["overlay", "rrd", "egodex", "clean"]),
              help="Asset types to download (default: overlay)")
@click.option("--parallel", default=8, help="Max concurrent downloads")
@click.option("--dry-run", is_flag=True, help="Show what would be downloaded")
def ego_download(
    output: str,
    category: str | None,
    asset_types: tuple[str, ...],
    parallel: int,
    dry_run: bool,
):
    """Download ego segment assets."""
    from verlet.ego.catalog import fetch_ego_catalog, presign_ego_asset
    from verlet.download import download_files
    from verlet.license import check_license_accepted, prompt_license_acceptance, write_license_file

    if not asset_types:
        asset_types = ("overlay",)

    if not dry_run and not check_license_accepted():
        if not prompt_license_acceptance():
            console.print("[dim]Download cancelled.[/dim]")
            return

    catalog = asyncio.run(fetch_ego_catalog(category))
    segments = catalog.get("segments", [])

    if not segments:
        console.print("[dim]No matching ego segments found.[/dim]")
        return

    # Build download plan: (key, asset_type) for segments that have the requested asset
    plan: list[tuple[str, str]] = []  # (segment_id, asset_type)
    for seg in segments:
        for asset in asset_types:
            flag = f"has_{asset}"
            if seg.get(flag, False):
                plan.append((seg["id"], asset))

    if not plan:
        console.print("[dim]No downloadable assets match your filters.[/dim]")
        return

    console.print(f"[bold]{len(segments)}[/bold] segments, "
                  f"[bold]{len(plan)}[/bold] assets to download "
                  f"({', '.join(asset_types)})")

    dest_root = Path(output) / "ego"

    if dry_run:
        console.print(f"\n[bold]Would download to {dest_root}:[/bold]\n")
        for seg_id, asset in plan[:30]:
            console.print(f"  {seg_id[:8]}  {asset}")
        if len(plan) > 30:
            console.print(f"  ... and {len(plan) - 30} more")
        return

    # Download using presign-per-file pattern
    # Build keys list where each "key" is "{segment_id}/{asset}.mp4" (logical)
    keys = [f"{seg_id}/{asset}" for seg_id, asset in plan]

    # Map logical keys to presign calls
    _plan_lookup = {f"{seg_id}/{asset}": (seg_id, asset) for seg_id, asset in plan}

    async def presign(key: str) -> str:
        seg_id, asset = _plan_lookup[key]
        return await presign_ego_asset(seg_id, asset)

    count = asyncio.run(
        download_files(
            keys=keys,
            dest_dir=dest_root,
            presign_fn=presign,
            parallel=parallel,
            dry_run=False,
        )
    )

    if count > 0:
        write_license_file(dest_root)

    console.print(f"\n[green]Downloaded {count} file(s) to {dest_root}[/green]")
