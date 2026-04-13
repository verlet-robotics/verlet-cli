"""Ego CLI commands."""
import asyncio
import json
from pathlib import Path

import click

from verlet.display import console, ego_segment_table, format_duration


@click.group("ego")
def ego_group():
    """Egocentric hand tracking data."""
    pass


def _resolve_segment(segments: list[dict], ident: str) -> dict:
    """Resolve a full UUID or short prefix to a single segment dict.

    Users see truncated 8-char IDs in `verlet ego list`, so `info` and other
    commands accept any prefix as long as it's unambiguous.
    """
    matches = [s for s in segments if s["id"].startswith(ident)]
    if not matches:
        raise click.ClickException(f"Segment '{ident}' not found.")
    if len(matches) > 1:
        ids = ", ".join(m["id"][:12] for m in matches[:5])
        raise click.ClickException(
            f"Segment prefix '{ident}' is ambiguous ({len(matches)} matches: {ids}...). "
            "Use a longer prefix."
        )
    return matches[0]


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
    """Show details for a specific ego segment.

    SEGMENT_ID may be a full UUID or any unambiguous prefix (e.g. the 8-char
    ID shown by `verlet ego list`).
    """
    from verlet.ego.catalog import fetch_ego_catalog

    catalog = asyncio.run(fetch_ego_catalog())
    segments = catalog.get("segments", [])

    seg = _resolve_segment(segments, segment_id)

    console.print(f"\n[bold]Segment:[/bold]  {seg['id']}")
    console.print(f"[bold]Category:[/bold] {seg.get('category', '---')}")
    console.print(f"[bold]Subcat:[/bold]   {seg.get('subcategory', '---')}")
    console.print(f"[bold]Duration:[/bold] {format_duration(seg.get('duration_s', 0))}")
    if seg.get("description"):
        console.print(f"[bold]Desc:[/bold]     {seg['description']}")

    cam = seg.get("camera_info")
    if cam and isinstance(cam, dict):
        console.print(
            f"[bold]Camera:[/bold]   fx={cam.get('fx', '?')} fy={cam.get('fy', '?')} "
            f"ppx={cam.get('ppx', '?')} ppy={cam.get('ppy', '?')}"
        )

    assets = []
    if seg.get("has_training_bundle"):
        assets.append("training-bundle")
    if seg.get("has_video"):
        assets.append("video")
    if seg.get("has_depth"):
        assets.append("depth")
    if seg.get("has_poses") or seg.get("has_egodex"):
        assets.append("poses")
    if seg.get("has_overlay"):
        assets.append("overlay")
    if seg.get("has_rrd"):
        assets.append("rrd")
    console.print(f"[bold]Assets:[/bold]   {', '.join(assets) if assets else 'none'}")


@ego_group.command("download")
@click.option("-o", "--output", default="./verlet-data", help="Output directory")
@click.option("--category", default=None, help="Filter by category")
@click.option(
    "--training",
    is_flag=True,
    help=(
        "Download the training bundle (video + depth + poses + camera + "
        "metadata) per segment. Recommended for model training."
    ),
)
@click.option(
    "--asset",
    "asset_types",
    multiple=True,
    type=click.Choice(
        ["video", "depth", "poses", "overlay", "rrd", "egodex", "clean"]
    ),
    help=(
        "Legacy asset types. `overlay`/`rrd` are visualization-only "
        "(not training data); `clean` returns a raw .egorec binary. Prefer "
        "--training for model training."
    ),
)
@click.option("--parallel", default=8, help="Max concurrent downloads")
@click.option(
    "--force",
    is_flag=True,
    help="Re-download files even when they already exist on disk",
)
@click.option("--dry-run", is_flag=True, help="Show what would be downloaded")
def ego_download(
    output: str,
    category: str | None,
    training: bool,
    asset_types: tuple[str, ...],
    parallel: int,
    force: bool,
    dry_run: bool,
):
    """Download ego segment assets.

    For training workloads use `--training`: one manifest call per segment
    returns all training URLs at once, files land under
    `<output>/ego/<segment_id>/{video.mp4,depth.mkv,poses.hdf5,camera.json,
    metadata.json}`. Re-runs are resumable (skip files already on disk,
    pass --force to override).
    """
    if training and asset_types:
        raise click.UsageError(
            "Use either --training or --asset, not both. --training "
            "downloads the full training bundle; --asset downloads specific "
            "legacy assets."
        )

    if training:
        _run_training_download(
            output=output,
            category=category,
            parallel=parallel,
            force=force,
            dry_run=dry_run,
        )
        return

    if not asset_types:
        console.print(
            "[yellow]Hint:[/yellow] For model training use "
            "[bold]--training[/bold]. Defaulting to legacy `--asset overlay` "
            "(visualization only — hand skeleton is burned into the RGB "
            "pixels)."
        )
        asset_types = ("overlay",)
    else:
        if "overlay" in asset_types or "rrd" in asset_types:
            console.print(
                "[yellow]Warning:[/yellow] `overlay` and `rrd` are "
                "visualization assets, not training data. For training use "
                "[bold]--training[/bold]."
            )
        if "clean" in asset_types:
            console.print(
                "[yellow]Warning:[/yellow] `--asset clean` returns a raw "
                ".egorec binary clip, not a playable video. For clean RGB "
                "training video use [bold]--training[/bold] (produces "
                "`video.mp4`)."
            )

    _run_legacy_download(
        output=output,
        category=category,
        asset_types=asset_types,
        parallel=parallel,
        force=force,
        dry_run=dry_run,
    )


def _run_legacy_download(
    output: str,
    category: str | None,
    asset_types: tuple[str, ...],
    parallel: int,
    force: bool,
    dry_run: bool,
) -> None:
    from verlet.download import download_files
    from verlet.ego.catalog import fetch_ego_catalog, presign_ego_asset
    from verlet.license import (
        check_license_accepted,
        prompt_license_acceptance,
        write_license_file,
    )

    if not dry_run and not check_license_accepted():
        if not prompt_license_acceptance():
            console.print("[dim]Download cancelled.[/dim]")
            return

    catalog = asyncio.run(fetch_ego_catalog(category))
    segments = catalog.get("segments", [])

    if not segments:
        console.print("[dim]No matching ego segments found.[/dim]")
        return

    plan: list[tuple[str, str]] = []
    for seg in segments:
        for asset in asset_types:
            if seg.get(f"has_{asset}", False):
                plan.append((seg["id"], asset))

    if not plan:
        console.print("[dim]No downloadable assets match your filters.[/dim]")
        return

    console.print(
        f"[bold]{len(segments)}[/bold] segments, "
        f"[bold]{len(plan)}[/bold] assets to download "
        f"({', '.join(asset_types)})"
    )

    dest_root = Path(output) / "ego"

    if dry_run:
        console.print(f"\n[bold]Would download to {dest_root}:[/bold]\n")
        for seg_id, asset in plan[:30]:
            console.print(f"  {seg_id[:8]}  {asset}")
        if len(plan) > 30:
            console.print(f"  ... and {len(plan) - 30} more")
        return

    keys = [f"{seg_id}/{asset}" for seg_id, asset in plan]
    _plan_lookup = {f"{seg_id}/{asset}": (seg_id, asset) for seg_id, asset in plan}

    async def presign(key: str) -> str:
        seg_id, asset = _plan_lookup[key]
        return await presign_ego_asset(seg_id, asset)

    result = asyncio.run(
        download_files(
            keys=keys,
            dest_dir=dest_root,
            presign_fn=presign,
            parallel=parallel,
            dry_run=False,
            skip_existing=not force,
        )
    )

    if result.downloaded > 0:
        write_license_file(dest_root)

    _print_result(result, dest_root)


def _run_training_download(
    output: str,
    category: str | None,
    parallel: int,
    force: bool,
    dry_run: bool,
) -> None:
    """Training-bundle flow: one manifest per segment, fixed filenames.

    Per segment, fetches /training-bundle (returns presigned URLs for video
    + depth + poses, plus inline camera_info and metadata), writes
    camera.json and metadata.json locally from the manifest, then downloads
    the three binary files under `<output>/ego/<segment_id>/`.
    """
    import httpx

    from verlet.download import DownloadPlanItem, download_resolved
    from verlet.ego.catalog import _auth_headers, fetch_ego_catalog, fetch_training_bundle
    from verlet.license import (
        check_license_accepted,
        prompt_license_acceptance,
        write_license_file,
    )

    if not dry_run and not check_license_accepted():
        if not prompt_license_acceptance():
            console.print("[dim]Download cancelled.[/dim]")
            return

    catalog = asyncio.run(fetch_ego_catalog(category))
    all_segments = catalog.get("segments", [])
    if not all_segments:
        console.print("[dim]No matching ego segments found.[/dim]")
        return

    ready = [s for s in all_segments if s.get("has_training_bundle")]
    pending_depth = sum(1 for s in ready if not s.get("has_depth"))
    pending_bundle = len(all_segments) - len(ready)

    if not ready:
        console.print(
            "[yellow]No segments with a training bundle are ready yet.[/yellow] "
            "The backend is still backfilling video + poses metadata for "
            f"{pending_bundle} segments. Try again shortly."
        )
        return

    console.print(
        f"[bold]Training bundle:[/bold] video + depth + poses + metadata"
    )
    console.print(
        f"  {len(ready)} segments ready"
        + (f", {pending_bundle} pending backend backfill" if pending_bundle else "")
    )
    if pending_depth:
        console.print(
            f"  [dim]{pending_depth} segments missing depth "
            f"(Phase B backfill still running — re-run later to pick up "
            f"depth as it lands)[/dim]"
        )

    dest_root = Path(output) / "ego"

    if dry_run:
        console.print(f"\n[bold]Would download to {dest_root}:[/bold]\n")
        for seg in ready[:10]:
            sid = seg["id"]
            console.print(f"  {sid[:8]}/video.mp4")
            if seg.get("has_depth"):
                console.print(f"  {sid[:8]}/depth.mkv")
            console.print(f"  {sid[:8]}/poses.hdf5")
            console.print(f"  {sid[:8]}/camera.json  [dim](synthesized)[/dim]")
            console.print(f"  {sid[:8]}/metadata.json [dim](synthesized)[/dim]")
        if len(ready) > 10:
            console.print(f"  ... and {len(ready) - 10} more segments")
        return

    # Fetch all training-bundle manifests in parallel. 2-3 round-trips
    # amortize across 2969 segments — a single shared httpx client keeps
    # the TCP/TLS handshake out of the hot loop.
    items: list[DownloadPlanItem] = []
    synthesized = 0

    async def gather_bundles() -> list[dict]:
        sem = asyncio.Semaphore(16)
        async with httpx.AsyncClient() as client:
            async def fetch_one(segment_id: str) -> dict | None:
                async with sem:
                    try:
                        return await fetch_training_bundle(client, segment_id)
                    except click.ClickException as e:
                        console.print(
                            f"  [yellow]skip[/yellow] {segment_id[:8]}: {e.message}"
                        )
                        return None

            console.print(
                f"Fetching training-bundle manifests for {len(ready)} segments..."
            )
            return [
                b
                for b in await asyncio.gather(
                    *(fetch_one(s["id"]) for s in ready)
                )
                if b is not None
            ]

    bundles = asyncio.run(gather_bundles())

    for bundle in bundles:
        sid = bundle["segment_id"]
        seg_dir = dest_root / sid
        seg_dir.mkdir(parents=True, exist_ok=True)

        # Synthesize camera.json + metadata.json client-side — no network.
        camera_path = seg_dir / "camera.json"
        metadata_path = seg_dir / "metadata.json"
        if force or not camera_path.exists():
            camera_path.write_text(
                json.dumps(bundle.get("camera_info") or {}, indent=2)
            )
            synthesized += 1
        if force or not metadata_path.exists():
            metadata_path.write_text(
                json.dumps(bundle.get("metadata") or {}, indent=2)
            )
            synthesized += 1

        if bundle.get("video_url"):
            items.append(
                DownloadPlanItem(bundle["video_url"], seg_dir / "video.mp4")
            )
        if bundle.get("depth_url"):
            items.append(
                DownloadPlanItem(bundle["depth_url"], seg_dir / "depth.mkv")
            )
        if bundle.get("poses_url"):
            items.append(
                DownloadPlanItem(bundle["poses_url"], seg_dir / "poses.hdf5")
            )

    console.print(
        f"[bold]{len(bundles)}[/bold] segments, "
        f"[bold]{len(items)}[/bold] binary files to download"
    )

    result = asyncio.run(
        download_resolved(
            items=items,
            parallel=parallel,
            skip_existing=not force,
        )
    )

    if result.downloaded > 0 or synthesized > 0:
        write_license_file(dest_root)

    _print_result(result, dest_root, extra_synthesized=synthesized)


def _print_result(result, dest_root: Path, extra_synthesized: int = 0) -> None:
    parts = []
    if result.downloaded:
        parts.append(f"[green]{result.downloaded}[/green] downloaded")
    if result.skipped:
        parts.append(f"[dim]{result.skipped} already on disk[/dim]")
    if result.failed:
        parts.append(f"[red]{result.failed} failed[/red]")
    if extra_synthesized:
        parts.append(f"[dim]{extra_synthesized} synthesized[/dim]")
    if not parts:
        parts.append("[dim]no files to download[/dim]")
    console.print(f"\n{', '.join(parts)} -> {dest_root}")
