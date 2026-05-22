"""Shared download-manifest parsing for showcase access-code downloads.

Both ``verlet datasets download`` (showcase-credential path) and the
deprecated ``verlet pull`` consume the gated showcase download manifest
from ``GET /api/v1/showcase/datasets/{slug}/download``. The backend serves
two shapes discriminated by ``modality``:

  * ``teleop`` — ``episodes[]`` each with ``parquet_url`` / ``video_urls`` /
    ``meta_urls`` (lerobot-v2 layout).
  * ``ego``   — ``segments[]`` each with ``files[]`` (role/url/key).

``plan_items`` projects either shape into ``DownloadPlanItem`` pairs so the
caller never branches on modality.
"""
from __future__ import annotations

from pathlib import Path

import click

from verlet.download import DownloadPlanItem


def plan_items(slug: str, dest_root: Path, manifest: dict) -> list[DownloadPlanItem]:
    """Project a showcase download manifest into (url, local_path) pairs.

    Ego manifests lay out as ``<dest>/<slug>/segment_<index>/<file>``; teleop
    manifests as ``<dest>/<slug>/episode_<index>/…`` with dataset-global meta
    files de-duplicated under ``<slug>/meta/``.
    """
    dataset_dir = dest_root / slug

    # Ego: one directory per segment, files named from the R2 key basename.
    if manifest.get("modality") == "ego" or manifest.get("segments"):
        out: list[DownloadPlanItem] = []
        for seg in manifest.get("segments", []):
            idx = seg.get("dataset_index", 0)
            seg_dir = dataset_dir / f"segment_{idx:06d}"
            for f in seg.get("files", []):
                filename = f["key"].rstrip("/").rsplit("/", 1)[-1]
                out.append(
                    DownloadPlanItem(url=f["url"], local_path=seg_dir / filename)
                )
        return out

    # Teleop: one directory per episode; meta files are dataset-global.
    out = []
    seen_meta: set[str] = set()
    for ep in manifest.get("episodes", []):
        ep_idx = ep["episode_index"]
        ep_dir = dataset_dir / f"episode_{ep_idx:06d}"
        if ep.get("parquet_url"):
            out.append(
                DownloadPlanItem(
                    url=ep["parquet_url"],
                    local_path=ep_dir / f"episode_{ep_idx:06d}.parquet",
                )
            )
        for v in ep.get("video_urls", []):
            out.append(
                DownloadPlanItem(
                    url=v["url"],
                    local_path=ep_dir / "videos" / f"{v['camera']}.mp4",
                )
            )
        for m in ep.get("meta_urls", []):
            if m["filename"] in seen_meta:
                continue
            seen_meta.add(m["filename"])
            out.append(
                DownloadPlanItem(
                    url=m["url"],
                    local_path=dataset_dir / "meta" / m["filename"],
                )
            )
    return out


def print_plan(manifest: dict, items: list[DownloadPlanItem]) -> None:
    """Print a human-readable summary of a download manifest + its file plan."""
    click.echo(
        f"Dataset: {manifest.get('dataset_title')} "
        f"({manifest.get('dataset_slug')})"
    )
    click.echo(f"Format:  {manifest.get('format')}")
    click.echo(f"Variant: {manifest.get('variant')}, scope: {manifest.get('scope')}")
    if manifest.get("modality") == "ego" or manifest.get("segments"):
        unit = f"Segments: {len(manifest.get('segments', []))}"
    else:
        unit = f"Episodes: {len(manifest.get('episodes', []))}"
    click.echo(f"{unit}, files: {len(items)}")
    quota = manifest.get("quota_remaining")
    if quota:
        b = quota.get("bytes")
        e = quota.get("episodes")
        parts = []
        if b is not None:
            parts.append(f"{b} bytes remaining")
        if e is not None:
            parts.append(f"{e} units remaining")
        if parts:
            click.echo(f"Quota:   {', '.join(parts)}")
    click.echo("")
    for it in items[:10]:
        click.echo(f"  {it.local_path}")
    if len(items) > 10:
        click.echo(f"  … and {len(items) - 10} more")
