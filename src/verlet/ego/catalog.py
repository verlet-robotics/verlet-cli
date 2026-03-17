"""Fetch and filter the EgoDex catalog from the API."""

from __future__ import annotations

from fnmatch import fnmatch

import click
import httpx

from verlet.auth import get_api_base, get_auth_headers

# Known files per segment in the output bucket.
# Key format: {station}/{episode}_seg{id}/{filename}
SEGMENT_FILES = [
    "segment.egorec",
    "hands.npz",
    "overlay.mp4",
    "recording.rrd",
    "egodex/manipulation/0.hdf5",
    "egodex/manipulation/0.mp4",
]


def fetch_catalog() -> dict:
    """Fetch the full catalog from the API."""
    base = get_api_base()
    headers = get_auth_headers()
    try:
        resp = httpx.get(f"{base}/api/catalog", headers=headers, timeout=30)
    except httpx.RequestError as e:
        raise click.ClickException(f"Connection failed: {e}")

    if resp.status_code == 401:
        raise click.ClickException("Session expired. Run 'verlet login' to re-authenticate.")
    if resp.status_code != 200:
        raise click.ClickException(f"Catalog request failed ({resp.status_code}): {resp.text}")

    return resp.json()


def flatten_segments(catalog: dict) -> list[dict]:
    """Extract a flat list of segments from the catalog."""
    segments = []
    for cat in catalog.get("categories", []):
        for sub in cat.get("subcategories", []):
            for seg in sub.get("segments", []):
                segments.append(seg)
    return segments


def filter_segments(
    segments: list[dict],
    *,
    task: str | None = None,
    category: str | None = None,
) -> list[dict]:
    """Filter segments by task (station) and/or category."""
    out = segments
    if task:
        task_lower = task.lower()
        out = [s for s in out if s.get("station", "").lower() == task_lower]
    if category:
        cat_lower = category.lower()
        out = [s for s in out if s.get("category", "").lower() == cat_lower]
    return out


def filter_categories(
    categories: list[dict],
    *,
    task: str | None = None,
    category: str | None = None,
) -> list[dict]:
    """Filter category groups. Returns a new list with matching entries."""
    out = categories
    if category:
        cat_lower = category.lower()
        out = [c for c in out if c["category"].lower() == cat_lower]
    if task:
        # Filter segments within each category/subcategory by station
        task_lower = task.lower()
        filtered = []
        for cat in out:
            new_subs = []
            for sub in cat.get("subcategories", []):
                matching = [
                    s for s in sub.get("segments", [])
                    if s.get("station", "").lower() == task_lower
                ]
                if matching:
                    new_subs.append({
                        **sub,
                        "segments": matching,
                        "segmentCount": len(matching),
                        "totalDurationSec": sum(s.get("durationSec", 0) for s in matching),
                    })
            if new_subs:
                filtered.append({
                    **cat,
                    "subcategories": new_subs,
                    "segmentCount": sum(s["segmentCount"] for s in new_subs),
                    "totalDurationSec": sum(s["totalDurationSec"] for s in new_subs),
                })
        out = filtered
    return out


def segment_r2_prefix(seg: dict) -> str:
    """Build the output-bucket prefix for a segment.

    Segment IDs are like 'station-1__episode_042_seg5'.
    Output bucket layout: station-1/episode_042_seg5/
    """
    sid = seg["id"]
    parts = sid.split("__", 1)
    if len(parts) == 2:
        return f"{parts[0]}/{parts[1]}"
    return sid


def segment_file_list(
    seg: dict,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> list[str]:
    """Return list of R2 keys for a segment, after include/exclude filtering."""
    prefix = segment_r2_prefix(seg)
    keys = [f"{prefix}/{f}" for f in SEGMENT_FILES]

    if include:
        keys = [
            k for k in keys
            if any(fnmatch(k.rsplit("/", 1)[-1], pat) for pat in include)
        ]
    if exclude:
        keys = [
            k for k in keys
            if not any(fnmatch(k.rsplit("/", 1)[-1], pat) for pat in exclude)
        ]

    return keys
