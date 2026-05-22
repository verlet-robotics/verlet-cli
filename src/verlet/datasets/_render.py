"""Rich table builders for ``verlet datasets list|info`` output.

All functions take dict payloads (deserialized JSON from the platform
endpoints) and return Rich ``Table`` objects (or strings for the JSON path /
truncation footer). The caller (commands.py, Plan 03) prints to
``verlet.display.console``.

Modality detection delegates to ``verlet.datasets._api.is_ego_row`` — single
source of truth, no duplicate heuristic in this module. Byte and duration
formatters are reused from ``verlet.display`` so output stays consistent with
the existing teleop/ego renderers.
"""
from __future__ import annotations

import json
from typing import Any

from rich.table import Table

from verlet.datasets._api import is_ego_row
from verlet.display import format_bytes, format_duration


def dataset_list_table(items: list[dict[str, Any]]) -> Table:
    """Modality-aware list view — one row per dataset.

    The Modality column is always present (D-MOD1 unified catalog). Hours and
    bytes pass through ``format_bytes`` / a fixed-precision float so the table
    aligns under monospace fonts.
    """
    table = Table(title="Datasets")
    table.add_column("Slug", style="cyan", no_wrap=True)
    table.add_column("Modality")
    table.add_column("Title")
    table.add_column("Episodes", justify="right")
    table.add_column("Hours", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Variants")
    table.add_column("Tiers")
    for ds in items:
        modality = "ego" if is_ego_row(ds) else "teleop"
        hours_val = ds.get("total_hours")
        hours_str = (
            f"{hours_val:.1f}h"
            if isinstance(hours_val, (int, float))
            else "—"
        )
        table.add_row(
            ds["slug"],
            modality,
            ds.get("title") or "—",
            str(ds.get("episode_count") or 0),
            hours_str,
            format_bytes(ds.get("total_bytes")),
            ", ".join(ds.get("available_variants") or []) or "—",
            ", ".join(ds.get("data_tiers") or []) or "—",
        )
    return table


def dataset_info_text(detail: dict[str, Any]) -> tuple[Table, Table]:
    """Two stacked tables: metadata + per-camera (arm) / segment-summary (ego).

    Caller prints both with a separator line in between (per
    29-CONTEXT.md §Claude's Discretion → ``info`` text layout).
    """
    modality = "ego" if is_ego_row(detail) else "teleop"

    meta = Table(
        title=f"{detail.get('title') or detail['slug']} ({modality})",
        show_header=False,
    )
    meta.add_column("Field", style="bold")
    meta.add_column("Value")
    meta.add_row("slug", detail["slug"])
    meta.add_row("modality", modality)
    meta.add_row("episodes", str(detail.get("episode_count") or 0))
    hours_val = detail.get("total_hours")
    meta.add_row(
        "hours",
        f"{hours_val:.1f}h" if isinstance(hours_val, (int, float)) else "—",
    )
    meta.add_row("size", format_bytes(detail.get("total_bytes")))
    meta.add_row(
        "variants",
        ", ".join(detail.get("available_variants") or []) or "—",
    )
    meta.add_row(
        "tiers",
        ", ".join(detail.get("data_tiers") or []) or "—",
    )
    meta.add_row(
        "license",
        detail.get("license_tier") or detail.get("license") or "—",
    )

    # Pricing — both tiers may be present on ego rows.
    price_proc = detail.get("price_per_hour_cents")
    price_raw = detail.get("price_raw_per_hour_cents")
    currency = detail.get("currency") or "USD"
    if price_proc is not None:
        meta.add_row(
            "price (processed)", f"{price_proc / 100:.2f} {currency}/hour"
        )
    if price_raw is not None:
        meta.add_row("price (raw)", f"{price_raw / 100:.2f} {currency}/hour")

    # Bottom table: arm episodes vs ego segments. Capped at 50 rows so noisy
    # CI logs stay readable; --json gives the full payload for scripting.
    if modality == "ego":
        bottom = Table(title="Segments")
        bottom.add_column("Segment")
        bottom.add_column("Category")
        bottom.add_column("Subcategory")
        bottom.add_column("Hand QC")
        for seg in (detail.get("segments") or [])[:50]:
            bottom.add_row(
                str(seg.get("id") or "—"),
                seg.get("category") or "—",
                seg.get("subcategory") or "—",
                seg.get("hand_qc_status") or "—",
            )
    else:
        bottom = Table(title="Episodes (per camera)")
        bottom.add_column("Episode")
        bottom.add_column("Cameras")
        bottom.add_column("Duration")
        bottom.add_column("Frames")
        for ep in (detail.get("episodes") or [])[:50]:
            bottom.add_row(
                str(ep.get("id") or "—"),
                ", ".join(ep.get("cameras") or []),
                format_duration(ep.get("duration_sec") or 0),
                str(ep.get("frame_count") or 0),
            )
    return (meta, bottom)


def showcase_info_text(detail: dict[str, Any]) -> tuple[Table, Table]:
    """Two stacked tables for ``verlet datasets info`` under a showcase code.

    Renders dataset-level metadata + the caller's ``effective_grants`` (what
    variant/scope/quota the access code is entitled to). Deliberately prints
    NO per-segment rows — internal segment IDs are never surfaced to showcase
    clients; only a segment count.
    """
    modality = detail.get("modality") or "teleop"
    meta = Table(
        title=f"{detail.get('title') or detail['slug']} ({modality})",
        show_header=False,
    )
    meta.add_column("Field", style="bold")
    meta.add_column("Value")
    meta.add_row("slug", detail["slug"])
    meta.add_row("modality", modality)
    if detail.get("description"):
        meta.add_row("description", detail["description"])
    if detail.get("task_type"):
        meta.add_row("task type", detail["task_type"])
    if detail.get("robot_embodiment"):
        meta.add_row("robot", detail["robot_embodiment"])
    hours_val = detail.get("total_hours")
    meta.add_row(
        "hours",
        f"{hours_val:.1f}h" if isinstance(hours_val, (int, float)) else "—",
    )
    if modality == "ego" and detail.get("segment_count") is not None:
        meta.add_row("segments", str(detail["segment_count"]))
    else:
        meta.add_row("episodes", str(detail.get("episode_count") or 0))
    variants = detail.get("variants_available") or []
    meta.add_row("variants available", ", ".join(variants) or "—")

    grants = Table(title="Your access (grants)")
    grants.add_column("Variant")
    grants.add_column("Scope")
    grants.add_column("Expires")
    grants.add_column("Quota remaining")
    for g in detail.get("effective_grants") or []:
        quota = g.get("quota_remaining")
        if quota:
            qparts = []
            if quota.get("bytes") is not None:
                qparts.append(format_bytes(quota["bytes"]))
            if quota.get("episodes") is not None:
                qparts.append(f"{quota['episodes']} units")
            quota_str = ", ".join(qparts) or "unlimited"
        else:
            quota_str = "unlimited"
        grants.add_row(
            g.get("variant") or "—",
            g.get("scope") or "—",
            g.get("expires_at") or "—",
            quota_str,
        )
    return (meta, grants)


def dataset_info_json(detail: dict[str, Any]) -> str:
    """Direct CatalogDatasetDetail dump — no client-side reshape (D-CONTEXT)."""
    return json.dumps(detail, indent=2, default=str)


def list_truncation_footer(total: int, returned: int) -> str | None:
    """Footer string per D-FL4 when results are truncated.

    Returns ``None`` when ``returned >= total`` (no footer needed).
    """
    if returned >= total:
        return None
    return (
        f"Showing {returned} of {total} — narrow with filters or use --json "
        "for the full list."
    )
