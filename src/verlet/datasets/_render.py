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

from verlet.datasets._api import is_ego_row, resolve_modality
from verlet.display import format_bytes, format_duration


def dataset_list_table(items: list[dict[str, Any]]) -> Table:
    """Modality-aware list view — one row per dataset.

    The Modality column is always present (D-MOD1 unified catalog). Hours and
    bytes pass through ``format_bytes`` / a fixed-precision float so the table
    aligns under monospace fonts.

    Column-width discipline mirrors the episodes/segments tables: every
    short column is ``no_wrap=True`` so rows stay single-line; Title is
    the only flexible column (``ratio=1`` with ``expand=True`` on the
    Table) and absorbs whatever horizontal room is left. Variants/Tiers
    are dropped when no row in the result set populates either column —
    the showcase listing always returns them empty, so a showcase user
    never sees a wall of "—" cells.
    """
    show_variants = any(ds.get("available_variants") for ds in items)
    show_tiers = any(ds.get("data_tiers") for ds in items)
    show_bytes = any(ds.get("total_bytes") for ds in items)
    table = Table(title="Datasets", expand=True)
    table.add_column(
        "Slug", style="cyan", no_wrap=True, overflow="ellipsis", max_width=30
    )
    table.add_column("Modality", no_wrap=True)
    table.add_column("Title", no_wrap=True, overflow="ellipsis", ratio=1)
    table.add_column("Episodes", justify="right", no_wrap=True)
    table.add_column("Hours", justify="right", no_wrap=True)
    if show_bytes:
        table.add_column("Size", justify="right", no_wrap=True)
    if show_variants:
        table.add_column("Variants", no_wrap=True, overflow="ellipsis")
    if show_tiers:
        table.add_column("Tiers", no_wrap=True, overflow="ellipsis")
    for ds in items:
        modality = "ego" if is_ego_row(ds) else "teleop"
        hours_val = ds.get("total_hours")
        hours_str = (
            f"{hours_val:.1f}h"
            if isinstance(hours_val, (int, float))
            else "—"
        )
        row = [
            ds["slug"],
            modality,
            ds.get("title") or "—",
            str(ds.get("episode_count") or 0),
            hours_str,
        ]
        if show_bytes:
            row.append(format_bytes(ds.get("total_bytes")))
        if show_variants:
            row.append(", ".join(ds.get("available_variants") or []) or "—")
        if show_tiers:
            row.append(", ".join(ds.get("data_tiers") or []) or "—")
        table.add_row(*row)
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

    Modality detection goes through ``resolve_modality`` so an ego dataset
    is labelled ``ego`` even when the live backend response omits the
    explicit ``modality`` field (see the docstring on ``resolve_modality``).
    For ego rows, ``segment_count`` is preferred but ``episode_count`` is
    used as a fallback when the wire response doesn't carry segments —
    showcase listings call them "episodes" uniformly today.
    """
    modality = resolve_modality(detail)
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
    if modality == "ego":
        # segment_count is the precise figure; the showcase listing
        # omits it (informational), so fall back to episode_count which
        # the listing always populates.
        count = detail.get("segment_count")
        if count is None:
            count = detail.get("episode_count") or 0
        meta.add_row("segments", str(count))
    else:
        meta.add_row("episodes", str(detail.get("episode_count") or 0))
    # ``variants_available`` may be missing on the wire even when the
    # caller's grants clearly cover raw/processed; derive it from the
    # grants in that case so the "variants available" row never
    # contradicts the grants table below.
    variants = detail.get("variants_available") or sorted(
        {
            g.get("variant")
            for g in (detail.get("effective_grants") or [])
            if g.get("variant")
        }
    )
    meta.add_row("variants available", ", ".join(variants) or "—")

    grants = Table(title="Your access (grants)")
    grants.add_column("Variant")
    grants.add_column("Scope")
    grants.add_column("Expires")
    grants.add_column("Quota remaining")
    for g in detail.get("effective_grants") or []:
        quota = g.get("quota_remaining")
        scope = g.get("scope") or "—"
        if quota:
            qparts = []
            if quota.get("bytes") is not None:
                qparts.append(format_bytes(quota["bytes"]))
            if quota.get("episodes") is not None:
                qparts.append(f"{quota['episodes']} units")
            quota_str = ", ".join(qparts) or "unlimited"
        elif scope == "samples":
            # Backend always returns quota_remaining=None for samples scope —
            # they're a free preview and don't decrement the paid budget.
            # Spell that out instead of "unlimited", which collides with
            # the meaning we use for truly uncapped full-scope grants.
            quota_str = "free preview"
        else:
            quota_str = "unlimited"
        grants.add_row(
            g.get("variant") or "—",
            scope,
            g.get("expires_at") or "—",
            quota_str,
        )
    return (meta, grants)


def library_table(datasets: list[dict[str, Any]]) -> Table:
    """Owned single-dataset purchases view for ``verlet datasets library`` (G-P1).

    One row per purchased catalog dataset. ``available_formats`` is inlined so
    a buyer can see which formats are download-ready without an extra round
    trip; ``purchased_at`` is trimmed to the date.
    """
    table = Table(title="Your Library — Purchased Datasets")
    table.add_column("Slug", style="cyan", no_wrap=True)
    table.add_column("Title")
    table.add_column("Variant")
    table.add_column("Episodes", justify="right")
    table.add_column("Hours", justify="right")
    table.add_column("Formats")
    table.add_column("Status")
    table.add_column("Purchased")
    for d in datasets:
        hours_val = d.get("total_hours")
        hours_str = (
            f"{float(hours_val):.1f}"
            if isinstance(hours_val, (int, float))
            else "—"
        )
        purchased = d.get("purchased_at")
        purchased_str = (
            purchased[:10] if isinstance(purchased, str) and purchased else "—"
        )
        table.add_row(
            d.get("dataset_slug", ""),
            d.get("dataset_title") or "—",
            d.get("variant") or "processed",
            str(d.get("episode_count") or 0),
            hours_str,
            ", ".join(d.get("available_formats") or []) or "—",
            d.get("status") or "—",
            purchased_str,
        )
    return table


def library_bundles_table(bundles: list[dict[str, Any]]) -> Table:
    """Owned bundle purchases view for ``verlet datasets library`` (G-P1).

    Bundles render as a separate section below the per-dataset table. A
    commercial bundle has no expiry (``expires_at`` is null) → ``—``.
    """
    table = Table(title="Your Library — Bundles")
    table.add_column("Bundle", style="cyan", no_wrap=True)
    table.add_column("Name")
    table.add_column("License")
    table.add_column("Datasets", justify="right")
    table.add_column("Hours", justify="right")
    table.add_column("Expires")
    for b in bundles:
        hours_val = b.get("total_hours")
        hours_str = (
            f"{float(hours_val):.1f}"
            if isinstance(hours_val, (int, float))
            else "—"
        )
        table.add_row(
            b.get("bundle_slug", ""),
            b.get("bundle_name") or "—",
            b.get("license_tier") or "—",
            str(b.get("dataset_count") or 0),
            hours_str,
            b.get("expires_at") or "—",
        )
    return table


def _short_id(value: Any) -> str:
    """Truncate a UUID to its first 8 chars + ellipsis for compact tables.

    The browse/segment listings cross-reference rows by ``dataset_index``,
    not UUID — ``--episode-ids`` / ``--segment-ids`` take the integer
    Index. The full UUID is informational only, so we elide it to keep the
    other columns readable at 80 cols. Full UUIDs are still in ``--json``.
    """
    s = str(value) if value is not None else ""
    if len(s) <= 12:
        return s or "—"
    return f"{s[:8]}…"


def episodes_table(items: list[dict[str, Any]]) -> Table:
    """Per-dataset episode listing (G-P7).

    The ``Index`` column is the ``dataset_index`` — the integer
    ``verlet datasets download --episode-ids`` expects, so a user can browse
    here and feed the indices straight into a selective download. ``ID``
    is shown elided (first 8 chars + ellipsis); the full UUID is in ``--json``.
    """
    table = Table(title="Episodes")
    table.add_column("Index", justify="right", no_wrap=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Duration", justify="right", no_wrap=True)
    table.add_column("Frames", justify="right", no_wrap=True)
    table.add_column("QC", no_wrap=True)
    table.add_column("Sample", no_wrap=True)
    for ep in items:
        idx = ep.get("dataset_index")
        table.add_row(
            str(idx) if idx is not None else "—",
            _short_id(ep.get("id")),
            format_duration(ep.get("duration_secs") or 0),
            str(ep.get("frame_count") or 0),
            ep.get("qc_status") or "—",
            "yes" if ep.get("is_free_sample") else "—",
        )
    return table


def segments_table(items: list[dict[str, Any]]) -> Table:
    """Per-dataset segment listing for ego datasets (G-P7).

    ``Index`` is the ``dataset_index`` consumed by
    ``verlet datasets download --segment-ids``. ``Name`` is the only
    free-text column and is allowed to wrap; everything else stays on one
    line so the row count is predictable. Full UUIDs are in ``--json``.
    """
    table = Table(title="Segments", expand=True)
    table.add_column("Index", justify="right", no_wrap=True)
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Name", no_wrap=True, overflow="ellipsis", ratio=1)
    table.add_column("Duration", justify="right", no_wrap=True)
    table.add_column("Category", no_wrap=True, overflow="ellipsis", max_width=14)
    table.add_column("Hand", justify="right", no_wrap=True)
    table.add_column("Depth", no_wrap=True)
    table.add_column("Sample", no_wrap=True)
    for s in items:
        idx = s.get("dataset_index")
        cov = s.get("hand_coverage")
        cov_str = f"{cov * 100:.0f}%" if isinstance(cov, (int, float)) else "—"
        table.add_row(
            str(idx) if idx is not None else "—",
            _short_id(s.get("id")),
            s.get("name") or "—",
            format_duration(s.get("duration_s") or 0),
            s.get("category") or "—",
            cov_str,
            "yes" if s.get("has_depth") else "—",
            "yes" if s.get("is_free_sample") else "—",
        )
    return table


def page_footer(body: dict[str, Any]) -> str | None:
    """Footer for a ``PaginatedResponse`` body when more pages remain."""
    total = body.get("total") or 0
    page = body.get("page") or 1
    items = body.get("items") or []
    page_size = body.get("page_size") or len(items) or 1
    seen = (page - 1) * page_size + len(items)
    if seen >= total:
        return None
    return (
        f"Page {page} — showing {len(items)} of {total}. "
        "Use --page to see more, or --json for the full payload."
    )


def qc_distributions_table(body: dict[str, Any]) -> Table:
    """Per-check QC-metric distributions for ``verlet datasets quality`` (G-P6)."""
    dists = body.get("distributions") or {}
    table = Table(title="QC Check Distributions")
    table.add_column("Check", style="cyan")
    table.add_column("Mean", justify="right")
    table.add_column("Std", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("N", justify="right")
    for name, d in sorted(dists.items()):
        table.add_row(
            name,
            f"{d.get('mean', 0):.3f}",
            f"{d.get('std', 0):.3f}",
            f"{d.get('min', 0):.3f}",
            f"{d.get('max', 0):.3f}",
            str(len(d.get("values") or [])),
        )
    return table


_ANALYTICS_METRIC_KEYS = (
    "duration",
    "hz_variance",
    "alignment_score",
    "mean_jerk",
    "mean_action_norm",
    "dropped_frames",
    "idle_frame_count",
    "duration_zscore",
    "max_jerk",
    "max_action_norm",
)


def analytics_view(body: dict[str, Any]) -> tuple[Table, Table, Table]:
    """Three stacked tables for ``verlet datasets analytics`` (G-P6).

    Returns (summary, qc-status counts, metric distributions); the caller
    prints them with separators, mirroring ``dataset_info_text``.
    """
    meta = Table(title="Dataset Analytics", show_header=False)
    meta.add_column("Field", style="bold")
    meta.add_column("Value")
    meta.add_row("episodes", str(body.get("episode_count") or 0))
    meta.add_row("episodes with QC", str(body.get("episodes_with_qc") or 0))

    qc_table = Table(title="QC status")
    qc_table.add_column("Status")
    qc_table.add_column("Episodes", justify="right")
    for status, count in sorted((body.get("qc_status_counts") or {}).items()):
        qc_table.add_row(status, str(count))

    metrics = Table(title="Metric distributions")
    metrics.add_column("Metric", style="cyan")
    metrics.add_column("Mean", justify="right")
    metrics.add_column("Std", justify="right")
    metrics.add_column("Min", justify="right")
    metrics.add_column("Max", justify="right")
    metrics.add_column("Median", justify="right")
    metrics.add_column("CV", justify="right")
    for key in _ANALYTICS_METRIC_KEYS:
        dist = body.get(key)
        if not dist:
            continue
        stats = dist.get("stats") or {}
        metrics.add_row(
            key,
            f"{stats.get('mean', 0):.3f}",
            f"{stats.get('std', 0):.3f}",
            f"{stats.get('min', 0):.3f}",
            f"{stats.get('max', 0):.3f}",
            f"{stats.get('median', 0):.3f}",
            f"{stats.get('cv', 0):.3f}",
        )
    return (meta, qc_table, metrics)


def conversions_table(items: list[dict[str, Any]]) -> Table:
    """Conversion-job listing for ``verlet datasets jobs`` (G-P5)."""
    table = Table(title="Conversion Jobs")
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("Target format")
    table.add_column("Status")
    table.add_column("Progress", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Created")
    for c in items:
        total_ep = c.get("total_episodes") or 0
        progress = (
            f"{c.get('current_episode') or 0}/{total_ep}" if total_ep else "—"
        )
        created = c.get("created_at")
        created_str = (
            created[:10] if isinstance(created, str) and created else "—"
        )
        table.add_row(
            str(c.get("id") or "—"),
            c.get("target_format") or "—",
            c.get("status") or "—",
            progress,
            format_bytes(c.get("total_size_bytes")),
            created_str,
        )
    return table


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
