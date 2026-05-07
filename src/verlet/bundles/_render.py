"""Rich table builders for verlet bundles output.

One table builder per browse / list / info view. Caller (commands.py) prints
to ``verlet.display.console`` so all CLI tables go through one Rich Console
instance.
"""
from __future__ import annotations

from typing import Any

from rich.table import Table


def bundles_browse_table(items: list[dict[str, Any]]) -> Table:
    """Public research-bundle browse view (CLIBUNDLE-01).

    Columns (left → right): Slug | Name | Datasets | License | Citation.
    Citation is truncated to 60 chars so the table aligns under monospace.
    """
    table = Table(title="Public Research Bundles", show_lines=False)
    table.add_column("Slug", style="cyan", no_wrap=True)
    table.add_column("Name")
    table.add_column("Datasets", justify="right")
    table.add_column("License")
    table.add_column("Citation", overflow="fold")
    for item in items:
        citation = (item.get("citation") or "")[:60]
        table.add_row(
            item.get("slug", ""),
            item.get("name", ""),
            str(item.get("dataset_count", 0)),
            item.get("license", "") or "—",
            citation or "—",
        )
    return table


# ---------------------------------------------------------------------------
# Plan 30-08 — `verlet bundles list` (CLIBUNDLE-03) + `verlet bundles info`
# (CLIBUNDLE-04). The list view colors the status column (D-BUNDLE1); the info
# view splits the bundle header (Panel) from the embedded datasets table.
# ---------------------------------------------------------------------------

_NL = chr(10)

STATUS_STYLES: dict[str, str] = {
    "active": "green",
    "expired": "yellow",
    "revoked": "red",
}
"""Per-status Rich style for the bundle list `Status` column.

Drift between this dict and the server's ``BundleStatus`` literal would
silently render unstyled cells (no exception). Tests in
``tests/bundles/test_list.py::test_list_status_color_coding`` byte-assert
against this map.
"""


def bundles_list_table(items: list[dict[str, Any]]) -> Table:
    """Authenticated bundle list view (CLIBUNDLE-03).

    Columns (left -> right): Slug | Name | Kind | Datasets | Size | Hours
    | Expires | Status. The Status column is a Rich ``Text`` so we can
    color-code active/expired/revoked rows per D-BUNDLE1.
    """
    from rich.text import Text

    from verlet.display import format_bytes

    table = Table(title="Your Bundles", show_lines=False)
    table.add_column("Slug", style="cyan", no_wrap=True)
    table.add_column("Name")
    table.add_column("Kind")
    table.add_column("Datasets", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Hours", justify="right")
    table.add_column("Expires")
    table.add_column("Status")
    for item in items:
        status = item.get("status", "active") or "active"
        status_text = Text(status, style=STATUS_STYLES.get(status, ""))
        hours_value = item.get("total_hours") or 0
        try:
            hours_str = f"{float(hours_value):.1f}"
        except (TypeError, ValueError):
            hours_str = "0.0"
        table.add_row(
            item.get("bundle_slug", ""),
            item.get("bundle_name", ""),
            item.get("kind", ""),
            str(item.get("dataset_count", 0)),
            format_bytes(item.get("total_size_bytes", 0) or 0),
            hours_str,
            item.get("expires_at") or "—",
            status_text,
        )
    return table


def bundle_detail_view(bundle: dict):
    """Authenticated bundle detail view (CLIBUNDLE-04).

    Renders a Rich ``Group`` of (Panel(header), Table(datasets)). The header
    Panel surfaces slug, name, kind, license, expiry, and citation -- the
    citation row is conditional on ``kind == "research"`` per the must-have
    truth (purchased bundles do not carry a citation row even if the field
    is populated by the server). The datasets table inlines the per-dataset
    ``available_formats`` list so a researcher can see at a glance whether
    a bundle's datasets ship in lerobot-v2 / hdf5 / etc. before reaching
    for ``verlet bundles download``.
    """
    from rich.console import Group
    from rich.panel import Panel

    from verlet.display import format_bytes

    kind = bundle.get("kind", "") or ""
    header_lines = [
        f"[bold cyan]{bundle.get('bundle_slug', '')}[/bold cyan] - "
        f"{bundle.get('bundle_name', '')}",
        f"[dim]kind:[/dim] {kind}    "
        f"[dim]license:[/dim] {bundle.get('license', '') or '-'}",
        f"[dim]expires:[/dim] {bundle.get('expires_at') or '-'}",
    ]
    if kind == "research" and bundle.get("citation"):
        header_lines.append(f"[dim]citation:[/dim] {bundle['citation']}")

    ds_table = Table(title="Datasets in bundle", show_lines=False)
    ds_table.add_column("Slug", style="cyan", no_wrap=True)
    ds_table.add_column("Name")
    ds_table.add_column("Episodes", justify="right")
    ds_table.add_column("Formats")
    ds_table.add_column("Size", justify="right")
    for d in bundle.get("datasets", []) or []:
        formats = d.get("available_formats") or []
        ds_table.add_row(
            d.get("slug", ""),
            d.get("name", ""),
            str(d.get("episode_count", 0)),
            ", ".join(formats),
            format_bytes(d.get("size_bytes", 0) or 0),
        )

    return Group(Panel(_NL.join(header_lines), title="Bundle"), ds_table)
