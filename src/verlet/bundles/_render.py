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
