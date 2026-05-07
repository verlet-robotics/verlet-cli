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
