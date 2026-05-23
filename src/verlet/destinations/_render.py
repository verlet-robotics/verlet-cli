"""Rich table builders for ``verlet destinations`` output.

Pure functions: take dict payloads, return Rich ``Table`` objects with no
print side-effect. The caller (commands.py) prints to ``verlet.display.console``.
"""
from __future__ import annotations

from typing import Any

from rich.table import Table


def destinations_table(items: list[dict[str, Any]]) -> Table:
    """Saved cloud destinations — one row per ``CloudDestination``.

    Credentials are never part of the response, so nothing secret renders.
    The ID column is truncated; ``--json`` carries the full id for scripting.
    """
    table = Table(title="Cloud Destinations")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Provider")
    table.add_column("Connect")
    table.add_column("Bucket")
    table.add_column("Prefix")
    table.add_column("Region")
    table.add_column("ID")
    for d in items:
        dest_id = d.get("id") or ""
        table.add_row(
            d.get("name", ""),
            d.get("provider") or "—",
            d.get("auth_kind") or "—",
            d.get("bucket") or "—",
            d.get("prefix") or "—",
            d.get("region") or "—",
            f"{dest_id[:8]}…" if dest_id else "—",
        )
    return table


def providers_table(items: list[dict[str, Any]]) -> Table:
    """Connectable destination providers — name, label, and how to connect."""
    table = Table(title="Destination Providers")
    table.add_column("Provider", style="cyan", no_wrap=True)
    table.add_column("Label")
    table.add_column("Connect via")
    table.add_column("Notes")
    for p in items:
        table.add_row(
            p.get("name", ""),
            p.get("label") or "—",
            p.get("auth_kind") or "—",
            p.get("deeplink_hint") or "",
        )
    return table
