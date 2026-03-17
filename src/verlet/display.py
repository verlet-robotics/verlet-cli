"""Shared Rich display utilities for tables and formatting."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

console = Console()


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins:02d}m"


def format_size(nbytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def category_table(categories: list[dict]) -> Table:
    """Build a Rich table for category listing."""
    table = Table(title="EgoDex Catalog")
    table.add_column("Category", style="bold cyan")
    table.add_column("Subcategory", style="white")
    table.add_column("Segments", justify="right", style="green")
    table.add_column("Duration", justify="right", style="yellow")

    for cat in categories:
        first = True
        for sub in cat.get("subcategories", []):
            table.add_row(
                cat["category"] if first else "",
                sub["subcategory"],
                str(sub["segmentCount"]),
                format_duration(sub["totalDurationSec"]),
            )
            first = False
        if not cat.get("subcategories"):
            table.add_row(
                cat["category"],
                "-",
                str(cat.get("segmentCount", 0)),
                format_duration(cat.get("totalDurationSec", 0)),
            )

    return table


def segment_table(segments: list[dict]) -> Table:
    """Build a Rich table for detailed segment listing."""
    table = Table(title="Segments")
    table.add_column("ID", style="bold")
    table.add_column("Category", style="cyan")
    table.add_column("Subcategory", style="white")
    table.add_column("Station", style="magenta")
    table.add_column("Duration", justify="right", style="yellow")

    for seg in segments:
        table.add_row(
            seg["id"],
            seg.get("category", ""),
            seg.get("subcategory", ""),
            seg.get("station", ""),
            format_duration(seg.get("durationSec", 0)),
        )
    return table
