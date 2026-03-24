"""Ego CLI commands."""
import asyncio

import click

from verlet.display import console, ego_segment_table


@click.group("ego")
def ego_group():
    """Egocentric hand tracking data."""
    pass


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
    console.print(f"\n[dim]{len(segments)} segment(s) total[/dim]")
