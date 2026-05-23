"""verlet datasets quality / analytics — inspect a dataset before download (G-P6).

A buyer evaluating data quality from the CLI had nothing. These two commands
expose the catalog's QC-distribution and analytics endpoints so a researcher
can vet a dataset before pulling gigabytes. Both accept optional auth.
"""
from __future__ import annotations

import asyncio
import json

import click

from verlet.datasets._api import (
    fetch_dataset_analytics,
    fetch_dataset_qc_distributions,
)
from verlet.datasets._render import analytics_view, qc_distributions_table
from verlet.display import console


@click.command("quality")
@click.argument("slug")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
@click.pass_context
def quality(ctx: click.Context, slug: str, as_json: bool) -> None:
    """Show a dataset's per-check QC-metric distributions."""
    profile_name = ctx.obj.get("profile") if ctx.obj else None
    body = asyncio.run(fetch_dataset_qc_distributions(profile_name, slug))

    if as_json:
        click.echo(json.dumps(body, indent=2, default=str))
        return
    if not (body.get("distributions") or {}):
        console.print(
            f"[dim]No QC distributions available for '{slug}'.[/dim]"
        )
        return
    console.print(qc_distributions_table(body))


@click.command("analytics")
@click.argument("slug")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
@click.pass_context
def analytics(ctx: click.Context, slug: str, as_json: bool) -> None:
    """Show a dataset's aggregate analytics — episode counts + metric stats."""
    profile_name = ctx.obj.get("profile") if ctx.obj else None
    body = asyncio.run(fetch_dataset_analytics(profile_name, slug))

    if as_json:
        click.echo(json.dumps(body, indent=2, default=str))
        return
    meta, qc_table, metrics = analytics_view(body)
    console.print(meta)
    console.print()
    console.print(qc_table)
    console.print()
    console.print(metrics)
