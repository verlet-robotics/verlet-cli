"""verlet datasets episodes / segments — browse a dataset's contents (G-P7).

`download --episode-ids` / `--segment-ids` need integer indices the user has
no other CLI way to discover. These two commands list a dataset's episodes
(any modality) and segments (ego only) with the ``Index`` column set to the
exact value those download flags consume.

Both endpoints accept optional auth — anonymous callers see public rows.
"""
from __future__ import annotations

import asyncio
import json

import click

from verlet.datasets._api import fetch_dataset_episodes, fetch_dataset_segments
from verlet.datasets._render import episodes_table, page_footer, segments_table
from verlet.display import console


@click.command("episodes")
@click.argument("slug")
@click.option("--page", default=1, type=int, help="Page number (default: 1).")
@click.option("--limit", default=20, type=int, help="Page size (max 100).")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
@click.pass_context
def episodes(
    ctx: click.Context, slug: str, page: int, limit: int, as_json: bool
) -> None:
    """List a dataset's episodes.

    The Index column is the value `verlet datasets download --episode-ids`
    expects — browse here, then feed the indices into a selective download.
    """
    profile_name = ctx.obj.get("profile") if ctx.obj else None
    body = asyncio.run(
        fetch_dataset_episodes(
            profile_name, slug, page=page, page_size=min(limit, 100)
        )
    )
    items = body.get("items") or []

    if as_json:
        click.echo(json.dumps(body, indent=2, default=str))
        return
    if not items:
        console.print(f"[dim]No episodes found for '{slug}'.[/dim]")
        return
    console.print(episodes_table(items))
    footer = page_footer(body)
    if footer:
        console.print(f"[dim]{footer}[/dim]")


@click.command("segments")
@click.argument("slug")
@click.option("--page", default=1, type=int, help="Page number (default: 1).")
@click.option("--limit", default=20, type=int, help="Page size (max 100).")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
@click.pass_context
def segments(
    ctx: click.Context, slug: str, page: int, limit: int, as_json: bool
) -> None:
    """List an ego dataset's segments.

    The Index column is the value `verlet datasets download --segment-ids`
    expects. Teleop datasets have no segments — they return an empty list.
    """
    profile_name = ctx.obj.get("profile") if ctx.obj else None
    body = asyncio.run(
        fetch_dataset_segments(
            profile_name, slug, page=page, page_size=min(limit, 100)
        )
    )
    items = body.get("items") or []

    if as_json:
        click.echo(json.dumps(body, indent=2, default=str))
        return
    if not items:
        console.print(
            f"[dim]No segments found for '{slug}' "
            "(segments are ego-only).[/dim]"
        )
        return
    console.print(segments_table(items))
    footer = page_footer(body)
    if footer:
        console.print(f"[dim]{footer}[/dim]")
