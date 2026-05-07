"""verlet bundles ... — Click group + subcommands (CLIBUNDLE-01..07).

Plan 30-07 wires the first two subcommands:

  * `verlet bundles browse`  — anonymous public catalog (CLIBUNDLE-01).
  * `verlet bundles redeem <code>` — D-BUNDLE2 idempotent redemption
    (CLIBUNDLE-02). See Task 2.

Later plans (30-08, 30-09) extend this group with `list`, `info`, `download`,
`export-manifest`. Each new subcommand follows the Phase 29 separation:
synchronous Click entry → asyncio.run(...) → async _api wrapper → render.
"""
from __future__ import annotations

import asyncio
import json

import click

from verlet.bundles._api import fetch_bundles_browse
from verlet.bundles._render import bundles_browse_table
from verlet.display import console


@click.group("bundles")
def bundles_group() -> None:
    """Browse, redeem, list, info, download Verlet research / purchased bundles."""


@bundles_group.command("browse")
@click.option(
    "--limit",
    default=50,
    type=int,
    show_default=True,
    help="Max bundles to display.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON to stdout instead of a Rich table.",
)
def browse(limit: int, as_json: bool) -> None:
    """List public research bundles. Anonymous; no auth required.

    \b
    Examples:
      verlet bundles browse
      verlet bundles browse --json | jq '.[0]'
      verlet bundles browse --limit 5
    """
    try:
        body = asyncio.run(fetch_bundles_browse(limit=limit))
    except Exception as exc:  # network down, server 500, etc.
        click.echo(f"failed to fetch bundles: {exc}", err=True)
        raise SystemExit(1)

    items = body.get("items", [])

    if as_json:
        click.echo(json.dumps(items, indent=2))
        return

    if not items:
        console.print("[dim]No public bundles available.[/dim]")
        return

    console.print(bundles_browse_table(items))
