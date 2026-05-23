"""verlet showcase — showcase-prospect commands (G-S3).

`showcase stats` exposes the fleet-aggregate `operation-stats` endpoint — the
credibility numbers (fleet size, throughput, QC pass rate) a sales prospect
wants. It requires a showcase access-code profile; the command rejects other
credential kinds up front.
"""
from __future__ import annotations

import asyncio
import json

import click

from verlet.auth.profiles import (
    ProfileNotFoundError,
    require_profile,
    resolve_profile_name,
)
from verlet.display import console


@click.group("showcase")
def showcase_group() -> None:
    """Showcase-prospect commands (require a showcase access code)."""


@showcase_group.command("stats")
@click.option("--json", "as_json", is_flag=True, help="Emit raw JSON.")
@click.pass_context
def stats(ctx: click.Context, as_json: bool) -> None:
    """Show Verlet fleet operation stats — fleet size, throughput, QC.

    \b
    Requires a showcase access code:
      verlet auth login --kind showcase
    """
    from verlet.datasets._api import resolve_credential_kind
    from verlet.showcase._api import fetch_operation_stats
    from verlet.showcase._render import operation_stats_view

    profile_name = resolve_profile_name(
        ctx.obj.get("profile") if ctx.obj else None
    )
    try:
        require_profile(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login --kind showcase` with "
            "your access code."
        )

    if resolve_credential_kind(profile_name) != "showcase_access_code":
        raise click.ClickException(
            "`showcase stats` requires a showcase access code. Sign in with "
            "`verlet auth login --kind showcase`."
        )

    body = asyncio.run(fetch_operation_stats(profile_name))
    if as_json:
        click.echo(json.dumps(body, indent=2, default=str))
        return
    console.print(operation_stats_view(body))
