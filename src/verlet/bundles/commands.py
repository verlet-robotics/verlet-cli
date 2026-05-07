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

@bundles_group.command("redeem")
@click.argument("code")
@click.option(
    "--email",
    default=None,
    help=(
        "Email to associate with the redemption (server may require for "
        "first-time redemptions on new accounts)."
    ),
)
@click.pass_context
def redeem(ctx: click.Context, code: str, email: str | None) -> None:
    """Redeem a research-access code; save bearer token to ~/.verlet/credentials.json.

    \b
    D-BUNDLE2 idempotent: re-redeeming the same code overwrites the local
    profile entry with the server-issued (fresh) token. Revoked / expired
    codes return 410 Gone with a verbatim server detail; unknown codes
    return 404 with "Invalid code".

    \b
    Examples:
      verlet bundles redeem ABCD-1234
      verlet --profile staging bundles redeem ABCD-1234
    """
    # Local imports keep the cold-import path of `verlet bundles browse` lean.
    from verlet.auth.credentials import upsert_bundle_grant_profile
    from verlet.auth.profiles import resolve_profile_name
    from verlet.bundles._api import RedeemError, redeem_bundle_code

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    try:
        response = asyncio.run(redeem_bundle_code(code, email=email))
    except RedeemError as exc:
        click.echo(exc.detail, err=True)
        raise SystemExit(1)
    except Exception as exc:  # network down, 5xx, etc.
        click.echo(f"redeem failed: {exc}", err=True)
        raise SystemExit(1)

    upsert_bundle_grant_profile(
        profile_name,
        access_token=response["access_token"],
        expires_at=response["expires_at"],
        bundle_slug=response["bundle_slug"],
    )
    console.print(
        f"[green]Redeemed.[/green] Bundle: [cyan]{response['bundle_slug']}[/cyan], "
        f"expires: {response['expires_at']}"
    )
