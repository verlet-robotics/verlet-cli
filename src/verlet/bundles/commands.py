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


# ---------------------------------------------------------------------------
# Plan 30-08 — `verlet bundles list` (CLIBUNDLE-03) + `verlet bundles info`
# (CLIBUNDLE-04). Both consume Plan 30-03's authenticated routes.
#
# `--all` for `list` maps to `?include_inactive=true` (D-BUNDLE1). 401 surfaces
# the verbatim string "not authenticated; run verlet auth login" via _api's
# `_exit_with_stderr` helper -- no try/except required here.
#
# Local imports inside each command body keep the cold-import path of
# `verlet bundles browse` lean (the browse path is the more common operation
# and never touches AuthenticatedClient).
# ---------------------------------------------------------------------------


@bundles_group.command("list")
@click.option(
    "--all",
    "show_all",
    is_flag=True,
    default=False,
    help="Include expired/revoked bundles (D-BUNDLE1).",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON to stdout instead of a Rich table.",
)
@click.pass_context
def list_bundles(ctx: click.Context, show_all: bool, as_json: bool) -> None:
    """List bundles in your account (research grants + purchased) (CLIBUNDLE-03).

    \b
    By default only active bundles are shown. ``--all`` includes expired
    and revoked grants with a ``Status`` column color-coded
    active=green / expired=yellow / revoked=red.

    \b
    Examples:
      verlet bundles list
      verlet bundles list --all
      verlet bundles list --json | jq '.[0].bundle_slug'
    """
    from verlet.api_client import AuthenticatedClient
    from verlet.auth.profiles import resolve_profile_name
    from verlet.bundles._api import fetch_bundles_list
    from verlet.bundles._render import bundles_list_table

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    async def _run() -> dict:
        client = AuthenticatedClient(profile_name)
        try:
            return await fetch_bundles_list(client, include_inactive=show_all)
        finally:
            client.close()

    body = asyncio.run(_run())
    items = body.get("items", []) or []

    if as_json:
        click.echo(json.dumps(items, indent=2))
        return

    if not items:
        console.print("[dim]No bundles in your account.[/dim]")
        return

    console.print(bundles_list_table(items))
