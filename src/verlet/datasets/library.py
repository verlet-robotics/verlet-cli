"""verlet datasets library — list purchased datasets + bundles (G-P1).

Closes the "I paid, now what" hole: a platform client can enumerate what
their account owns without leaving the terminal. Backs onto the existing
``GET /api/platform/v1/downloads/library`` endpoint (``LibraryListResponse``).

Showcase access codes have no purchases — the command rejects that credential
kind up front with a clear pointer rather than letting the backend 401/403.
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
from verlet.datasets._api import fetch_library, resolve_credential_kind
from verlet.datasets._render import library_bundles_table, library_table
from verlet.display import console


@click.command("library")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit the raw LibraryListResponse JSON instead of Rich tables.",
)
@click.pass_context
def library(ctx: click.Context, as_json: bool) -> None:
    """List the datasets and bundles your account has purchased.

    \b
    The library is a platform-account feature — sign in with
    `verlet auth login`. Showcase access codes evaluate granted datasets
    (see `verlet datasets list`); they do not own purchases.
    """
    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    # Auth gate — fail fast pre-HTTP (D-MOD4, mirrors `datasets download`).
    try:
        require_profile(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` to sign in to your "
            "platform account."
        )

    # Showcase access codes carry no purchases — reject with a clear pointer
    # instead of surfacing an opaque backend 401/403.
    if resolve_credential_kind(profile_name) == "showcase_access_code":
        raise click.ClickException(
            "`datasets library` is a platform-account feature. Showcase "
            "access codes evaluate granted datasets — see `verlet datasets list`."
        )

    body = asyncio.run(fetch_library(profile_name))
    datasets = body.get("datasets") or []
    bundles = body.get("bundles") or []

    if as_json:
        click.echo(json.dumps(body, indent=2, default=str))
        return

    if not datasets and not bundles:
        console.print(
            "[dim]Your library is empty. Browse the catalog with "
            "`verlet datasets list`.[/dim]"
        )
        return

    if datasets:
        console.print(library_table(datasets))
    if bundles:
        if datasets:
            console.print()
        console.print(library_bundles_table(bundles))
