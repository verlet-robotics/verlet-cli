"""``verlet auth`` Click group — login + logout subcommands.

Plan 28-03 will add an ``auth tokens`` sibling subgroup; Plan 28-04 will add
``auth status``. The legacy top-level ``verlet login`` command stays in
``cli.py`` as the showcase access-code shim until Plan 28-04 finalizes its
deprecation.
"""
from __future__ import annotations

import click

from .credentials import load_credentials
from .login import device_flow_login
from .logout import logout as do_logout
from .profiles import ProfileNotFoundError, resolve_profile_name


@click.group(name="auth")
def auth_group() -> None:
    """Manage authentication: login, logout, status, and tokens."""


@auth_group.command("login")
@click.option(
    "--api-url",
    default=None,
    help=(
        "Override API base URL (default: https://api.verlet.co or the "
        "active profile's api_url)."
    ),
)
@click.option(
    "--no-browser",
    is_flag=True,
    default=False,
    help="Do not open a browser; print the verification URL instead.",
)
@click.option(
    "--kind",
    type=click.Choice(["device", "showcase"]),
    default="device",
    show_default=True,
    help=(
        "device = OAuth device flow (default). "
        "showcase = legacy access-code flow (finalized in Plan 28-04)."
    ),
)
@click.pass_context
def cmd_login(ctx: click.Context, api_url: str | None, no_browser: bool, kind: str) -> None:
    """Sign in to Verlet via the OAuth device flow (default) or legacy showcase code."""
    profile_name = resolve_profile_name(ctx.obj.get("profile"))
    if kind == "showcase":
        click.echo(
            "Showcase login is wired in Plan 28-04. For now, run "
            "`verlet login` (legacy command).",
            err=True,
        )
        raise SystemExit(2)

    # Resolve api_url: flag wins, else profile's existing api_url, else default.
    doc = load_credentials()
    existing = doc["profiles"].get(profile_name, {})
    resolved_api_url = (
        api_url or existing.get("api_url") or "https://api.verlet.co"
    )
    device_flow_login(
        api_url=resolved_api_url,
        profile_name=profile_name,
        no_browser=no_browser,
    )


@auth_group.command("logout")
@click.pass_context
def cmd_logout(ctx: click.Context) -> None:
    """Log out the active profile (kind-aware)."""
    profile_name = resolve_profile_name(ctx.obj.get("profile"))
    try:
        do_logout(profile_name)
    except ProfileNotFoundError as exc:
        click.echo(str(exc), err=True)
        raise SystemExit(1)
