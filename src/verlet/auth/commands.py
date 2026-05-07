"""``verlet auth`` Click group — login + logout + tokens subcommands.

Plan 28-04 will add ``auth status`` alongside the existing groups. The
legacy top-level ``verlet login`` command stays in ``cli.py`` as the
showcase access-code shim until Plan 28-04 finalizes its deprecation.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import click
from rich.console import Console
from rich.table import Table

from .credentials import load_credentials
from .login import device_flow_login
from .logout import logout as do_logout
from .profiles import ProfileNotFoundError, resolve_profile_name
from .showcase import showcase_login as _showcase_login
from .status import render_status as _render_status
from .tokens import create_pat as _create_pat
from .tokens import list_pats as _list_pats
from .tokens import revoke_pat as _revoke_pat
from .tokens import show_pat as _show_pat


_LOGIN_EPILOG = """\b
Examples:

```bash
verlet auth login
```

\b
Use a named profile (writes ~/.verlet/credentials.json under that key):

```bash
verlet --profile staging auth login
```

\b
Headless / SSH session -- print the URL instead of opening a browser:

```bash
verlet auth login --no-browser
```
"""


_TOKENS_CREATE_EPILOG = """\b
Examples:

```bash
verlet auth tokens create --name ci --scope read:datasets --scope write:push
```

\b
30-day expiry:

```bash
verlet auth tokens create --name ci --scope read:catalog --expires-in 30d
```
"""


@click.group(name="auth")
def auth_group() -> None:
    """Manage authentication: login, logout, status, and tokens."""


@auth_group.command("login", epilog=_LOGIN_EPILOG)
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
    # Resolve api_url: flag wins, else profile's existing api_url, else default.
    doc = load_credentials()
    existing = doc["profiles"].get(profile_name, {})
    resolved_api_url = (
        api_url or existing.get("api_url") or "https://api.verlet.co"
    )

    if kind == "showcase":
        _showcase_login(
            api_url=resolved_api_url,
            profile_name=profile_name,
            access_code=None,
        )
        return

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


# ---------------------------------------------------------------------------
# auth status — kind-aware identity / token / expiry summary (CLIAUTH-09)
# ---------------------------------------------------------------------------


@auth_group.command("status")
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON instead of human text.",
)
@click.option(
    "--refresh",
    is_flag=True,
    default=False,
    help="Re-probe /auth/me and update cached identity (device_flow / pat only).",
)
@click.pass_context
def cmd_status(ctx: click.Context, json_output: bool, refresh: bool) -> None:
    """Show the active profile's identity, token, scopes, and expiry."""
    try:
        rc = _render_status(
            ctx.obj.get("profile"),
            json_output=json_output,
            refresh=refresh,
        )
    except ProfileNotFoundError as exc:
        click.echo(str(exc), err=True)
        raise SystemExit(1)
    if rc != 0:
        raise SystemExit(rc)


# ---------------------------------------------------------------------------
# auth tokens — Personal Access Tokens (PATs)
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^(\d+)([dwmy])$", re.IGNORECASE)
_DURATION_UNITS = {"d": 1, "w": 7, "m": 30, "y": 365}


def _parse_duration(value: str | None) -> str | None:
    """Translate ``30d`` / ``2w`` / ``6m`` / ``1y`` into ISO-8601 ``expires_at``.

    Returns None when ``value`` is None / empty (no expiry). Raises
    ``click.BadParameter`` for malformed input so the user sees the same
    Click error chrome they'd get from any other option.
    """
    if not value:
        return None
    m = _DURATION_RE.match(value)
    if not m:
        raise click.BadParameter(
            f"--expires-in must look like '30d' / '90d' / '1y' (got '{value}')."
        )
    qty, unit = int(m.group(1)), m.group(2).lower()
    days = qty * _DURATION_UNITS[unit]
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()


@auth_group.group("tokens")
def tokens_group() -> None:
    """Manage Personal Access Tokens (PATs)."""


@tokens_group.command("create", epilog=_TOKENS_CREATE_EPILOG)
@click.option(
    "--name",
    required=True,
    help="PAT name (1-255 chars, unique among your active PATs).",
)
@click.option(
    "--scope",
    "scopes",
    multiple=True,
    help=(
        "One of: read:catalog, read:datasets, read:ego_segments, "
        "read:account, read:purchases, write:push, write:tokens. "
        "Pass --scope multiple times to grant multiple scopes."
    ),
)
@click.option(
    "--expires-in",
    "expires_in",
    default=None,
    help="Duration like 30d, 90d, 1y (default: never expires).",
)
@click.option(
    "--save-to",
    default=None,
    help="Profile to save the token to (default: active profile).",
)
@click.option(
    "--no-save",
    is_flag=True,
    default=False,
    help="Print the plaintext PAT but do not write it to credentials.json.",
)
@click.pass_context
def cmd_tokens_create(
    ctx: click.Context,
    name: str,
    scopes: tuple[str, ...],
    expires_in: str | None,
    save_to: str | None,
    no_save: bool,
) -> None:
    """Mint a new Personal Access Token."""
    expires_at = _parse_duration(expires_in)
    _create_pat(
        name=name,
        scopes=list(scopes),
        expires_at=expires_at,
        save_to=save_to,
        no_save=no_save,
        profile_name=ctx.obj.get("profile"),
    )


@tokens_group.command("list")
@click.pass_context
def cmd_tokens_list(ctx: click.Context) -> None:
    """List all active PATs (plaintext is never displayed)."""
    items = _list_pats(profile_name=ctx.obj.get("profile"))
    console = Console()
    table = Table(show_header=True, header_style="bold")
    table.add_column("ID")
    table.add_column("Name")
    table.add_column("Scopes")
    table.add_column("Last 4")
    table.add_column("Expires")
    table.add_column("Last used")
    for item in items:
        table.add_row(
            (item["id"][:8] + "..."),
            item["name"],
            ", ".join(item["scopes"]),
            item["last_4"],
            item.get("expires_at") or "never",
            item.get("last_used_at") or "never",
        )
    console.print(table)


@tokens_group.command("revoke")
@click.argument("id_or_name")
@click.pass_context
def cmd_tokens_revoke(ctx: click.Context, id_or_name: str) -> None:
    """Revoke a PAT by id or name (idempotent)."""
    ok = _revoke_pat(id_or_name, profile_name=ctx.obj.get("profile"))
    if not ok:
        raise SystemExit(1)


@tokens_group.command("show")
@click.argument("id_or_name")
@click.pass_context
def cmd_tokens_show(ctx: click.Context, id_or_name: str) -> None:
    """Show metadata for a single PAT (plaintext is never displayed)."""
    item = _show_pat(id_or_name, profile_name=ctx.obj.get("profile"))
    if item is None:
        click.echo(f"No PAT with id/name '{id_or_name}'.", err=True)
        raise SystemExit(1)
    for key in (
        "id",
        "name",
        "scopes",
        "last_4",
        "created_at",
        "expires_at",
        "last_used_at",
    ):
        click.echo(f"{key}: {item.get(key)}")


# ---------------------------------------------------------------------------
# Plan 30-05 (D-FORMAT2): `verlet auth tokens set hf <token>`
#
# Adds a `set` subgroup under `tokens` with a single `hf` leaf command.
# The two-level nesting (`set hf`) leaves room for future auxiliary tokens
# (e.g. `set wandb`, `set s3-access-key`) without re-organizing the surface.
# ---------------------------------------------------------------------------


@tokens_group.group("set")
def tokens_set_group() -> None:
    """Set auxiliary tokens (HuggingFace, etc.) on the active profile."""


@tokens_set_group.command("hf")
@click.argument("token")
@click.pass_context
def cmd_tokens_set_hf(ctx: click.Context, token: str) -> None:
    """Save a HuggingFace token to the active profile.

    \b
    Used by `verlet datasets push --to huggingface://...`. Token persists in
    ~/.verlet/credentials.json (mode 0o600) until removed manually
    (no `unset hf` shipped yet — edit the file directly).
    """
    from .credentials import set_hf_token

    profile_name = resolve_profile_name(ctx.obj.get("profile") if ctx.obj else None)
    set_hf_token(profile_name, token)
    click.echo(f"Saved HF token to profile '{profile_name}'.")
