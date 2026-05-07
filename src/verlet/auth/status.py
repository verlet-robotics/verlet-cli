"""CLIAUTH-09 — ``verlet auth status`` kind-aware renderer.

Three coexisting profile kinds, three slightly different status renderings
(per Research §9). Shared invariants:

  * Bearer tokens are always displayed masked (`first8...last4`); plaintext
    never reaches stdout.
  * Expiry math runs against ``profile["expires_at"]`` parsed as ISO-8601.
    Within 24h of expiry → yellow near-expiry warning. Past expiry → red
    EXPIRED line + exit code 1 (so CI scripts can detect).
  * ``--json`` short-circuits text rendering and emits a single JSON object
    with stable keys: profile, kind, api_url, identity, namespace,
    expires_at, expired, expires_in_seconds, scopes, customer_name.
  * ``--refresh`` re-probes ``GET /auth/me`` for ``device_flow`` and ``pat``
    profiles, updating cached identity. ``showcase_access_code`` profiles
    DO NOT call /auth/me (showcase JWTs carry ``type=showcase`` and the
    backend rejects them at /me with 401 — Research §1.4).

The renderer returns ``0`` on healthy, ``1`` on expired. Callers (the Click
subcommand) raise ``SystemExit(rc)`` so CI sees the right code.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import click

from ..api_client import AuthenticatedClient
from .credentials import upsert_profile
from .profiles import resolve_profile_name, require_profile

ME_PATH = "/api/platform/v1/auth/me"


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp, tolerating a trailing ``Z``."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _mask(token: str | None) -> str:
    """Return ``first8...last4`` for non-trivial tokens, else the token verbatim.

    For tokens shorter than the prefix+suffix overlap, we still avoid
    plaintext leakage by returning ``<short token>`` rather than echoing
    a recognizable substring of a real token.
    """
    if not token:
        return "<none>"
    if len(token) <= 8:
        return token
    return f"{token[:8]}...{token[-4:]}"


def _humanize_seconds(seconds: int) -> str:
    """Render a ``seconds`` delta as a friendly relative duration."""
    if seconds < 0:
        return f"expired {(-seconds) // 60} min ago"
    if seconds < 3600:
        return f"in {seconds // 60}m"
    if seconds < 86400:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"in {h}h {m}m"
    return f"in {seconds // 86400}d"


def _refresh_identity(profile_name: str, profile: dict) -> dict:
    """Re-probe ``/auth/me`` for device_flow / pat and persist updates.

    Returns the (possibly updated) profile dict. On any failure we surface
    a stderr warning and fall back to the cached profile — status is still
    useful even if the network is down.
    """
    kind = profile.get("kind", "")
    if kind not in ("device_flow", "pat"):
        return profile
    try:
        client = AuthenticatedClient(profile_name)
        try:
            r = client.get(ME_PATH)
        finally:
            client.close()
    except Exception as exc:  # network down, profile unauthenticated, etc.
        sys.stderr.write(f"warning: --refresh probe failed: {exc}\n")
        return profile
    if r.status_code != 200:
        sys.stderr.write(
            f"warning: --refresh probe returned HTTP {r.status_code}; "
            f"showing cached identity.\n"
        )
        return profile
    me = r.json()
    identity = {
        "id": me.get("id"),
        "account_id": me.get("account_id"),
        "email": me.get("email"),
        "display_name": me.get("display_name"),
        "slug": me.get("slug"),
    }
    # Build a refreshed entry: keep every existing field, overwrite identity
    # and active_namespace. Drop kind from fields (passed positionally below).
    fields = {k: v for k, v in profile.items() if k != "kind"}
    fields["identity"] = identity
    fields["active_namespace"] = me.get("active_namespace")
    upsert_profile(profile_name, kind=kind, **fields)
    profile = {**profile, "identity": identity, "active_namespace": me.get("active_namespace")}
    return profile


def render_status(
    profile_name: str | None,
    json_output: bool = False,
    refresh: bool = False,
) -> int:
    """Render auth status for the active (or named) profile.

    Returns ``0`` on healthy, ``1`` on expired. Callers translate the int
    into a SystemExit so CI sees the right exit code.
    """
    name = resolve_profile_name(profile_name)
    profile = require_profile(name)
    kind = profile.get("kind", "")
    api_url = profile.get("api_url") or "https://api.verlet.co"

    if refresh:
        profile = _refresh_identity(name, profile)

    expires_at = _parse_iso(profile.get("expires_at"))
    now = datetime.now(timezone.utc)
    if expires_at is not None:
        expires_in = int((expires_at - now).total_seconds())
        expired = expires_in <= 0
    else:
        expires_in = None
        expired = False

    if json_output:
        sys.stdout.write(
            json.dumps(
                {
                    "profile": name,
                    "kind": kind,
                    "api_url": api_url,
                    "identity": profile.get("identity"),
                    "namespace": profile.get("active_namespace"),
                    "expires_at": profile.get("expires_at"),
                    "expired": expired,
                    "expires_in_seconds": expires_in,
                    "scopes": profile.get("scopes") if kind == "pat" else None,
                    "customer_name": (
                        profile.get("customer_name")
                        if kind == "showcase_access_code"
                        else None
                    ),
                },
                indent=2,
            )
            + "\n"
        )
        return 1 if expired else 0

    # Header (common to all kinds)
    click.echo(f"Profile: {name}  (kind={kind})")
    click.echo(f"API:     {api_url}")
    click.echo("")

    # Identity block (device_flow + pat only — showcase has no /me)
    if kind in ("device_flow", "pat"):
        identity = profile.get("identity") or {}
        click.echo(
            f"Identity:    {identity.get('display_name', '<unknown>')} "
            f"<{identity.get('email', '<unknown>')}>"
        )
        account_id = identity.get("account_id") or ""
        account_display = (
            account_id[:8] + "..." if account_id else "<unknown>"
        )
        click.echo(
            f"Account:     {identity.get('slug', '<unknown>')} "
            f"(account_id: {account_display})"
        )
        ns = profile.get("active_namespace") or {}
        if ns:
            click.echo(
                f"Namespace:   {ns.get('type', '?')}: {ns.get('slug', '?')} "
                f"(role={ns.get('role', '?')})"
            )
        click.echo("")

    # Per-kind body
    if kind == "device_flow":
        click.echo(f"Token:       {_mask(profile.get('access_token'))}")
        if expires_at:
            if expired:
                click.secho(
                    "EXPIRED -- run `verlet auth login` to refresh.",
                    fg="red",
                    bold=True,
                )
            else:
                click.echo(
                    f"Expires:     {_humanize_seconds(expires_in)}  "
                    f"({profile.get('expires_at')})"
                )
        click.echo(
            f"Refresh:     {'present' if profile.get('refresh_token') else 'absent'}"
        )
        click.echo(f"Issued:      {profile.get('issued_at', '<unknown>')}")
    elif kind == "pat":
        click.echo(f"PAT name:    {profile.get('name', '<unknown>')}")
        click.echo(f"PAT id:      {(profile.get('pat_id') or '')[:8]}...")
        click.echo(f"Token:       {_mask(profile.get('access_token'))}")
        click.echo(f"Scopes:      {', '.join(profile.get('scopes') or [])}")
        click.echo(f"Last 4:      {profile.get('last_4', '<unknown>')}")
        if profile.get("expires_at"):
            if expired:
                click.secho(
                    "EXPIRED -- mint a new PAT with `verlet auth tokens create`.",
                    fg="red",
                    bold=True,
                )
            else:
                click.echo(
                    f"Expires:     {_humanize_seconds(expires_in)}  "
                    f"({profile.get('expires_at')})"
                )
        else:
            click.echo("Expires:     never")
        click.echo(f"Created:     {profile.get('created_at', '<unknown>')}")
    elif kind == "showcase_access_code":
        click.echo(f"Customer:    {profile.get('customer_name', '<unknown>')}")
        click.echo(f"Token:       {_mask(profile.get('access_token'))}")
        if expires_at:
            if expired:
                click.secho(
                    "EXPIRED -- run `verlet auth login --kind showcase` to refresh.",
                    fg="red",
                    bold=True,
                )
            else:
                click.echo(
                    f"Expires:     {_humanize_seconds(expires_in)}  "
                    f"({profile.get('expires_at')})"
                )
        click.echo(f"Issued:      {profile.get('issued_at', '<unknown>')}")
    else:
        click.echo(f"Unknown kind '{kind}'.")

    # Near-expiry tail (within 24h, not yet expired)
    if expires_in is not None and not expired and expires_in < 24 * 3600:
        click.secho(
            "[!] Expiring soon -- run `verlet auth login` to refresh.",
            fg="yellow",
        )
    return 1 if expired else 0
