"""CLIAUTH-07 — Personal Access Token (PAT) lifecycle helpers.

These four functions back the ``verlet auth tokens`` Click subgroup
(create / list / revoke / show). They reuse :class:`AuthenticatedClient`
so the bearer header is set uniformly regardless of whether the active
profile holds a device-flow JWT or a higher-scope PAT (Research §1.3,
§10).

Wire format mirrors ``backend/services/platform_auth/routes.py:1307-1443``:

  * POST /api/platform/v1/auth/tokens   -> mint (returns plaintext ONCE)
  * GET  /api/platform/v1/auth/tokens   -> list (never plaintext)
  * DELETE /api/platform/v1/auth/tokens/{id} -> revoke (idempotent 204/404)

The PAT format is ``pat_<lookup>_<secret>`` with EXACTLY two underscores
after ``pat_`` (Research §1.1 / middleware.py:166-181); the CLI does not
need to parse it — backend middleware does the dispatch.
"""
from __future__ import annotations

import sys
from typing import Any

import click

from ..api_client import AuthenticatedClient
from .credentials import delete_profile, get_profile, upsert_profile
from .scopes import validate_scopes

TOKENS_PATH = "/api/platform/v1/auth/tokens"


def create_pat(
    name: str,
    scopes: list[str],
    expires_at: str | None,
    save_to: str | None,
    no_save: bool,
    profile_name: str | None,
) -> dict:
    """Mint a PAT and (optionally) persist the plaintext to a local profile.

    Steps:
        1. Validate scopes BEFORE any HTTP call. ``test_invalid_scope_rejected``
           verifies this fast-fail path.
        2. POST /api/platform/v1/auth/tokens with JSON {name, scopes, expires_at}.
        3. Render the plaintext ONCE with a yellow ``SAVE THIS NOW`` warning
           surrounded by blank lines.
        4. Unless ``no_save`` is set, upsert the active (or ``save_to``) profile
           under ``kind=pat`` with the plaintext as ``access_token``.

    Returns the parsed PATCreatedResponse dict.
    """
    # 1. Validate before any HTTP. test_invalid_scope_rejected hits this path.
    validate_scopes(scopes)

    client = AuthenticatedClient(profile_name)
    body: dict[str, Any] = {"name": name, "scopes": scopes}
    if expires_at is not None:
        body["expires_at"] = expires_at

    r = client.post(TOKENS_PATH, json=body)
    if r.status_code == 409:
        try:
            detail = (r.json() or {}).get("detail", "")
        except ValueError:
            detail = ""
        if isinstance(detail, str) and "cap" in detail.lower():
            raise click.ClickException(
                "PAT cap reached. Revoke an existing token before creating a new one."
            )
        raise click.ClickException(
            f"A PAT named '{name}' already exists. "
            f"Pick another name or revoke the existing one."
        )
    if r.status_code in (422, 403):
        try:
            payload = r.json()
        except ValueError:
            payload = r.text
        raise click.ClickException(f"Server rejected request: {payload}")
    r.raise_for_status()
    resp = r.json()

    # 2. Render plaintext-once warning.
    sys.stdout.write("\n")
    sys.stdout.write(f"Minted PAT '{resp['name']}' (id={resp['id']})\n")
    sys.stdout.write(f"Scopes: {', '.join(resp['scopes'])}\n")
    exp_display = resp.get("expires_at") or "never"
    sys.stdout.write(f"Expires: {exp_display}\n")
    sys.stdout.write("\n")
    sys.stdout.write(f"  {resp['plaintext']}\n")
    sys.stdout.write("\n")
    click.secho(
        "[!] SAVE THIS NOW. This is the only time the token will be displayed.",
        fg="yellow",
        bold=True,
    )
    click.secho(
        "    Store it in your password manager or a secrets file (mode 0600).",
        fg="yellow",
    )

    # 3. Persist to local profile unless --no-save.
    if not no_save:
        target = save_to or client.profile_name
        upsert_profile(
            target,
            kind="pat",
            api_url=client.api_url,
            access_token=resp["plaintext"],
            pat_id=resp["id"],
            name=resp["name"],
            scopes=resp["scopes"],
            last_4=resp["last_4"],
            created_at=resp["created_at"],
            expires_at=resp.get("expires_at"),
        )
        sys.stdout.write(f"\nSaved to profile '{target}' (kind=pat).\n")
    return resp


def list_pats(profile_name: str | None) -> list[dict]:
    """GET /api/platform/v1/auth/tokens and return the list of PATListItem dicts.

    Defensively asserts that no item carries a ``plaintext`` field — Research §1.1
    invariant. The list endpoint never echoes plaintext; if it ever did, it would
    be a serious server-side bug and we refuse to render it.
    """
    client = AuthenticatedClient(profile_name)
    r = client.get(TOKENS_PATH)
    r.raise_for_status()
    items = r.json()
    for item in items:
        assert "plaintext" not in item, (
            "BUG: backend returned plaintext in list response — refusing to render."
        )
    return items


def revoke_pat(id_or_name: str, profile_name: str | None) -> bool:
    """DELETE /api/platform/v1/auth/tokens/{id} after resolving id-or-name.

    If the deleted PAT matches the active profile's ``pat_id``, the local
    profile is also cleared so subsequent commands don't hit the server with
    a dead token. Returns True on success, False if no matching PAT was
    found (already revoked or not yours — backend deliberately conflates).
    """
    client = AuthenticatedClient(profile_name)
    items = list_pats(profile_name)
    match = next(
        (i for i in items if i["id"] == id_or_name or i["name"] == id_or_name),
        None,
    )
    if match is None:
        sys.stderr.write(
            f"No PAT with id/name '{id_or_name}' (already revoked or not yours).\n"
        )
        return False
    target_id = match["id"]
    r = client.delete(f"{TOKENS_PATH}/{target_id}")
    if r.status_code not in (204, 404):
        sys.stderr.write(
            f"Server returned {r.status_code} when revoking PAT.\n"
        )
        return False

    # Clear local profile if it points at the deleted PAT.
    active = get_profile(client.profile_name)
    if active and active.get("pat_id") == target_id:
        delete_profile(client.profile_name)
        sys.stdout.write(
            f"Cleared local profile '{client.profile_name}' (its PAT was revoked).\n"
        )
    sys.stdout.write(f"Revoked PAT '{match['name']}' (id={target_id}).\n")
    return True


def show_pat(id_or_name: str, profile_name: str | None) -> dict | None:
    """Return the metadata dict for a single PAT, or ``None`` if not found.

    Backend has no per-id GET endpoint (Research §10) — only the list endpoint.
    We list and filter client-side.
    """
    items = list_pats(profile_name)
    return next(
        (i for i in items if i["id"] == id_or_name or i["name"] == id_or_name),
        None,
    )
