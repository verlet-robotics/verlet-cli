"""CLIAUTH-COEX — legacy showcase access-code login.

Wraps the live ``POST /api/v1/showcase/auth`` endpoint (NOT under
``/api/platform/v1``) and persists the issued JWT into the new
credentials.json schema under ``kind=showcase_access_code``.

Used by:
  * ``verlet auth login --kind showcase`` (the documented future-proof path)
  * The legacy top-level ``verlet login`` shim in ``cli.py`` (kept working
    through 0.6.x with a stderr deprecation hint, removed in 0.7.0)

Wire format (verified at backend/services/showcase/routes.py):

  Request:  POST /api/v1/showcase/auth   {"code": "abc123"}
  Response: 200  {"token": "<showcase-jwt>", "customer_name": "Acme",
                  "expires_in": 86400}
  Error:    401  {"detail": "Invalid access code"} | {"detail": "Access code expired"}

The backend field is ``code``, not ``access_code`` (the plan's wire-format
section had a transcription error; the live endpoint, the 0.4.0 cli.py
``verlet login``, and the showcase ``access_codes`` table all key on
``code``). Showcase JWTs carry ``type=showcase`` and are explicitly NOT
accepted by ``/api/platform/v1/auth/me`` (Research §1.4) — status renderer
short-circuits that check.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import click
import httpx

from .credentials import upsert_profile

SHOWCASE_AUTH_PATH = "/api/v1/showcase/auth"
SHOWCASE_TTL_SECONDS = 24 * 3600  # Research §1.4 / §7 — server JWT TTL is 24h


def showcase_login(
    api_url: str,
    profile_name: str,
    access_code: str | None = None,
    email: str | None = None,
) -> dict:
    """Run the legacy showcase access-code flow and persist the result.

    ``access_code`` is prompted hidden when not provided. ``email`` is
    prompted (visible) when not provided and is required by the server —
    it attributes every download to the individual using the (possibly
    shared) access code. Returns ``{"profile": <name>, "customer_name":
    <str>}`` on success. Raises ``SystemExit(1)`` on auth failure (invalid
    / expired access code, missing/invalid email) or when the server
    response is malformed.
    """
    if access_code is None:
        access_code = click.prompt("Access code", hide_input=True)
    if email is None:
        email = click.prompt("Email")

    with httpx.Client(timeout=30.0) as http:
        try:
            r = http.post(
                api_url + SHOWCASE_AUTH_PATH,
                json={"code": access_code, "email": email},
            )
        except httpx.HTTPError as exc:
            click.echo(f"Network error: {exc}", err=True)
            raise SystemExit(1)

        if r.status_code == 401:
            detail = "Invalid access code."
            try:
                body = r.json()
                if isinstance(body, dict) and body.get("detail"):
                    detail = body["detail"]
            except Exception:
                pass
            click.echo(detail, err=True)
            raise SystemExit(1)

        if r.status_code == 422:
            # Request-body validation failure — almost always a malformed
            # or missing email. Pydantic returns detail as a list of errors.
            msg = "Invalid email address."
            try:
                body = r.json()
                errors = body.get("detail") if isinstance(body, dict) else None
                if isinstance(errors, list) and errors:
                    msg = "; ".join(
                        e.get("msg", "invalid value")
                        for e in errors
                        if isinstance(e, dict)
                    ) or msg
            except Exception:
                pass
            click.echo(msg, err=True)
            raise SystemExit(1)

        if r.status_code != 200:
            click.echo(
                f"Showcase login failed (HTTP {r.status_code}).",
                err=True,
            )
            raise SystemExit(1)

        try:
            body = r.json()
        except Exception:
            click.echo("Showcase server returned invalid JSON.", err=True)
            raise SystemExit(1)

    token = body.get("token")
    customer_name = body.get("customer_name")
    if not token:
        click.echo(
            "Showcase server did not return a token; cannot continue.",
            err=True,
        )
        raise SystemExit(1)

    # The server reports its own TTL (``expires_in`` seconds); fall back to
    # the documented 24h default if missing. Compute an absolute ISO-8601
    # expiry so the status renderer can do relative-time math.
    ttl = int(body.get("expires_in") or SHOWCASE_TTL_SECONDS)
    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(seconds=ttl)).isoformat()

    upsert_profile(
        profile_name,
        kind="showcase_access_code",
        api_url=api_url,
        access_token=token,
        customer_name=customer_name,
        expires_at=expires_at,
        issued_at=now.isoformat(),
    )

    sys.stdout.write(
        f"Authenticated as {customer_name} (showcase JWT, "
        f"expires in {ttl // 3600}h).\n"
        f"Saved to profile '{profile_name}' (kind=showcase_access_code).\n"
    )
    return {"profile": profile_name, "customer_name": customer_name}
