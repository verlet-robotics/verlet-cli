"""Authentication: login, logout, token validation."""

from __future__ import annotations

import base64
import json
import time

import click
import httpx

from verlet.config import DEFAULT_API_BASE, clear_config, load_config, save_config


def _decode_jwt_payload(token: str) -> dict:
    """Decode JWT payload (no signature verification — server is authoritative)."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid JWT format")
    # Add padding
    payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
    return json.loads(base64.urlsafe_b64decode(payload_b64))


def get_auth_headers() -> dict[str, str]:
    """Return headers for authenticated API requests, or raise click.ClickException."""
    cfg = load_config()
    token = cfg.get("token")
    if not token:
        raise click.ClickException(
            "Not logged in. Run 'verlet login' first."
        )
    expires_at = cfg.get("expires_at", 0)
    if time.time() > expires_at:
        raise click.ClickException(
            "Session expired. Run 'verlet login' to re-authenticate."
        )
    return {
        "Cookie": f"access_token={token}",
        "Authorization": f"Bearer {token}",
    }


def get_api_base() -> str:
    return load_config().get("api_base", DEFAULT_API_BASE)


def login(api_base: str | None = None) -> None:
    """Prompt for access code and authenticate."""
    api_base = api_base or DEFAULT_API_BASE
    code = click.prompt("Access code", hide_input=True)

    try:
        resp = httpx.post(
            f"{api_base}/api/auth",
            json={"code": code},
            timeout=15,
        )
    except httpx.RequestError as e:
        raise click.ClickException(f"Connection failed: {e}")

    if resp.status_code != 200:
        detail = resp.json().get("error", resp.text)
        raise click.ClickException(f"Authentication failed: {detail}")

    body = resp.json()
    customer = body.get("customer", "unknown")

    # Get token from response body (preferred) or Set-Cookie header (fallback)
    token = body.get("token")
    if not token:
        cookie_header = resp.headers.get("set-cookie", "")
        for part in cookie_header.split(";"):
            part = part.strip()
            if part.startswith("access_token="):
                token = part.split("=", 1)[1]
                break

    if not token:
        raise click.ClickException("No token received from server.")

    # Decode expiry from JWT
    try:
        payload = _decode_jwt_payload(token)
        expires_at = payload.get("exp", time.time() + 604800)
    except Exception:
        expires_at = time.time() + 604800  # fallback: 7 days

    save_config({
        "token": token,
        "customer": customer,
        "api_base": api_base,
        "expires_at": expires_at,
    })

    click.echo(f"Logged in as {click.style(customer, bold=True)}")


def logout() -> None:
    """Remove stored credentials."""
    clear_config()
    click.echo("Logged out.")
