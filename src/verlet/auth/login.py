"""CLIAUTH-06 — RFC 8628 device-flow login.

``device_flow_login(api_url, profile_name, no_browser, scope)`` runs the full
device-authorization grant per Research §1.2 + §6 + §7:

  1. POST /api/platform/v1/auth/device/code (form-encoded)
  2. Display the user_code + verification URL; webbrowser.open unless
     --no-browser was passed
  3. Poll POST /api/platform/v1/auth/device/token (form-encoded) at
     ``interval`` seconds; honor RFC 8628 §3.5 ``slow_down`` (+5s) and
     ``authorization_pending`` (continue)
  4. On success, GET /api/platform/v1/auth/me to populate identity
  5. Persist into the active profile under ``kind=device_flow`` with
     access_token + refresh_token + expires_at + identity + active_namespace

The function raises ``SystemExit(1)`` on any unrecoverable failure (denied,
expired, unknown error, polling timeout).
"""
from __future__ import annotations

import sys
import time
import webbrowser
from datetime import datetime, timedelta, timezone

import httpx

from .. import __version__
from .credentials import upsert_profile

DEVICE_CODE_PATH = "/api/platform/v1/auth/device/code"
DEVICE_TOKEN_PATH = "/api/platform/v1/auth/device/token"
ME_PATH = "/api/platform/v1/auth/me"
GRANT_TYPE_DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code"
DEVICE_FLOW_ACCESS_TTL_SECONDS = 8 * 3600  # Research §7 — server JWT TTL is 480 min


def _format_user_code(code: str) -> str:
    """Render an 8-char Crockford-base32 code as ``ABCD-1234`` for display."""
    if len(code) >= 8:
        return f"{code[:4]}-{code[4:8]}"
    return code


def device_flow_login(
    api_url: str,
    profile_name: str,
    no_browser: bool = False,
    scope: str | None = None,
) -> dict:
    """Run the device-authorization grant end-to-end and persist the result."""
    with httpx.Client(timeout=30.0) as http:
        # 1. /device/code — form-encoded body per RFC 8628 §3.1
        form: dict[str, str] = {
            "client_id": "verlet-cli",
            "client_version": __version__,
        }
        if scope:
            form["scope"] = scope
        r = http.post(
            api_url + DEVICE_CODE_PATH,
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        r.raise_for_status()
        body = r.json()
        user_code = body["user_code"]
        device_code = body["device_code"]
        interval = int(body.get("interval", 5))
        expires_in = int(body.get("expires_in", 600))
        verification_uri_complete = body["verification_uri_complete"]

        # 2. Display + 3. open browser (or skip)
        sys.stdout.write(
            f"Starting device authorization with {api_url}\n"
            f"Your verification code: {_format_user_code(user_code)}\n"
            f"Visit: {verification_uri_complete}\n"
        )
        if no_browser:
            sys.stdout.write(
                "(--no-browser specified -- open the URL manually.)\n"
            )
        else:
            sys.stdout.write("Opening browser...\n")
            try:
                opened = webbrowser.open(verification_uri_complete)
            except Exception:
                opened = False
            if not opened:
                sys.stdout.write(
                    "Could not open browser automatically. "
                    "Visit the URL above.\n"
                )

        # 4. Poll /device/token — RFC 8628 §3.4-§3.5
        sys.stdout.write("Waiting for approval...\n")
        deadline = time.monotonic() + expires_in
        tokens: dict | None = None
        while time.monotonic() < deadline:
            time.sleep(interval)  # sleep BEFORE the first poll so interval is honored
            r = http.post(
                api_url + DEVICE_TOKEN_PATH,
                data={
                    "grant_type": GRANT_TYPE_DEVICE_CODE,
                    "device_code": device_code,
                    "client_id": "verlet-cli",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if r.status_code == 200:
                tokens = r.json()
                break
            if r.status_code == 400:
                try:
                    err_body = r.json() or {}
                except Exception:
                    err_body = {}
                err = err_body.get("error", "")
                if err == "authorization_pending":
                    continue
                if err == "slow_down":
                    interval += 5  # RFC 8628 §3.5
                    continue
                if err == "access_denied":
                    sys.stderr.write(
                        "Login was denied in the browser. "
                        "Run `verlet auth login` again to retry.\n"
                    )
                    raise SystemExit(1)
                if err == "expired_token":
                    sys.stderr.write(
                        "The verification code expired. "
                        "Run `verlet auth login` again to start over.\n"
                    )
                    raise SystemExit(1)
                sys.stderr.write(f"Login failed: {err or '<no error>'}\n")
                raise SystemExit(1)
            sys.stderr.write(f"Unexpected status: {r.status_code}\n")
            raise SystemExit(1)

        if tokens is None:
            sys.stderr.write(
                "Login timed out (device code expired). Try again.\n"
            )
            raise SystemExit(1)

        access_token = tokens["access_token"]
        refresh_token = tokens.get("refresh_token")

        # 5. /auth/me for identity (Research §1.4)
        me_r = http.get(
            api_url + ME_PATH,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        me_r.raise_for_status()
        me = me_r.json()

        # 6. Persist (Research §3 schema)
        now = datetime.now(timezone.utc)
        expires_at = (
            now + timedelta(seconds=DEVICE_FLOW_ACCESS_TTL_SECONDS)
        ).isoformat()
        identity = {
            "id": me.get("id"),
            "account_id": me.get("account_id"),
            "email": me.get("email"),
            "display_name": me.get("display_name"),
            "slug": me.get("slug"),
        }
        upsert_profile(
            profile_name,
            kind="device_flow",
            api_url=api_url,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at,
            identity=identity,
            active_namespace=me.get("active_namespace"),
            issued_at=now.isoformat(),
        )

        # 7. Friendly output
        sys.stdout.write(
            f"Signed in as {me.get('display_name')} ({me.get('email')}).\n"
        )
        return {"profile": profile_name, **identity}
