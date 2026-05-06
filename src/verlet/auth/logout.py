"""CLIAUTH-06 — kind-aware ``verlet auth logout``.

Per Research §8:

  * ``device_flow``: clear the local profile only. There is no public
    backend endpoint to revoke a device-flow JWT or its refresh token,
    so the cached access_token continues to work until its natural expiry.
  * ``pat``: DELETE /api/platform/v1/auth/tokens/{pat_id} to revoke the PAT
    server-side, then clear the local profile. If the server is unreachable
    or returns a non-2xx, surface a warning but still clear local state —
    the user told us to log out.
  * ``showcase_access_code``: clear the local profile only (no revoke
    endpoint exists for legacy showcase tokens).
"""
from __future__ import annotations

import sys

import httpx

from .credentials import delete_profile, get_profile

TOKENS_DELETE_PATH = "/api/platform/v1/auth/tokens/{pat_id}"


def logout(profile_name: str) -> None:
    """Clear the named profile. PAT profiles also DELETE the token server-side."""
    profile = get_profile(profile_name)
    if profile is None:
        sys.stderr.write(
            f"No profile named '{profile_name}' (nothing to log out).\n"
        )
        raise SystemExit(1)

    kind = profile.get("kind")
    api_url = profile.get("api_url") or "https://api.verlet.co"

    if kind == "pat":
        pat_id = profile.get("pat_id")
        name = profile.get("name", "<unnamed>")
        access_token = profile.get("access_token")
        if pat_id and access_token:
            try:
                r = httpx.delete(
                    api_url + TOKENS_DELETE_PATH.format(pat_id=pat_id),
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=30.0,
                )
                if r.status_code in (204, 404):
                    sys.stdout.write(
                        f"Revoked PAT '{name}' (kind=pat) on the server.\n"
                    )
                else:
                    sys.stderr.write(
                        f"warning: server returned {r.status_code} when "
                        f"revoking PAT; clearing local profile anyway.\n"
                    )
            except httpx.HTTPError as exc:
                sys.stderr.write(
                    f"warning: could not reach server to revoke PAT "
                    f"({exc}); clearing local profile anyway.\n"
                )

    delete_profile(profile_name)
    sys.stdout.write(
        f"Cleared '{profile_name}' profile credentials"
        + (f" (kind={kind})" if kind else "")
        + ".\n"
    )
    if kind == "device_flow":
        expires = profile.get("expires_at")
        if expires:
            sys.stdout.write(
                f"The session JWT will continue to work until {expires} "
                f"(no server-side device-flow revoke endpoint exists yet).\n"
            )
