"""Centralized authenticated HTTP client.

`AuthenticatedClient` resolves the active profile, builds an `httpx.Client`,
injects ``Authorization: Bearer <access_token>`` on every outgoing request,
and (for ``kind=device_flow`` profiles) opportunistically refreshes the
access token via ``POST /api/platform/v1/auth/refresh`` when the cached
``expires_at`` is within ``REFRESH_LEAD_SECONDS`` of now.

Per Plan 28-02 / Research §1.4 / §7:

  * The ``Authorization`` header is uniform across all kinds — the backend
    middleware in ``core/domains/client_user/middleware.py`` switches on
    the ``pat_`` prefix, so the CLI does not need to know whether the
    bearer is a JWT or a PAT to set the right header.
  * Refresh is only meaningful for device-flow profiles (PATs are long-lived
    until revoked; showcase access-code JWTs are not refreshable).
  * On 401 from /auth/refresh we treat the session as dead, clear the
    access_token + refresh_token in the profile, set ``needs_relogin: true``,
    and exit non-zero with a stderr hint pointing at ``verlet auth login``.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .auth.credentials import upsert_profile
from .auth.profiles import require_profile, resolve_profile_name

DEFAULT_API_URL = "https://api.verlet.co"
REFRESH_PATH = "/api/platform/v1/auth/refresh"
DEVICE_FLOW_ACCESS_TTL_SECONDS = 8 * 3600  # Research §7 — server JWT TTL is 480 min


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp, tolerating a trailing ``Z``."""
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


class AuthenticatedClient:
    """Profile-aware httpx wrapper with opportunistic refresh."""

    REFRESH_LEAD_SECONDS = 300  # 5 minutes

    def __init__(self, profile_name: str | None = None) -> None:
        self._profile_name = resolve_profile_name(profile_name)
        self._profile = require_profile(self._profile_name)
        self._http = httpx.Client(timeout=30.0)

    @property
    def api_url(self) -> str:
        return self._profile.get("api_url") or DEFAULT_API_URL

    @property
    def profile_name(self) -> str:
        return self._profile_name

    @property
    def kind(self) -> str:
        return self._profile.get("kind", "")

    def headers(self) -> dict[str, str]:
        """Return the ``Authorization`` header for the active profile."""
        return {"Authorization": f"Bearer {self._profile['access_token']}"}

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    def _refresh_if_needed(self) -> None:
        if self._profile.get("kind") != "device_flow":
            return
        refresh_token = self._profile.get("refresh_token")
        expires_at = _parse_iso(self._profile.get("expires_at"))
        if not refresh_token or not expires_at:
            return
        now = datetime.now(timezone.utc)
        if expires_at - now > timedelta(seconds=self.REFRESH_LEAD_SECONDS):
            return

        r = self._http.post(
            self.api_url + REFRESH_PATH,
            json={"refresh_token": refresh_token},
        )
        if r.status_code == 401:
            sys.stderr.write(
                "Session expired. Run `verlet auth login` to sign in again.\n"
            )
            # Clear the dead access token and mark needs_relogin so subsequent
            # commands can short-circuit with a clear error rather than a
            # confusing 401 from /me etc.
            upsert_profile(
                self._profile_name,
                kind="device_flow",
                api_url=self.api_url,
                access_token=None,
                refresh_token=None,
                expires_at=None,
                needs_relogin=True,
                identity=self._profile.get("identity"),
            )
            raise SystemExit(1)
        r.raise_for_status()
        tokens = r.json()
        new_expires_at = (
            datetime.now(timezone.utc)
            + timedelta(seconds=DEVICE_FLOW_ACCESS_TTL_SECONDS)
        ).isoformat()
        # Refresh tokens rotate (Research §13.7 — assume rotation, always
        # overwrite). If the response omits a new refresh_token, keep the
        # existing one so the next refresh still works.
        self._profile["access_token"] = tokens["access_token"]
        self._profile["refresh_token"] = tokens.get(
            "refresh_token", refresh_token
        )
        self._profile["expires_at"] = new_expires_at
        upsert_profile(
            self._profile_name,
            kind="device_flow",
            api_url=self.api_url,
            access_token=self._profile["access_token"],
            refresh_token=self._profile["refresh_token"],
            expires_at=new_expires_at,
            identity=self._profile.get("identity"),
            active_namespace=self._profile.get("active_namespace"),
            scopes_granted=self._profile.get("scopes_granted"),
            issued_at=self._profile.get("issued_at"),
        )

    # ------------------------------------------------------------------
    # Request helpers
    # ------------------------------------------------------------------

    def request(self, method: str, path: str, **kw: Any) -> httpx.Response:
        self._refresh_if_needed()
        url = self.api_url + path if path.startswith("/") else path
        headers = kw.pop("headers", {}) or {}
        headers = {**self.headers(), **headers}
        return self._http.request(method, url, headers=headers, **kw)

    def get(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("POST", path, **kw)

    def delete(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("DELETE", path, **kw)

    def close(self) -> None:
        self._http.close()


def auth_headers_for_profile(profile_name: str | None = None) -> dict[str, str]:
    """Convenience: just the ``Authorization`` header for the active profile.

    Used by the ego/teleop catalog migration in Plan 28-04, which needs to
    drop the ad-hoc ``_headers()`` helpers in favor of a single source of
    truth.
    """
    client = AuthenticatedClient(profile_name)
    try:
        return client.headers()
    finally:
        client.close()
