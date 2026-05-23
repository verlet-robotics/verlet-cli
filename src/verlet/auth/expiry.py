"""Shared profile-expiry detection + refresh-command hints.

A single source of truth so every authed surface (AuthenticatedClient,
catalog browse fallback, future auth/status callers) agrees on what
"expired" means and what to tell the user to do about it.

The existing expiry math in :mod:`verlet.auth.status` is kept inline
because it formats human-readable timestamps and per-kind status output;
this module's helpers are the machine-readable counterpart.
"""
from __future__ import annotations

from datetime import datetime, timezone


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp, tolerating a trailing ``Z``."""
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def is_profile_expired(profile: dict, *, now: datetime | None = None) -> bool:
    """Return True if the profile carries an ``expires_at`` already in the past.

    Profiles with no ``expires_at`` (legacy long-lived PATs) are reported as
    not-expired — the server stays the final arbiter for those. The ``now``
    parameter is for deterministic testing; production callers omit it.
    """
    expires_at = _parse_iso(profile.get("expires_at"))
    if expires_at is None:
        return False
    return expires_at <= (now or datetime.now(timezone.utc))


def refresh_command(profile: dict) -> str:
    """Per-kind command the user runs to refresh their credentials.

    Mirrors the EXPIRED hints already shown by ``verlet auth status``
    (status.py lines 198-262), kept in lockstep so a user who sees the
    same advice in two places never reads contradictory commands.
    """
    kind = profile.get("kind")
    if kind == "showcase_access_code":
        return "verlet auth login --kind showcase"
    if kind == "pat":
        return "verlet auth tokens create"
    if kind == "bundle_grant":
        return "verlet bundles redeem <your code>"
    # device_flow + unknown — `auth login` is the safe default.
    return "verlet auth login"
