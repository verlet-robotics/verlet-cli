"""Tests for the shared expiry-detection + refresh-hint helpers.

Coverage:

* ``is_profile_expired`` — past timestamps, future timestamps, missing
  ``expires_at`` (legacy long-lived PATs), and an unparseable string.
* ``refresh_command`` — every profile kind maps to its dedicated refresh
  command, and unknown kinds fall back to ``verlet auth login``.

The AuthenticatedClient + catalog-browse integration paths are exercised
in test_expired_token_friendly.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from verlet.auth.expiry import is_profile_expired, refresh_command

NOW = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)


def _profile(expires_at: str | None, kind: str = "showcase_access_code") -> dict:
    return {"kind": kind, "expires_at": expires_at}


def test_is_expired_when_expires_at_is_in_the_past():
    past = (NOW - timedelta(hours=1)).isoformat()
    assert is_profile_expired(_profile(past), now=NOW) is True


def test_is_expired_false_when_expires_at_is_in_the_future():
    future = (NOW + timedelta(hours=1)).isoformat()
    assert is_profile_expired(_profile(future), now=NOW) is False


def test_is_expired_false_when_expires_at_missing():
    """Legacy long-lived PATs carry no ``expires_at`` and must never be
    pre-rejected — the server stays the arbiter for those."""
    assert is_profile_expired(_profile(None), now=NOW) is False


def test_is_expired_tolerates_trailing_z():
    """The credentials file historically wrote ``...Z`` instead of ``+00:00``;
    both must parse the same."""
    past = (NOW - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    assert is_profile_expired(_profile(past), now=NOW) is True


def test_is_expired_false_on_unparseable_expires_at():
    """A garbled ``expires_at`` (e.g. truncated file) returns False so the
    server gets the chance to reject — the alternative is bricking the CLI."""
    assert is_profile_expired(_profile("not-a-date"), now=NOW) is False


def test_refresh_command_per_kind():
    assert (
        refresh_command({"kind": "showcase_access_code"})
        == "verlet auth login --kind showcase"
    )
    assert refresh_command({"kind": "pat"}) == "verlet auth tokens create"
    assert (
        refresh_command({"kind": "bundle_grant"})
        == "verlet bundles redeem <your code>"
    )
    assert refresh_command({"kind": "device_flow"}) == "verlet auth login"


def test_refresh_command_unknown_kind_falls_back_to_login():
    """Defensive default — if the credentials file picks up a new kind the
    CLI doesn't recognize, we still emit a non-empty actionable string."""
    assert refresh_command({"kind": "some_new_kind"}) == "verlet auth login"
    assert refresh_command({}) == "verlet auth login"
