"""Pre-flight validation + name→id resolution for ``verlet destinations``.

Kept separate from ``commands.py`` so the pure parsing / regex logic is
unit-testable without a Click context. ``resolve_destination_ref`` is the one
function here that touches the network (a single ``GET /destinations``).
"""
from __future__ import annotations

import re

import click

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
# IAM role ARN: arn:aws:iam::<12-digit account>:role/<name>.
_ROLE_ARN_RE = re.compile(r"^arn:aws:iam::\d{12}:role/.+$")


def is_uuid(value: str) -> bool:
    """True when ``value`` is a canonical UUID (a destination id)."""
    return bool(_UUID_RE.match(value))


def parse_credential_pairs(pairs: tuple[str, ...]) -> dict[str, str]:
    """Turn repeated ``--credential KEY=VALUE`` flags into a credentials dict.

    Raises ``click.BadParameter`` (exit 2) on a pair without ``=`` or with an
    empty key — so malformed input fails fast, pre-HTTP.
    """
    creds: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise click.BadParameter(
                f"--credential must be KEY=VALUE (got '{pair}')"
            )
        key, _, value = pair.partition("=")
        key = key.strip()
        if not key:
            raise click.BadParameter(f"--credential has an empty key in '{pair}'")
        creds[key] = value
    return creds


def validate_role_arn(value: str) -> str:
    """Return ``value`` if it is a well-formed IAM role ARN, else BadParameter.

    Client-side shape check before the connect callback — saves a round trip
    on an obvious typo.
    """
    if not _ROLE_ARN_RE.match(value):
        raise click.BadParameter(
            f"'{value}' is not a valid IAM role ARN "
            "(expected arn:aws:iam::<account-id>:role/<name>)"
        )
    return value


async def resolve_destination_ref(value: str, profile_name: str | None) -> str:
    """Resolve a destination name-or-id to its id.

    A UUID passes straight through (no lookup). Otherwise ``GET /destinations``
    and match on ``name``: zero matches → a clear error pointing at
    ``destinations list``; more than one → an error listing the colliding ids
    (names are not guaranteed unique server-side).
    """
    if is_uuid(value):
        return value
    from verlet.destinations._api import fetch_destinations

    items = await fetch_destinations(profile_name)
    matches = [d for d in items if d.get("name") == value]
    if not matches:
        raise click.ClickException(
            f"No saved destination named '{value}'. "
            "Run `verlet destinations list` to see your destinations."
        )
    if len(matches) > 1:
        ids = ", ".join(d.get("id", "") for d in matches)
        raise click.ClickException(
            f"Multiple destinations named '{value}' ({ids}). "
            "Pass the destination id instead."
        )
    return matches[0]["id"]
