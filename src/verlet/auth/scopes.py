"""CLIAUTH-07 — Personal Access Token (PAT) scope vocabulary.

The seven entries below mirror the locked frozenset on the backend
(``backend/core/domains/personal_access_token/schema.py:17-37`` per
Research §1.1 / §10.5). The CLI rejects unknown scopes BEFORE issuing
``POST /api/platform/v1/auth/tokens`` so users see a fast, clear error
without burning a server round-trip.

Note: ``read:tokens`` is intentionally NOT in this set. The list endpoint
requires ``read:account``; minting and revoking require ``write:tokens``.
"""
from __future__ import annotations

import click

PAT_SCOPES: frozenset[str] = frozenset({
    "read:catalog",
    "read:datasets",
    "read:ego_segments",
    "read:account",
    "read:purchases",
    "write:push",
    "write:tokens",
})


def validate_scopes(scopes: list[str]) -> None:
    """Validate a user-supplied list of PAT scopes against ``PAT_SCOPES``.

    Raises:
        click.UsageError: if ``scopes`` is empty (at least one is required) or
            contains any string outside the locked seven-element vocabulary.
            The error message lists every valid scope (sorted) so the user can
            immediately fix the typo without consulting docs.
    """
    if not scopes:
        raise click.UsageError("At least one --scope is required.")
    for s in scopes:
        if s not in PAT_SCOPES:
            valid = ", ".join(sorted(PAT_SCOPES))
            raise click.UsageError(
                f"Invalid scope '{s}'. Valid scopes: {valid}"
            )
