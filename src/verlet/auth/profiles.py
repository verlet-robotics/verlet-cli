"""Profile resolution: --profile flag → VERLET_PROFILE env → file default → "default".

The flag-vs-env half of precedence is handled inside Click via ``envvar=`` on
the root ``--profile`` option (cli.py); by the time ``ctx.obj["profile"]`` lands
here it is already either the user-typed flag value or the env var value (or
``None`` if neither was set). The file-default and literal-default fallbacks
are this module's job.
"""
from __future__ import annotations

from typing import Any

from .credentials import get_profile, load_credentials


class ProfileNotFoundError(Exception):
    """Raised when a command needs a profile but the named one doesn't exist."""


def resolve_profile_name(ctx_flag_value: str | None) -> str:
    """Resolve the active profile name.

    Click already collapses ``--profile`` flag and ``VERLET_PROFILE`` env var
    into a single value (flag wins) via ``envvar=`` on the root option. If
    that combined value is ``None``, fall back to ``default_profile`` from
    credentials.json (literal ``"default"`` if the file is missing).
    """
    if ctx_flag_value is not None:
        return ctx_flag_value
    doc = load_credentials()
    return doc.get("default_profile", "default")


def require_profile(name: str) -> dict[str, Any]:
    """Load the named profile or raise ``ProfileNotFoundError``.

    Used by commands that need an existing profile (status, tokens, logout).
    Lazy-create commands (login) should NOT call this — they create the
    profile on success.
    """
    entry = get_profile(name)
    if entry is None:
        raise ProfileNotFoundError(
            f"No profile named '{name}' "
            f"(run `verlet --profile {name} auth login` to create it)."
        )
    return entry
