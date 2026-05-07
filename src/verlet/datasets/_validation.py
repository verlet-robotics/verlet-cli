"""Pre-flight flag validation for ``verlet datasets download``.

Mirrors Phase 27 D-FM4 + D-EE4 + D-CV2 server contracts so users see the same
error string CLI-side or server-side. Error wording for ego variant mismatches
is lifted verbatim from ``backend/services/downloads/routes.py:1126-1131``:

  - ``"segment_ids invalid for variant=raw; use episode_ids"``
  - ``"episode_ids invalid for variant=processed; use segment_ids"``

The matrix is encoded as a single pure function (``validate_download_flags``)
so it can be unit-tested in isolation and reused by ``commands.py`` (Plan 03)
and any future surfaces (e.g. Phase 31's ``verlet ego``) without a Click round
trip. ``validate_kind_category`` covers the catalog-list-side D-FL1 lock that
``--category`` is ego-only.

Phase 30 (Plan 30-04) re-exports ``validate_format`` and ``SUPPORTED_FORMATS``
from ``verlet.datasets.convert`` so callers needing the full flag-matrix
surface have one canonical import (the Phase 29 convention).
"""
from __future__ import annotations

import click

# Re-export from convert.py so the flag-matrix surface stays in one place.
from verlet.datasets.convert import (  # noqa: F401
    SUPPORTED_FORMATS,
    validate_format,
)

__all__ = [
    "SUPPORTED_FORMATS",
    "validate_download_flags",
    "validate_format",
    "validate_kind_category",
]


def validate_download_flags(
    *,
    modality: str,                   # "arm" or "ego" (resolved from catalog row)
    variant: str | None,
    episode_ids: str | None,
    segment_ids: str | None,
    format: str | None,
) -> None:
    """Raise ``click.UsageError`` on any illegal flag combination.

    Returns ``None`` on success. Caller is responsible for resolving
    ``modality`` from the catalog row before calling (Phase 27 D-EE4 means
    the server has no default for ``--variant`` either).
    """
    if modality == "arm":
        if variant is not None:
            raise click.UsageError(
                "--variant is ego-only; this is a teleop dataset."
            )
        if segment_ids is not None:
            raise click.UsageError(
                "--segment-ids is ego-only; this is a teleop dataset."
            )
        # Phase 30 (CLIDATA-07): all 8 formats from SUPPORTED_FORMATS are
        # accepted; the validate_format Click callback (verlet/datasets/convert.py)
        # rejects unknown values pre-HTTP. The 202+job-id polling branch in
        # commands.py handles non-native formats end-to-end.
        return

    # modality == "ego"
    if variant is None:
        raise click.UsageError(
            "--variant is required for ego datasets (raw|processed)."
        )
    if variant not in ("raw", "processed"):
        raise click.UsageError(
            f"Invalid --variant '{variant}'. Valid: raw, processed."
        )
    if format is not None:
        # Ego variants (raw/processed) are exclusive of arm-style format
        # conversion — they go through the variant-specific manifest endpoints
        # (Phase 27 D-EE4). The CLI surfaces this as a flag-incompatibility.
        raise click.UsageError(
            "--format is teleop-only; ego datasets use --variant raw|processed.",
        )
    if variant == "raw" and segment_ids is not None:
        # Verbatim from backend/services/downloads/routes.py:1126-1131.
        raise click.UsageError(
            "segment_ids invalid for variant=raw; use episode_ids"
        )
    if variant == "processed" and episode_ids is not None:
        # Verbatim from backend/services/downloads/routes.py:1126-1131.
        raise click.UsageError(
            "episode_ids invalid for variant=processed; use segment_ids"
        )


def validate_kind_category(*, kind: str, category: str | None) -> None:
    """Reject ``--category`` when ``--kind teleop`` is set explicitly (D-FL1).

    ``--kind all`` (the default) sends both filters and lets the server return
    the natural intersection — arm rows have no category, so they drop out
    organically. ``--kind ego`` always allows ``--category``.
    """
    if category is None:
        return
    if kind == "teleop":
        raise click.UsageError(
            "--category is ego-only; remove it or pass --kind ego."
        )


# ---------------------------------------------------------------------------
# Plan 30-05 (CLIDATA-07): HuggingFace push URL parser + token resolver
#
# `verlet datasets push --to huggingface://org/repo` lives in push.py; the
# pure-function pieces (URL shape + D-FORMAT2 token precedence) live here so
# they share the existing _validation.py home for flag-matrix logic.
# ---------------------------------------------------------------------------

import os
import re

HF_URL_RE = re.compile(r"^huggingface://(?P<org>[^/]+)/(?P<repo>[^/]+)$")
"""Strict ``huggingface://org/repo`` shape — single slash, both segments
non-empty. Sub-paths (``huggingface://org/repo/branch``) intentionally
rejected; HF Hub repos don't carry a third segment in this contract."""

NO_HF_TOKEN_MSG = (
    "No HF token configured. "
    "Run `verlet auth tokens set hf <token>` or set HF_TOKEN env."
)
"""D-FORMAT2 verbatim error string — byte-asserted in Plan 30-05's tests
(Phase 31 verbatim-error pattern). Must match the wording in 30-RESEARCH.md
Q2 exactly so users see a single canonical hint regardless of where the
guard fires."""


def parse_hf_url(url: str) -> tuple[str, str]:
    """Parse ``huggingface://org/repo`` → ``(org, repo)`` (CLIDATA-07).

    Click ``BadParameter`` raised on:

      * non-``huggingface://`` schemes (``s3://``, ``gs://``, ``http://``);
        message contains ``"only huggingface:// supported"``.
      * malformed shapes (no slash, empty org, empty repo, sub-paths);
        message mentions the expected ``huggingface://org/repo`` form.

    The strict regex avoids surfacing into Click's standard argument-parser
    "Invalid value" wording — users see an actionable hint immediately.
    """
    if not url.startswith("huggingface://"):
        raise click.BadParameter(
            "only huggingface:// supported (expected huggingface://org/repo)",
        )
    m = HF_URL_RE.match(url)
    if m is None:
        raise click.BadParameter("expected huggingface://org/repo")
    return (m.group("org"), m.group("repo"))


def resolve_hf_token(profile_name: str) -> str:
    """Resolve the active HF token (D-FORMAT2 precedence).

    Order:

      1. Active profile's ``hf_token`` field in ``~/.verlet/credentials.json``.
      2. ``HF_TOKEN`` env var.
      3. Raise ``click.UsageError(NO_HF_TOKEN_MSG)``.

    The profile-resolved value beats the env var because the profile is
    more specific (you can keep ``HF_TOKEN`` unset on a workstation that
    has different tokens stored across multiple profiles). CI runs that
    only export ``HF_TOKEN`` keep working because step 2 picks it up.
    """
    from verlet.auth.credentials import read_hf_token

    token = read_hf_token(profile_name)
    if token:
        return token
    env = os.environ.get("HF_TOKEN")
    if env:
        return env
    raise click.UsageError(NO_HF_TOKEN_MSG)
