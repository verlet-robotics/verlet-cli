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
