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
"""
from __future__ import annotations

import click


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
        if format is not None and format != "lerobot-v2":
            raise click.UsageError(
                f"--format {format} requires the Phase 30 conversion engine. "
                "Coming soon. For now, --format lerobot-v2 (native) ships in "
                "this release."
            )
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
        raise click.UsageError("--format is teleop-only in Phase 29.")
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
