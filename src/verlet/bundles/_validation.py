"""verlet bundles — flag validators + verbatim error-string constants.

Following the Phase 31 verbatim-error pattern: error strings live as
module-level constants so tests can byte-assert them and a future docs-gen
pass can pull them for the troubleshooting page. Editing the message
intentionally requires updating the test.
"""
from __future__ import annotations


BUNDLES_ARE_PROCESSED_ONLY: str = (
    "bundles are processed-only; --variant raw is not allowed"
)
"""Verbatim error string for `verlet bundles download --variant raw`.

Plan 30-09 wires this into the download subcommand's pre-flight gate
(D-BUNDLE3: zero network calls before exit). Plan 30-07 ships the constant
ahead of time so the test surface stays byte-stable when 30-09 lands.
"""


def validate_bundle_download_flags(
    *, variant: str | None, format: str | None
) -> None:
    """Pre-flight gate for `verlet bundles download` (CLIBUNDLE-05, D-BUNDLE3).

    Bundles are processed-only — there is no "raw bundle" concept. We reject
    ``--variant raw`` here BEFORE any network call so a misuse never burns a
    server round-trip. The exit code is 2 (same level as click.BadParameter
    / argparse-typo) to match the user's mental model: this is a flag-value
    error, not a runtime failure.

    ``format`` is currently unused (the caller validates via ``validate_format``
    from ``verlet.datasets.convert``); it is accepted in the signature for
    forward-compat so a future bundle-only format restriction lands in this
    helper rather than diverging across call sites.
    """
    if variant == "raw":
        import click

        click.echo(BUNDLES_ARE_PROCESSED_ONLY, err=True)
        raise SystemExit(2)
