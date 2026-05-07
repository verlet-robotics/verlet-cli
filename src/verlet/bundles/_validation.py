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
