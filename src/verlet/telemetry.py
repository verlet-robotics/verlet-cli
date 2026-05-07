"""Opt-in telemetry via User-Agent header — CLIDIST-05 / D-DIST1.

Default = OFF. ``~/.verlet/config.json`` is the toggle:

  * absent / missing key / non-bool value -> disabled
  * exact Python ``True`` (JSON ``true``)  -> enabled

Privacy contract (NEVER emitted):

  * command names (``download``, ``push``, ``redeem``, ...)
  * dataset slugs / segment ids / bundle codes
  * filesystem paths / argv slices
  * user identities / email addresses

Only fields shipped when enabled: ``cli_version``, ``python_version``,
``os/arch``. The transport is the existing ``User-Agent`` request header
on every CLI -> backend call (authenticated or anonymous). Zero new
endpoints; zero new outbound network paths.
"""
from __future__ import annotations

import platform
import sys

from verlet.config import load_config


def telemetry_enabled() -> bool:
    """Pitfall 7: explicit ``is True`` check.

    Returns False for missing key, ``null``, string ``"true"``/``"false"``,
    integer ``0``/``1``, lists, dicts, or any other non-bool. Only the exact
    Python ``True`` (which JSON ``true`` deserializes to) flips the flag.
    """
    value = load_config().get("telemetry_enabled")
    return value is True


def build_user_agent(version: str, *, telemetry_enabled: bool) -> str:
    """Build the ``User-Agent`` header value.

    Disabled: bare ``verlet-cli/<version>``.
    Enabled:  ``verlet-cli/<version> (python/<py> <os>/<arch>)``.

    The privacy contract is enforced by the test suite — see
    ``tests/test_telemetry.py::test_user_agent_privacy_payload_no_command_or_path``.
    """
    if not telemetry_enabled:
        return f"verlet-cli/{version}"
    py = ".".join(map(str, sys.version_info[:2]))
    plat = platform.system().lower()
    arch = platform.machine().lower()
    return f"verlet-cli/{version} (python/{py} {plat}/{arch})"


def current_user_agent() -> str:
    """Convenience: build the UA for the live CLI version + telemetry state.

    Used by ``api_client.py`` and the anonymous httpx callers in
    ``bundles/_api.py`` + ``datasets/_api.py`` so they share one entry
    point and the test suite has one place to assert against.
    """
    from verlet import __version__

    return build_user_agent(__version__, telemetry_enabled=telemetry_enabled())
