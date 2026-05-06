"""CLIAUTH-08 — legacy ``~/.verlet/token.json`` migration.

Wave 1 (Plan 28-01) replaces the xfail body with an assertion that a
synthetic legacy ``token.json`` fixture is migrated losslessly into the
``default`` profile under ``credentials.json`` with ``kind=showcase_access_code``.
"""

import pytest


def test_legacy_migration(tmp_home):
    pytest.xfail("Implemented in Plan 28-01")
