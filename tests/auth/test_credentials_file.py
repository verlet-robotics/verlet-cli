"""CLIAUTH-08 — credentials.json file-permission contract.

Wave 1 (Plan 28-01) replaces the xfail bodies with real assertions on
the mode-0600 chmod and the over-permissive warning emitted on read.
"""

import pytest


def test_mode_0600(tmp_home):
    pytest.xfail("Implemented in Plan 28-01")


def test_warns_on_overpermissive(tmp_home, capsys):
    pytest.xfail("Implemented in Plan 28-01")
