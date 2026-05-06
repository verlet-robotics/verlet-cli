"""CLIAUTH-07 — ``verlet auth tokens create|list|revoke|show``.

Wave 3 (Plan 28-03) replaces the xfail bodies with assertions that
PAT mint persists to the active profile, list never echoes plaintext,
revoke clears the local profile, and unknown ``--scope`` values are
rejected client-side without a server round-trip.
"""

import pytest


def test_create_persists_and_warns(tmp_home, respx_mock, cli_runner):
    pytest.xfail("Implemented in Plan 28-03")


def test_list_no_plaintext(tmp_home, respx_mock, cli_runner):
    pytest.xfail("Implemented in Plan 28-03")


def test_revoke_clears_local(tmp_home, respx_mock, cli_runner):
    pytest.xfail("Implemented in Plan 28-03")


def test_invalid_scope_rejected(tmp_home, cli_runner):
    pytest.xfail("Implemented in Plan 28-03")
