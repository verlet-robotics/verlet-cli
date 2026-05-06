"""CLIAUTH-08 — multi-profile resolution + isolation.

Wave 1 (Plan 28-01) replaces the xfail bodies with assertions that
``--profile`` flag beats ``VERLET_PROFILE`` env which beats the
``default_profile`` field in ``credentials.json``.
"""

import pytest


def test_env_var_precedence(tmp_home, monkeypatch):
    pytest.xfail("Implemented in Plan 28-01")


def test_flag_beats_env(tmp_home, monkeypatch, cli_runner):
    pytest.xfail("Implemented in Plan 28-01")


def test_profile_isolation(tmp_home, cli_runner):
    pytest.xfail("Implemented in Plan 28-01")
