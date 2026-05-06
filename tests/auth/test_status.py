"""CLIAUTH-09 — ``verlet auth status``.

Wave 4 (Plan 28-04) replaces the xfail bodies with assertions on the
per-kind output rendering (device_flow / pat / showcase_access_code) and
the machine-readable ``--json`` output.
"""

import pytest


def test_device_flow_status(tmp_home, respx_mock, cli_runner):
    pytest.xfail("Implemented in Plan 28-04")


def test_pat_status(tmp_home, respx_mock, cli_runner):
    pytest.xfail("Implemented in Plan 28-04")


def test_showcase_status(tmp_home, cli_runner):
    pytest.xfail("Implemented in Plan 28-04")


def test_json_output(tmp_home, cli_runner):
    pytest.xfail("Implemented in Plan 28-04")
