"""CLIAUTH-06 — ``verlet auth login`` device flow.

Wave 2 (Plan 28-02) replaces the xfail bodies with end-to-end device-flow
runs through ``CliRunner`` + ``respx`` mocks for ``/device/code`` and
``/device/token``.
"""

import pytest


def test_full_device_flow_cycle(tmp_home, respx_mock, mocked_webbrowser, monkeypatch):
    pytest.xfail("Implemented in Plan 28-02")


def test_no_browser_prints_url(tmp_home, respx_mock, mocked_webbrowser, monkeypatch):
    pytest.xfail("Implemented in Plan 28-02")


def test_slow_down_bumps_interval(tmp_home, respx_mock, mocked_webbrowser, monkeypatch):
    pytest.xfail("Implemented in Plan 28-02")
