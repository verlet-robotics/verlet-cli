"""Shared fixtures for Phase 28 CLI auth tests.

These fixtures back every CLIAUTH-06/07/08/09 test in this package.
Production modules referenced by the fixtures (e.g. ``verlet.auth.login``)
do not exist until Wave 1+; the fixtures are written so importing them
in Wave 0 does not require those modules to be present.
"""

from pathlib import Path
from unittest.mock import patch

import pytest
import respx
from click.testing import CliRunner


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """Redirect ``Path.home()`` and the HOME / USERPROFILE env vars to ``tmp_path``.

    Auth code under test reads ``~/.verlet/credentials.json``; this fixture
    isolates each test to a fresh per-tmp_path home so writes never leak across
    tests or onto the real developer machine.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))  # Windows
    # Some code paths call ``Path.home()`` directly; patch the classmethod too.
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    verlet_dir = tmp_path / ".verlet"
    verlet_dir.mkdir(exist_ok=True)
    return tmp_path


@pytest.fixture
def respx_mock():
    """httpx mock router scoped to ``https://api.verlet.co``.

    Tests register routes on the yielded ``respx.Router`` and assert call counts
    explicitly. ``assert_all_called=False`` so unused stubs don't fail the test.
    """
    with respx.mock(base_url="https://api.verlet.co", assert_all_called=False) as router:
        yield router


@pytest.fixture
def cli_runner():
    """Click ``CliRunner`` returning separated stdout / stderr.

    ``CliRunner.__init__`` lost the ``mix_stderr`` kwarg in Click 8.2+
    (and it stays gone in 9.x); separated stderr is now the default
    behavior, so tests can assert on ``result.stderr`` directly.
    """
    return CliRunner()


@pytest.fixture
def mocked_webbrowser():
    """Patch ``webbrowser.open`` so device-flow tests never spawn a real browser.

    Yields the mock so tests can assert it was (or wasn't) called. Patching the
    stdlib ``webbrowser`` module directly keeps this fixture usable even before
    ``verlet.auth.login`` exists (Wave 1+).
    """
    with patch("webbrowser.open", return_value=True) as m:
        yield m
