"""Shared fixtures across the entire ``tests/`` tree.

Originally these lived in ``tests/auth/conftest.py`` and were re-exported to
``tests/datasets/`` via ``pytest_plugins = ["tests.auth.conftest"]`` in the
datasets conftest. That worked when the suites ran in isolation but tripped
pytest's "Plugin already registered under a different name" error when both
suites ran in one invocation (``pytest tests/datasets/ tests/auth/``):
pytest auto-discovers ``tests/auth/conftest.py`` once natively AND once via
the plugin name from the directive — same module, two registration names →
hard fail.

Moving the fixtures up to ``tests/conftest.py`` lets pytest's normal
conftest-discovery propagate them to every subdir without `pytest_plugins`.
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
    """Click ``CliRunner`` for invoking the CLI in tests.

    Click 8.1.x folds stderr into ``result.output``; Click 8.2+ captures it
    separately on ``result.stderr`` (and ``result.stderr`` raises
    ``ValueError`` under 8.1.x). Tests that assert on stderr text should use
    ``combined_output`` from this module, which tolerates either layout.
    """
    return CliRunner()


def combined_output(result) -> str:
    """Return stdout + stderr regardless of the installed Click version.

    Click 8.1.x folds stderr into ``result.output`` and raises ``ValueError``
    on ``result.stderr``; Click 8.2+ separates the streams. Reading both
    defensively keeps assertions stable across either layout.
    """
    out = result.output or ""
    try:
        err = result.stderr or ""
    except ValueError:
        # Click 8.1.x mixed mode — stderr is already inside ``result.output``.
        err = ""
    return out + err


@pytest.fixture
def mocked_webbrowser():
    """Patch ``webbrowser.open`` so device-flow tests never spawn a real browser.

    Yields the mock so tests can assert it was (or wasn't) called. Patching the
    stdlib ``webbrowser`` module directly keeps this fixture usable even before
    ``verlet.auth.login`` exists (Wave 1+).
    """
    with patch("webbrowser.open", return_value=True) as m:
        yield m
