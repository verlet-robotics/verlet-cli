"""Auth-suite-local conftest (no fixtures).

The shared CLI/auth fixtures (``cli_runner``, ``tmp_home``, ``respx_mock``,
``mocked_webbrowser``) moved up to ``tests/conftest.py`` in Plan 29-04 so
they propagate to ``tests/datasets/`` (and any future sibling) without a
``pytest_plugins`` re-export directive — that directive caused a hard
"Plugin already registered under a different name" failure when both
suites ran in one invocation.

This file is kept (empty) so future auth-only fixtures have a home.
"""
