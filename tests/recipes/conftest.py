"""Recipe-CI test config.

Adds the ``staging`` marker so ``tests/recipes/test_recipes_run.py`` can declare
``pytestmark = pytest.mark.staging`` and the marker doesn't trigger a
PytestUnknownMarkWarning in unit-test runs (which skip the whole module via the
``_staging_gate`` fixture). Also mirrors the marker registration that lives in
``pyproject.toml`` so the local conftest is independently honest about what
this directory does.
"""

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "staging: tests against staging-api.verlet.co (requires VERLET_STAGING_TEST=1)",
    )
