"""End-to-end staging recipe — gated on ``VERLET_STAGING_TEST=1``.

Plan 28-04 (Wave 4) wires the body against ``staging-api.verlet.co``:
device-flow login → mint PAT → list → revoke → cleanup. Skipped by default
so day-to-day ``pytest tests/auth/`` runs stay hermetic.
"""

import os

import pytest

pytestmark = pytest.mark.staging


@pytest.mark.skipif(
    os.environ.get("VERLET_STAGING_TEST") != "1",
    reason="Set VERLET_STAGING_TEST=1 to run end-to-end staging recipe.",
)
def test_staging_recipe(tmp_home):
    pytest.xfail("Recipe completed in Plan 28-04 against staging-api.verlet.co")
