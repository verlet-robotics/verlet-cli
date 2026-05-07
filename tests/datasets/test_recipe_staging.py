"""CLIDATA-04 staging recipe — ROADMAP §29 SC1 snapshot. VERLET_STAGING_TEST=1 to enable."""
import os

import pytest

pytestmark = pytest.mark.staging

STAGING_GATE = os.getenv("VERLET_STAGING_TEST") == "1"


@pytest.mark.xfail(reason="Phase 29 implementation pending — green by Plan 04 Task 1", strict=True)
@pytest.mark.skipif(not STAGING_GATE, reason="VERLET_STAGING_TEST=1 not set")
def test_staging_pick_and_place_yam_since_april():
    """`verlet datasets list --task pick-and-place --robot yam --since 2026-04-01 --limit 50`
    against staging-api.verlet.co snapshots the slug list against a fixture."""
    raise NotImplementedError("Plan 04 Task 1")
