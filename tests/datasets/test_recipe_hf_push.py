"""Plan 30-05 Task 3 — staging-CI matrix for `verlet datasets push --to hf://`.

D-DIST4 staging-only recipe-CI test: skipped by default, runs against
``staging-api.verlet.co`` when ``VERLET_STAGING_TEST=1`` AND ``HF_TOKEN`` are
set. Plan 30-13 (recipe-CI) wires the env vars in CI yaml.

Two sequential assertions per run:

  1. ``verlet datasets push <slug> --to huggingface://verlet-staging-test/<random>``
     exits 0 against staging-api (D-FORMAT2 + Plan 30-01 server endpoint).
  2. ``HEAD https://huggingface.co/datasets/verlet-staging-test/<random>``
     returns 200 — proves the push actually created the destination repo
     (closes 30-RESEARCH.md Pitfall 6 from staging side).

Verlet-staging-test is the canonical sandbox HF org for these tests; it is
expected to exist + accept dataset uploads from the staging service account.
The random suffix prevents cross-run collisions when CI runs in parallel.
"""
from __future__ import annotations

import os
import subprocess
import time
import uuid

import httpx
import pytest

pytestmark = pytest.mark.staging

SLUG = os.environ.get(
    "VERLET_STAGING_FIXTURE_SLUG", "staging-fixture-teleop-001"
)
HF_TEST_ORG = os.environ.get("VERLET_HF_TEST_ORG", "verlet-staging-test")


@pytest.fixture(autouse=True)
def _staging_gate():
    """Skip unless both VERLET_STAGING_TEST=1 and HF_TOKEN are set."""
    if os.environ.get("VERLET_STAGING_TEST") != "1":
        pytest.skip("staging tests require VERLET_STAGING_TEST=1")
    if os.environ.get("HF_TOKEN") is None:
        pytest.skip("HF_TOKEN env required for HF push staging test")


def test_push_to_huggingface():
    """End-to-end staging push: CLI exits 0 + HF repo reachable."""
    repo = f"recipe-{uuid.uuid4().hex[:8]}"
    target = f"huggingface://{HF_TEST_ORG}/{repo}"
    result = subprocess.run(
        ["verlet", "datasets", "push", SLUG, "--to", target],
        env={**os.environ, "VERLET_API_URL": "https://staging-api.verlet.co"},
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, f"push failed: {result.stderr}"

    # Verify the destination repo materialized on HF Hub.
    url = f"https://huggingface.co/datasets/{HF_TEST_ORG}/{repo}"
    for _ in range(10):
        r = httpx.head(url, follow_redirects=True)
        if r.status_code == 200:
            return
        time.sleep(3)
    pytest.fail(f"HF repo {url} did not become reachable after push")
