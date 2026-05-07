"""CLIBUNDLE-05/06 staging recipe -- Plan 30-09 Task 3 + ROADMAP §30 SC2.

Gated by ``VERLET_STAGING_TEST=1`` AND ``VERLET_STAGING_BUNDLE_CODE`` so
local ``pytest tests/`` invocations skip both staging cases. Recipe-CI
(Plan 30-13) sets the env vars on the runner.

Three cases:

* ``test_redeem_list_download_end_to_end`` -- happy-path: redeem a known
  staging code, list shows the bundle, download fetches per-dataset content
  to a tmp dir, bundle_manifest.json present (D-BUNDLE4).
* ``test_redeem_list_download_with_format_hdf5`` -- same flow with
  ``--format hdf5``; verifies CLIBUNDLE-05's --format fan-out works
  end-to-end against staging.
* ``test_raw_rejected_without_network`` -- D-BUNDLE3 byte-exact: even on
  staging, ``--variant raw`` exits 2 with the verbatim error and ZERO
  network calls. Runs unconditionally (no staging gate) so the no-network
  invariant is checked even in local pytest runs that lack the staging
  bundle code.

Per project memory ``run-backfills-on-railway`` these run on github-hosted
runners hitting staging directly; we do not burn local staging resources.
The bundle code is rotatable via ``VERLET_STAGING_BUNDLE_CODE`` so the
staging team can refresh the fixture without code changes.
"""
from __future__ import annotations

import json
import os
import subprocess

import pytest


pytestmark = pytest.mark.staging

# Recipe-CI exports VERLET_STAGING_TEST=1 + VERLET_STAGING_BUNDLE_CODE.
STAGING_GATE = os.environ.get("VERLET_STAGING_TEST") == "1"
BUNDLE_CODE = os.environ.get("VERLET_STAGING_BUNDLE_CODE")
STAGING_API_URL = os.environ.get(
    "VERLET_STAGING_API_URL", "https://staging-api.verlet.co"
)

# Verbatim D-BUNDLE3 error -- byte-locked across this scaffold + the unit
# test in test_download.py + the constant in src/verlet/bundles/_validation.py.
EXPECTED_RAW_REJECTION = (
    "bundles are processed-only; --variant raw is not allowed"
)


@pytest.fixture
def staging_env():
    return {**os.environ, "VERLET_API_URL": STAGING_API_URL}


@pytest.mark.skipif(
    not STAGING_GATE or not BUNDLE_CODE,
    reason=(
        "staging tests require VERLET_STAGING_TEST=1 + "
        "VERLET_STAGING_BUNDLE_CODE"
    ),
)
def test_redeem_list_download_end_to_end(staging_env, tmp_path):
    """Redeem -> list -> download -> bundle_manifest.json (CLIBUNDLE-02/03/05)."""
    # 1. Redeem the staging code.
    r = subprocess.run(
        ["verlet", "bundles", "redeem", BUNDLE_CODE],
        env=staging_env, capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 0, (r.stdout, r.stderr)

    # 2. List -- find the redeemed bundle in the JSON output.
    r = subprocess.run(
        ["verlet", "bundles", "list", "--json"],
        env=staging_env, capture_output=True, text=True, timeout=30,
    )
    assert r.returncode == 0, (r.stdout, r.stderr)
    items = json.loads(r.stdout)
    assert isinstance(items, list) and len(items) >= 1, items
    bundle_id = items[0]["bundle_id"]

    # 3. Download to a tmp dir; assert bundle_manifest.json present.
    out = tmp_path / "bundle"
    r = subprocess.run(
        ["verlet", "bundles", "download", bundle_id, "-o", str(out)],
        env=staging_env, capture_output=True, text=True, timeout=900,
    )
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert (out / "bundle_manifest.json").exists(), list(out.iterdir())


@pytest.mark.skipif(
    not STAGING_GATE or not BUNDLE_CODE,
    reason=(
        "staging tests require VERLET_STAGING_TEST=1 + "
        "VERLET_STAGING_BUNDLE_CODE"
    ),
)
def test_redeem_list_download_with_format_hdf5(staging_env, tmp_path):
    """Same flow with --format hdf5 (CLIBUNDLE-05 fan-out + Plan 30-04 polling)."""
    r = subprocess.run(
        ["verlet", "bundles", "redeem", BUNDLE_CODE],
        env=staging_env, capture_output=True, text=True, timeout=60,
    )
    assert r.returncode == 0, (r.stdout, r.stderr)

    r = subprocess.run(
        ["verlet", "bundles", "list", "--json"],
        env=staging_env, capture_output=True, text=True, timeout=30,
    )
    assert r.returncode == 0, (r.stdout, r.stderr)
    items = json.loads(r.stdout)
    assert len(items) >= 1
    bundle_id = items[0]["bundle_id"]

    out = tmp_path / "bundle-hdf5"
    r = subprocess.run(
        [
            "verlet", "bundles", "download", bundle_id,
            "--format", "hdf5",
            "-o", str(out),
            "--quiet",
        ],
        env=staging_env, capture_output=True, text=True, timeout=900,
    )
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert (out / "bundle_manifest.json").exists(), list(out.iterdir())


def test_raw_rejected_without_network():
    """D-BUNDLE3 byte-exact: --variant raw exits 2 with verbatim error.

    Runs unconditionally -- the validator is not network-coupled, so this
    invariant must hold whether or not the staging gate is set. The error
    is byte-asserted against the constant in src/verlet/bundles/_validation.py.
    """
    r = subprocess.run(
        [
            "verlet", "bundles", "download", "any-bundle-id",
            "--variant", "raw",
        ],
        env={**os.environ, "VERLET_API_URL": STAGING_API_URL},
        capture_output=True, text=True, timeout=10,
    )
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert r.stderr.strip() == EXPECTED_RAW_REJECTION, r.stderr
