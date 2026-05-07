"""CLIDATA-07 staging recipe — Phase 30 SC2 format-matrix snapshot.

Gated by ``VERLET_STAGING_TEST=1`` so local ``pytest tests/`` invocations skip
all 8 cases. Recipe-CI (Plan 30-13) sets the env var and exports a
``VERLET_PROFILE`` whose access token is signed with a staging PAT.

Each case runs ``verlet datasets download <slug> --format <fmt> -o <tmp>``
against ``staging-api.verlet.co``. Native (``lerobot-v2``) returns 200 +
manifest immediately; the other 7 take the 202 + job-id polling branch.
The test asserts only:

* exit code 0 (the recipe finished)
* the output directory exists and contains at least one file

Per project memory ``run-backfills-on-railway`` these tests run only in CI
(github-hosted runners hitting staging directly); we do **not** burn local
staging resources. The staging fixture slug is overridable via
``VERLET_STAGING_FIXTURE_SLUG`` so the staging team can rotate the fixture
without code changes.
"""
from __future__ import annotations

import os
import subprocess

import pytest


pytestmark = pytest.mark.staging

# Recipe-CI exports VERLET_STAGING_TEST=1 (Plan 30-13). Local invocations
# leave it unset so all 8 cases skip.
STAGING_GATE = os.environ.get("VERLET_STAGING_TEST") == "1"
SLUG = os.environ.get(
    "VERLET_STAGING_FIXTURE_SLUG", "staging-fixture-teleop-001"
)
STAGING_API_URL = os.environ.get(
    "VERLET_STAGING_API_URL", "https://staging-api.verlet.co"
)

FORMATS: tuple[str, ...] = (
    "lerobot-v2",
    "lerobot-v3",
    "hdf5",
    "zarr",
    "rlds",
    "rosbag",
    "robodm",
    "egomimic",
)


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.skipif(
    not STAGING_GATE, reason="staging tests require VERLET_STAGING_TEST=1",
)
def test_download_with_format(fmt: str, tmp_path):
    """``verlet datasets download <slug> --format <fmt>`` against staging.

    The recipe-CI matrix in Plan 30-13 invokes this test once per (Python,
    OS) pair. A single failure across 8 formats blocks merge per D-DIST4.
    """
    out = tmp_path / fmt
    env = {
        **os.environ,
        "VERLET_API_URL": STAGING_API_URL,
    }
    result = subprocess.run(
        [
            "verlet",
            "datasets",
            "download",
            SLUG,
            "--format",
            fmt,
            "-o",
            str(out),
            # --quiet keeps CI logs readable; the verbose path is exercised
            # by unit tests in test_download_format.py.
            "--quiet",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert result.returncode == 0, (
        f"format {fmt} failed: stdout={result.stdout!r} "
        f"stderr={result.stderr!r}"
    )
    assert out.exists(), f"output dir missing for {fmt}"
    # The download driver creates a per-dataset subdirectory; just confirm
    # ANY file ended up on disk under the format-specific output root.
    written = list(out.rglob("*"))
    assert any(p.is_file() for p in written), (
        f"no files written under {out} for format {fmt}"
    )


def test_format_matrix_collects_eight_cases():
    """Sanity: pytest must see all 8 parametrized cases regardless of gate.

    Runs unconditionally (no skipif) so collection-only invocations
    (``pytest --collect-only``) verify the matrix shape. The actual
    download-vs-staging assertions still skip without VERLET_STAGING_TEST=1.
    """
    assert len(FORMATS) == 8
    # Lock the order — recipe-CI matrix order in Plan 30-13 mirrors this
    # tuple, so any reorder must update both sides in lockstep.
    assert FORMATS == (
        "lerobot-v2",
        "lerobot-v3",
        "hdf5",
        "zarr",
        "rlds",
        "rosbag",
        "robodm",
        "egomimic",
    )
