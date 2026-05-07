"""CLIDATA-04 staging recipe — ROADMAP §29 SC1 snapshot. VERLET_STAGING_TEST=1 to enable.

Greened in Plan 29-04 Task 3 (manual SC1 spot-check approved 2026-05-07).

This test runs the canonical CLIDATA-04 query against the live
``staging-api.verlet.co`` catalog and asserts the response shape. The slug
list is printed (use ``-s -v``) so the operator can cross-check against the
platform/showcase web UI for the same auth context — that manual eyeball is
the SC1 acceptance gate.

Assertions are intentionally low-fidelity (shape only, no exact slug set)
because staging row-set drift would otherwise cause flakes; the snapshot
match is the human's job.
"""
import os

import pytest

from click.testing import CliRunner

pytestmark = pytest.mark.staging

STAGING_GATE = os.getenv("VERLET_STAGING_TEST") == "1"


@pytest.mark.skipif(not STAGING_GATE, reason="VERLET_STAGING_TEST=1 not set")
def test_staging_pick_and_place_yam_since_april():
    """`verlet datasets list --task pick-and-place --robot yam --since 2026-04-01 --limit 50`
    against staging-api.verlet.co — shape-only assertion, slugs printed for manual SC1 cross-check."""
    from verlet.cli import cli

    cli_runner = CliRunner()
    # Run the canonical CLIDATA-04 query against live staging.
    result = cli_runner.invoke(
        cli,
        [
            "datasets", "list",
            "--task", "pick-and-place",
            "--robot", "yam",
            "--since", "2026-04-01",
            "--limit", "50",
            "--json",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    import json
    items = json.loads(result.output)
    # SC1: result is a list (may be empty if staging has no matching rows;
    # fail loudly only if the response shape is wrong).
    assert isinstance(items, list), f"expected list, got {type(items)}"
    for item in items:
        assert "slug" in item, item
        # SC1 asserts snapshot match against the web UI; record the slug list
        # for manual cross-check (printed on -s / -v invocation).
        print(item["slug"])
