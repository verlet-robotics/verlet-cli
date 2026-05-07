"""Schema-level tests for ``.github/workflows/release.yml`` (Plan 30-13).

These tests parse release.yml and assert the structural invariants the plan
locks in: brew-bump job (Task 3), recipe-staging matrix (Task 5), correct
trigger surface, draft-PR skip, and required env wiring. Pure structural
checks — no GitHub API calls, no actions network access.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1] / ".github" / "workflows" / "release.yml"
)


@pytest.fixture(scope="module")
def workflow() -> dict:
    return yaml.safe_load(WORKFLOW_PATH.read_text())


# YAML parses the unquoted ``on:`` key as Python ``True``. Helper to dodge that.
def _on(workflow: dict) -> dict:
    return workflow.get("on") or workflow.get(True)


# ---------------------------------------------------------------------------
# Trigger surface (Task 5: PR trigger added alongside existing tag push)
# ---------------------------------------------------------------------------


def test_workflow_triggers_on_tag_push_and_pr(workflow: dict):
    on = _on(workflow)
    assert "push" in on
    assert "pull_request" in on
    pr_types = on["pull_request"]["types"]
    assert "opened" in pr_types
    assert "synchronize" in pr_types
    assert "ready_for_review" in pr_types


# ---------------------------------------------------------------------------
# Task 3: brew-bump job (auto-bump Homebrew formula on PyPI release)
# ---------------------------------------------------------------------------


def test_brew_bump_job_exists(workflow: dict):
    assert "brew-bump" in workflow["jobs"]


def test_brew_bump_uses_dawidd6_action(workflow: dict):
    job = workflow["jobs"]["brew-bump"]
    uses = [step.get("uses", "") for step in job["steps"]]
    assert any("dawidd6/action-homebrew-bump-formula" in u for u in uses), uses


def test_brew_bump_only_on_tag_push(workflow: dict):
    job = workflow["jobs"]["brew-bump"]
    # Must guard so manual workflow_dispatch / PR runs don't bump the formula.
    assert "startsWith(github.ref, 'refs/tags/v')" in job["if"]


def test_brew_bump_creates_pr_not_direct_push(workflow: dict):
    """PR-with-auto-merge per CONTEXT.md Claude's Discretion — never direct push."""
    job = workflow["jobs"]["brew-bump"]
    bump_step = next(
        s for s in job["steps"] if "dawidd6/action-homebrew-bump-formula" in s.get("uses", "")
    )
    assert bump_step["with"]["create_pullrequest"] is True


def test_brew_bump_uses_tap_token_secret(workflow: dict):
    job = workflow["jobs"]["brew-bump"]
    bump_step = next(
        s for s in job["steps"] if "dawidd6/action-homebrew-bump-formula" in s.get("uses", "")
    )
    assert "HOMEBREW_TAP_GITHUB_TOKEN" in bump_step["with"]["token"]


def test_brew_bump_targets_correct_tap(workflow: dict):
    job = workflow["jobs"]["brew-bump"]
    bump_step = next(
        s for s in job["steps"] if "dawidd6/action-homebrew-bump-formula" in s.get("uses", "")
    )
    assert bump_step["with"]["tap"] == "verlet-robotics/homebrew-verlet"
    assert bump_step["with"]["formula"] == "verlet"


# ---------------------------------------------------------------------------
# Task 5: recipe-staging matrix (D-DIST4 quality gate)
# ---------------------------------------------------------------------------


def test_recipe_staging_job_exists(workflow: dict):
    assert "recipe-staging" in workflow["jobs"]


def test_recipe_staging_skips_drafts(workflow: dict):
    job = workflow["jobs"]["recipe-staging"]
    assert (
        job["if"]
        == "github.event_name == 'pull_request' && github.event.pull_request.draft == false"
    )


def test_recipe_staging_matrix_is_4_combinations(workflow: dict):
    job = workflow["jobs"]["recipe-staging"]
    matrix = job["strategy"]["matrix"]
    assert sorted(matrix["os"]) == ["macos-latest", "ubuntu-latest"]
    assert sorted(matrix["python-version"]) == ["3.11", "3.12"]


def test_recipe_staging_checks_out_verlet_server(workflow: dict):
    job = workflow["jobs"]["recipe-staging"]
    checkouts = [s for s in job["steps"] if s.get("uses", "").startswith("actions/checkout")]
    # First checks out verlet-cli (default, no `with`), second checks out verlet-server.
    assert len(checkouts) >= 2
    verlet_server_checkout = next(
        c for c in checkouts if c.get("with", {}).get("repository") == "verlet-robotics/verlet-server"
    )
    assert verlet_server_checkout["with"]["path"] == "verlet-server"
    assert verlet_server_checkout["with"]["ref"] == "main"


def test_recipe_staging_sets_required_env(workflow: dict):
    job = workflow["jobs"]["recipe-staging"]
    run_step = next(
        s for s in job["steps"]
        if "pytest tests/recipes" in s.get("run", "") or s.get("name", "").lower().startswith("run all recipes")
    )
    env = run_step["env"]
    assert env["VERLET_STAGING_TEST"] == "1"
    assert env["VERLET_API_URL"] == "https://staging-api.verlet.co"
    assert env["VERLET_PROFILE"] == "ci"
    # VERLET_DOCS_CONTENT_ROOT must point at the verlet-server checkout.
    assert "verlet-server/frontend/docs/content/cli" in env["VERLET_DOCS_CONTENT_ROOT"]


def test_recipe_staging_runs_pytest_with_marker(workflow: dict):
    job = workflow["jobs"]["recipe-staging"]
    run_step = next(s for s in job["steps"] if "pytest tests/recipes" in s.get("run", ""))
    assert "-m staging" in run_step["run"]
