"""Recipe-CI staging tests.

This module is gated by ``VERLET_STAGING_TEST=1`` so the unit-test suite stays
hermetic. The recipe-staging job in ``.github/workflows/release.yml`` is the
only consumer that flips the gate on; locally a maintainer can run::

    VERLET_STAGING_TEST=1 \\
    VERLET_DOCS_CONTENT_ROOT=$HOME/Documents/GitHub/verlet-server-gsd/teleop-manager/frontend/docs/content \\
    uv run pytest tests/recipes/ -v -m staging

The job walks every ``*.mdx`` under ``VERLET_DOCS_CONTENT_ROOT``, extracts
every ```bash recipe``` fence, and shells each one out with
``VERLET_API_URL=https://staging-api.verlet.co`` + ``VERLET_PROFILE=ci``. Any
non-zero exit fails the job — that's the D-DIST4 quality gate.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.recipes.run_recipes import discover_recipes, run_recipe

pytestmark = pytest.mark.staging


@pytest.fixture(autouse=True)
def _staging_gate():
    if os.environ.get("VERLET_STAGING_TEST") != "1":
        pytest.skip("staging tests require VERLET_STAGING_TEST=1")


@pytest.fixture
def staging_env() -> dict:
    env = {**os.environ, "VERLET_API_URL": "https://staging-api.verlet.co"}
    # VERLET_PROFILE=ci is the Doppler-injected service-account profile on the
    # runner; default it here so local resumption Just Works.
    env.setdefault("VERLET_PROFILE", "ci")
    return env


def _content_root() -> Path:
    """Resolve the docs-content root for the recipe walker.

    Prefers the explicit env var (release.yml sets this from the verlet-server
    sibling checkout). Falls back to a sibling layout for local runs.
    """
    root = os.environ.get("VERLET_DOCS_CONTENT_ROOT")
    if root:
        return Path(root)
    candidate = (
        Path(__file__).resolve().parents[3]
        / "verlet-server"
        / "teleop-manager"
        / "frontend"
        / "docs"
        / "content"
    )
    if candidate.exists():
        return candidate
    pytest.skip(
        "VERLET_DOCS_CONTENT_ROOT not set and no sibling verlet-server checkout"
    )


@pytest.fixture
def content_root() -> Path:
    return _content_root()


def test_all_recipes_run(staging_env: dict, content_root: Path):
    recipes = list(discover_recipes(content_root))
    assert recipes, f"no recipes found under {content_root}"
    failures: list[tuple[Path, int, str, str, str]] = []
    for path, idx, body in recipes:
        result = run_recipe(body, env=staging_env)
        if result.returncode != 0:
            failures.append((path, idx, body, result.stdout, result.stderr))
    if failures:
        msg = "\n\n".join(
            f"{p.name} recipe #{i} FAILED:\n  body: {b!r}\n  stdout: {out}\n  stderr: {err}"
            for p, i, b, out, err in failures
        )
        pytest.fail(msg)
