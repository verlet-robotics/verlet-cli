"""MDX → bash recipe extractor + runner. Powers recipe-CI per D-DIST4.

Extracts every ```bash recipe``` code fence from a tree of MDX docs, then
shells each block out as a real bash invocation. The recipe-CI matrix in
``.github/workflows/release.yml`` runs this against staging-api.verlet.co on
every non-draft PR — failures block merge (Plan 30-13, CLIDIST-07).

The convention is locked at the docs export site: Plan 30-11's
``verlet docs export`` emits ```bash recipe``` for runnable examples and
leaves plain ```bash``` for illustrative-only fragments (Pitfall 5 escape
hatch). This walker only picks up the runnable form; plain ``bash`` is
ignored on purpose.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Iterator

# Match a fenced code block whose info string is exactly ``bash recipe``
# (allowing trailing whitespace). The fence-open line must end with a newline
# so we don't accidentally match a non-fence backtick run mid-paragraph.
RECIPE_BLOCK_RE = re.compile(
    r"```bash\s+recipe[^\n]*\n(.*?)```",
    re.DOTALL,
)


def extract_recipes(mdx_text: str) -> list[str]:
    """Return list of bash recipe code strings from MDX content.

    Pitfall 5: only fences tagged ``bash recipe`` are returned. Plain ``bash``
    fences are illustrative-only and intentionally skipped.
    """
    return [m.strip() for m in RECIPE_BLOCK_RE.findall(mdx_text)]


def discover_recipes(content_root: Path) -> Iterator[tuple[Path, int, str]]:
    """Yield ``(file_path, recipe_index_in_file, recipe_body)`` for every
    bash recipe block under ``content_root`` (recursively, sorted)."""
    for mdx in sorted(content_root.rglob("*.mdx")):
        text = mdx.read_text()
        for i, body in enumerate(extract_recipes(text)):
            yield (mdx, i, body)


def run_recipe(
    body: str,
    *,
    env: dict,
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    """Run a single recipe body via ``bash -c``.

    Captures stdout/stderr so the gathered failure report (test_recipes_run)
    can surface them per-recipe. Returns the CompletedProcess unmodified;
    callers inspect ``returncode``.
    """
    return subprocess.run(
        ["bash", "-c", body],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
