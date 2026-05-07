"""Unit tests for the MDX → bash recipe extractor (Plan 30-13 Task 4).

These tests pin the ``bash recipe`` code-fence convention established in
Plan 30-11 (CLIDIST-06): only fences that explicitly carry the ``recipe`` tag
are runnable; plain ``bash`` fences are illustrative-only (Pitfall 5 escape
hatch). The recipe-CI walker must skip plain ``bash`` and pick up only
``bash recipe``.
"""

from tests.recipes.run_recipes import extract_recipes


def test_extracts_bash_recipe():
    mdx = "```bash recipe\nverlet auth login\n```"
    assert extract_recipes(mdx) == ["verlet auth login"]


def test_skips_plain_bash():
    mdx = "```bash\necho 'illustrative only'\n```"
    assert extract_recipes(mdx) == []


def test_finds_multiple_recipes():
    mdx = (
        "Some text.\n"
        "```bash recipe\nverlet auth login\n```\n"
        "\n"
        "More text.\n"
        "```bash recipe\nverlet --version\n```\n"
    )
    assert extract_recipes(mdx) == ["verlet auth login", "verlet --version"]


def test_strips_trailing_whitespace():
    mdx = "```bash recipe\nverlet --version\n   \n```"
    assert extract_recipes(mdx)[0] == "verlet --version"
