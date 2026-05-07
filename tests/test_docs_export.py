"""Plan 30-11 Task 1 (CLIDIST-06, D-DIST3): walker tests for verlet docs export.

The walker recurses through ``cli.commands`` and emits one Fumadocs-flavored
MDX file per Click command. Frontmatter shape mirrors the Phase 34 docs site
(``frontend/docs/content/docs/install.mdx`` is the canonical reference).
Bash code blocks in command epilogs are normalized to ``bash recipe`` so the
Plan 30-13 recipe-CI walker can pick them up.

Task 2 adds further tests for production-command epilogs in this same file.
"""
from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from verlet.cli import cli


def _run_export(tmp_path: Path) -> Path:
    out = tmp_path / "cli-mdx"
    runner = CliRunner()
    result = runner.invoke(cli, ["docs", "export", "--out", str(out)])
    assert result.exit_code == 0, result.output + (result.stderr or "")
    return out


def _count_commands(grp) -> int:
    """Count leaf Click commands recursively under ``grp`` (excludes groups themselves)."""
    import click

    n = 0
    for _name, cmd in grp.commands.items():
        if isinstance(cmd, click.Group):
            n += _count_commands(cmd)
        else:
            n += 1
    return n


# ---------------------------------------------------------------------------
# Behavior tests (Plan 30-11 Task 1) -- walker contract.
# ---------------------------------------------------------------------------


def test_docs_export_writes_file_per_command(tmp_path):
    """Walker emits one .mdx per leaf Click command."""
    out = _run_export(tmp_path)
    n_commands = _count_commands(cli)
    n_files = sum(1 for _ in out.rglob("*.mdx"))
    assert n_files >= n_commands, (
        f"expected >= {n_commands} mdx files, got {n_files}"
    )


def test_frontmatter_shape_matches_phase34(tmp_path):
    """Frontmatter is exactly ``---\\ntitle: ...\\ndescription: ...\\n---``."""
    out = _run_export(tmp_path)
    sample = out / "auth" / "login.mdx"
    assert sample.exists()
    content = sample.read_text()
    lines = content.splitlines()
    assert lines[0] == "---"
    assert lines[1].startswith("title: verlet auth login")
    assert lines[2].startswith("description: ")
    assert lines[3] == "---"


def test_auth_login_mdx_at_nested_path(tmp_path):
    """Group nesting maps to directory tree: ``auth login`` -> ``auth/login.mdx``."""
    out = _run_export(tmp_path)
    assert (out / "auth" / "login.mdx").exists()


def test_bundles_redeem_has_required_sections(tmp_path):
    """Each MDX has Synopsis, Description, Options sections."""
    out = _run_export(tmp_path)
    body = (out / "bundles" / "redeem.mdx").read_text()
    assert "## Synopsis" in body
    assert "## Description" in body
    assert "## Options" in body


def test_command_with_epilog_has_examples_section(tmp_path):
    """A command with epilog set emits an Examples section (verifies via the
    docs_export Click command which carries the test-only sentinel epilog)."""
    out = _run_export(tmp_path)
    candidates = list(out.rglob("*.mdx"))
    found_examples = False
    for path in candidates:
        text = path.read_text()
        if "## Examples" in text:
            found_examples = True
            break
    assert found_examples, "expected at least one MDX with an Examples section"


def test_command_without_epilog_omits_examples_section(tmp_path):
    """A command with no epilog must NOT emit an empty Examples section."""
    out = _run_export(tmp_path)
    # ``verlet update`` ships without an epilog (leaf command at root).
    body = (out / "update.mdx").read_text()
    assert "## Examples" not in body, (
        "Examples section must be omitted when epilog is None"
    )


def test_bash_recipe_marker_used_in_epilogs(tmp_path):
    """Bash blocks in epilog text are normalized to ``bash recipe`` for recipe-CI (Pitfall 5)."""
    out = _run_export(tmp_path)
    matches = [p for p in out.rglob("*.mdx") if "```bash recipe" in p.read_text()]
    assert len(matches) >= 1, (
        "expected >=1 file with ```bash recipe (Plan 30-13 marker)"
    )


def test_walker_recurses_into_subgroups(tmp_path):
    """Nested groups (auth tokens) emit ``auth/tokens/<cmd>.mdx``."""
    out = _run_export(tmp_path)
    tokens_dir = out / "auth" / "tokens"
    assert tokens_dir.is_dir()
    assert (tokens_dir / "create.mdx").exists()
    assert (tokens_dir / "list.mdx").exists()


def test_golden_snapshot_login_and_redeem(tmp_path):
    """Curated subset matches expected MDX byte-for-byte."""
    out = _run_export(tmp_path)
    fixtures = Path(__file__).parent / "fixtures" / "docs"

    for rel in ("auth/login.mdx", "bundles/redeem.mdx"):
        live = (out / rel).read_text()
        expected = (fixtures / f"expected_{rel.replace('/', '_')}").read_text()
        assert live == expected, (
            f"snapshot drift in {rel}; regenerate fixture if change is intentional"
        )
