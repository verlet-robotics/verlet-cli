"""Plan 30-11 Task 1 (CLIDIST-06, D-DIST3): walker tests for verlet docs export.

The walker recurses through ``cli.commands`` and emits one Fumadocs-flavored
MDX file per Click command. Frontmatter shape mirrors the Phase 34 docs site
(``frontend/docs/content/docs/install.mdx`` is the canonical reference).
Bash code blocks in command epilogs are normalized to ``bash recipe`` so the
Plan 30-13 recipe-CI walker can pick them up.

Task 2 adds further tests for production-command epilogs in this same file.

MDX-safety section (bottom) covers the YAML / angle-bracket escape pass that
keeps the Fumadocs build green when Click ``help=`` / ``epilog=`` strings
contain colons or ``<placeholder>`` tokens.
"""
from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from verlet.cli import cli
from verlet.docs_export import (
    _extract_footer,
    _mdx_escape_prose,
    _yaml_scalar,
)


def _run_export(tmp_path: Path) -> Path:
    out = tmp_path / "cli-mdx"
    runner = CliRunner()
    result = runner.invoke(cli, ["docs", "export", "--out", str(out)])
    assert result.exit_code == 0, result.output + (result.stderr or "")
    return out


def _count_commands(grp) -> int:
    """Count leaf Click commands recursively under ``grp``.

    Excludes groups themselves and hidden commands — the walker skips
    hidden commands, so the file count must too.
    """
    import click

    n = 0
    for _name, cmd in grp.commands.items():
        if getattr(cmd, "hidden", False):
            continue
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



# ---------------------------------------------------------------------------
# Plan 30-11 Task 2 -- production-command epilog coverage.
#
# Adds bash recipe blocks to >=6 user-facing commands so the Plan 30-13
# recipe-CI walker has runnable inputs and `verlet <cmd> --help` shows a
# canonical example. Tests pin the recipe contents byte-loosely (substring
# match) so wording can evolve without churn, but the runnable-block count
# is asserted as >=6 to satisfy CLIDIST-06 SC-recipe-coverage.
# ---------------------------------------------------------------------------


def test_help_text_for_auth_login_shows_recipe_marker():
    """`verlet auth login --help` epilog renders the canonical example."""
    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "login", "--help"])
    assert result.exit_code == 0
    assert "verlet auth login" in result.output


def test_help_text_for_datasets_push_shows_hf_recipe(tmp_path):
    """`verlet datasets push` ships an epilog-driven HF push recipe.

    Asserts the generated MDX (not the docstring fallback) carries the
    huggingface://acme/imitate-cube target as a ``bash recipe`` block, so
    Plan 30-13 recipe-CI has a stable runnable block to lift.
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["datasets", "push", "--help"])
    assert result.exit_code == 0
    assert "huggingface://acme/imitate-cube" in result.output

    out = _run_export(tmp_path)
    body = (out / "datasets" / "push.mdx").read_text()
    assert "## Examples" in body, "datasets push must have epilog -> Examples section"
    assert "```bash recipe" in body
    assert "huggingface://acme/imitate-cube" in body


def test_help_text_for_bundles_redeem_shows_runnable_example(tmp_path):
    """`verlet bundles redeem` ships an epilog-driven runnable example.

    Like the datasets-push test above, asserts the recipe lives in the
    generated MDX, not just the docstring.
    """
    runner = CliRunner()
    result = runner.invoke(cli, ["bundles", "redeem", "--help"])
    assert result.exit_code == 0
    assert "verlet bundles redeem" in result.output

    out = _run_export(tmp_path)
    body = (out / "bundles" / "redeem.mdx").read_text()
    assert "## Examples" in body
    assert "```bash recipe" in body


def test_at_least_six_commands_have_recipe_markers(tmp_path):
    """>=6 commands ship ```bash recipe blocks for recipe-CI (Plan 30-13)."""
    out = _run_export(tmp_path)
    files_with_recipe = [
        p.relative_to(out)
        for p in out.rglob("*.mdx")
        if "```bash recipe" in p.read_text()
    ]
    assert len(files_with_recipe) >= 6, (
        f"only {len(files_with_recipe)} commands with bash recipe blocks: "
        f"{files_with_recipe}"
    )


# ---------------------------------------------------------------------------
# MDX-safety tests — frontmatter YAML quoting + angle-bracket escape.
#
# These guard against the May 2026 docs deploy failure where Fumadocs' MDX 3
# parser broke on (a) unquoted colons in `description:` and (b) `<word>` /
# `<digit>` placeholders in Click help / epilog text. The escape pass is
# tested at unit level (the helpers) and end-to-end (the generated MDX).
# ---------------------------------------------------------------------------


def test_yaml_scalar_quotes_colons():
    """Description with a colon must round-trip through YAML cleanly."""
    s = _yaml_scalar("Show bundle detail: included datasets, license, citation.")
    assert s.startswith("'") and s.endswith("'")
    assert ": included" in s
    # The internal text must NOT contain an unescaped sequence that yaml would
    # parse as a key:value mapping at the top level.


def test_yaml_scalar_doubles_embedded_single_quotes():
    """`'` inside a single-quoted scalar is escaped as `''`."""
    assert _yaml_scalar("don't") == "'don''t'"


def test_yaml_frontmatter_parses_with_pyyaml(tmp_path):
    """Every generated frontmatter block parses as valid YAML."""
    yaml = __import__("yaml")
    out = _run_export(tmp_path)
    for path in out.rglob("*.mdx"):
        text = path.read_text()
        # Frontmatter is everything between the first two `---` lines.
        assert text.startswith("---\n"), path
        end = text.index("\n---\n", 4)
        block = text[4:end]
        parsed = yaml.safe_load(block)
        assert isinstance(parsed, dict), f"{path}: {parsed!r}"
        assert "title" in parsed and "description" in parsed, path
        # Description must be a string scalar, not a nested mapping introduced
        # by an unquoted colon.
        assert isinstance(parsed["description"], str), path


def test_mdx_escape_prose_escapes_angle_brackets_outside_code():
    """`<word>` / `<digit>` in prose becomes `\\<...>`; backslash-`<` renders literally."""
    src = "Use <slug> or <bundle_uuid> after waiting <24h."
    out = _mdx_escape_prose(src)
    assert "\\<slug>" in out
    assert "\\<bundle_uuid>" in out
    assert "\\<24h" in out
    assert "<slug>" not in out.replace("\\<slug>", "")  # only escaped form remains


def test_mdx_escape_prose_escapes_curly_braces_outside_code():
    """`{expr}` in prose becomes `\\{expr}` so MDX doesn't try to resolve it as JSX."""
    src = "Output directory (per-dataset subdir rooted at {output}/{slug}/)."
    out = _mdx_escape_prose(src)
    assert "\\{output}" in out
    assert "\\{slug}" in out
    assert "{output}" not in out.replace("\\{output}", "")


def test_mdx_escape_prose_preserves_curly_braces_in_inline_code():
    """`{id}` inside a code span stays literal — MDX doesn't parse JSX inside code."""
    src = "The CLI polls ``/downloads/jobs/{id}`` until ready."
    out = _mdx_escape_prose(src)
    assert "``/downloads/jobs/{id}``" in out, "code-span `{id}` must not be escaped"
    assert "\\{" not in out


def test_mdx_escape_prose_preserves_inline_code_spans():
    """Backticked `<slug>` stays as-is; only prose `<x>` gets escaped."""
    src = "Pass `<slug>` to download; the URL looks like <https://verlet.co>."
    out = _mdx_escape_prose(src)
    assert "`<slug>`" in out, "inline code span must not be escaped"
    assert "\\<https://verlet.co>" in out


def test_mdx_escape_prose_preserves_fenced_code_blocks():
    """Inside ```` ``` ```` fences, `<x>` is left alone."""
    src = "Run this:\n```bash\nverlet datasets info <slug>\n```\nThen check <state>."
    out = _mdx_escape_prose(src)
    assert "verlet datasets info <slug>" in out, "fence content must not be escaped"
    assert "\\<state>" in out


def _bare_jsx_chars_in_prose(line: str) -> list[tuple[int, str]]:
    """Return positions of unescaped JSX-significant chars (``<``, ``{``) in prose.

    Handles single-backtick and multi-backtick inline code spans (CommonMark:
    opening run of N backticks closes on next run of exactly N backticks). A
    character inside a span is not flagged.
    """
    hits: list[tuple[int, str]] = []
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        if ch == "`":
            j = i
            while j < n and line[j] == "`":
                j += 1
            run_len = j - i
            k = j
            matched = False
            while k < n:
                if line[k] != "`":
                    k += 1
                    continue
                m = k
                while m < n and line[m] == "`":
                    m += 1
                if m - k == run_len:
                    i = m
                    matched = True
                    break
                k = m
            if matched:
                continue
            i = j
            continue
        if ch in ("<", "{") and (i == 0 or line[i - 1] != "\\"):
            hits.append((i, ch))
        i += 1
    return hits


def test_generated_mdx_has_no_bare_jsx_triggers_in_prose(tmp_path):
    """End-to-end: no MDX in the live export has an un-escaped prose ``<`` or ``{``."""
    out = _run_export(tmp_path)
    offenders: list[tuple[Path, int, str, str]] = []
    for path in out.rglob("*.mdx"):
        in_fence = False
        in_frontmatter = False
        frontmatter_close_seen = False
        for i, line in enumerate(path.read_text().splitlines(), start=1):
            if line == "---":
                if not in_frontmatter and not frontmatter_close_seen:
                    in_frontmatter = True
                elif in_frontmatter:
                    in_frontmatter = False
                    frontmatter_close_seen = True
                continue
            if in_frontmatter:
                continue
            stripped = line.lstrip()
            if stripped.startswith("```") or stripped.startswith("~~~"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            for _pos, ch in _bare_jsx_chars_in_prose(line):
                offenders.append((path.relative_to(out), i, ch, line))
                break
    assert not offenders, (
        "Un-escaped JSX-significant char in generated MDX prose breaks the Fumadocs build:\n"
        + "\n".join(f"  {p}:{n}: [{c}] {ln}" for p, n, c, ln in offenders[:10])
    )


# ---------------------------------------------------------------------------
# mirror_changelog tests.
# ---------------------------------------------------------------------------


def _run_mirror(tmp_path: Path, changelog_text: str) -> Path:
    src = tmp_path / "CHANGELOG.md"
    dst = tmp_path / "out" / "changelog" / "index.mdx"
    src.write_text(changelog_text)
    result = CliRunner().invoke(
        cli,
        ["docs", "mirror-changelog", "--in", str(src), "--out", str(dst)],
    )
    assert result.exit_code == 0, result.output + (result.stderr or "")
    return dst


def test_mirror_changelog_writes_valid_frontmatter(tmp_path):
    """Output starts with a YAML frontmatter parseable to title+description."""
    yaml = __import__("yaml")
    dst = _run_mirror(tmp_path, "## 0.1.0\n\n- Initial release.\n")
    text = dst.read_text()
    assert text.startswith("---\n")
    end = text.index("\n---\n", 4)
    parsed = yaml.safe_load(text[4:end])
    assert parsed["title"] == "Changelog"
    assert isinstance(parsed["description"], str)


def test_mirror_changelog_escapes_angle_brackets(tmp_path):
    """Bare `<24h` and `<slug>` in CHANGELOG prose are MDX-escaped."""
    src = (
        "## 0.8.6\n\n"
        "- Every just-tagged version is <24h old by definition.\n"
        "- `verlet datasets info <slug>` was leaking grants.\n"
    )
    dst = _run_mirror(tmp_path, src)
    text = dst.read_text()
    assert "\\<24h" in text, "prose `<24h` must be escaped"
    assert "`verlet datasets info <slug>`" in text, "code-span `<slug>` stays literal"


def test_mirror_changelog_preserves_footer_on_overwrite(tmp_path):
    """Trailing `---` rule + footer body survive a second-pass overwrite."""
    src = tmp_path / "CHANGELOG.md"
    dst = tmp_path / "out" / "changelog" / "index.mdx"
    dst.parent.mkdir(parents=True)
    # Seed an existing MDX with a footer block past the body.
    dst.write_text(
        "---\n"
        "title: Changelog\n"
        "description: 'old'\n"
        "---\n"
        "\n"
        "## 0.1.0\n\n- Initial.\n"
        "\n---\n\n"
        "See [milestones/v2.2](/docs/milestones/v2.2) for context.\n"
    )
    src.write_text("## 0.2.0\n\n- New release.\n")
    result = CliRunner().invoke(
        cli,
        ["docs", "mirror-changelog", "--in", str(src), "--out", str(dst)],
    )
    assert result.exit_code == 0, result.output
    text = dst.read_text()
    assert "## 0.2.0" in text, "new changelog body must be present"
    assert "milestones/v2.2" in text, "footer link must be preserved"
    # And the footer must come AFTER the body, not BEFORE.
    assert text.index("## 0.2.0") < text.index("milestones/v2.2")


def test_mirror_changelog_no_footer_when_none_existed(tmp_path):
    """If the existing file (or no file) has no body `---` rule, no footer is added."""
    # Pre-existing file without a footer rule:
    src = tmp_path / "CHANGELOG.md"
    dst = tmp_path / "out" / "changelog" / "index.mdx"
    dst.parent.mkdir(parents=True)
    dst.write_text(
        "---\ntitle: Changelog\ndescription: 'old'\n---\n\n## 0.1.0\n\n- Initial.\n"
    )
    src.write_text("## 0.2.0\n\n- New release.\n")
    result = CliRunner().invoke(
        cli,
        ["docs", "mirror-changelog", "--in", str(src), "--out", str(dst)],
    )
    assert result.exit_code == 0, result.output
    text = dst.read_text()
    # The only `---` lines must be the frontmatter open/close — no third one.
    assert text.count("\n---\n") == 1


def test_extract_footer_skips_frontmatter_closing_rule(tmp_path):
    """The closing `---` of frontmatter must NOT be treated as a footer rule."""
    p = tmp_path / "f.mdx"
    p.write_text("---\ntitle: x\ndescription: 'y'\n---\n\nbody only, no footer.\n")
    assert _extract_footer(p) == ""
