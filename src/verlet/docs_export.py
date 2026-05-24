"""Click -> Fumadocs MDX walker (CLIDIST-06, D-DIST3).

Plan 30-11. ``verlet docs export --out <dir>`` walks the live ``cli.commands``
tree and emits one MDX file per leaf Click command. The shape mirrors the
Phase 34 Fumadocs site (``frontend/docs/content/docs/install.mdx`` is the
canonical reference) so the generated tree drops straight into
``frontend/docs/content/cli/`` without a build break.

Frontmatter (D-DIST3): ``---\\ntitle: <full command>\\ndescription: <one liner>\\n---``.
Body sections: Synopsis / Description / Options / (optional) Examples.

Bash code blocks inside command epilogs are auto-normalized to
``\\`\\`\\`bash recipe`` so the Plan 30-13 recipe-CI walker only runs runnable
blocks (not illustrative fragments). The convention is documented at
RESEARCH.md "Pitfall 5".

MDX safety: Click help text and epilog text routinely contain angle-bracket
placeholders like ``<slug>``, ``<bundle_uuid>``, ``<24h``. MDX 3 parses
``<letter``, ``<digit``, ``</``, and ``<!`` as the start of a JSX/HTML tag and
errors out when no matching close tag follows. Frontmatter ``description``
strings routinely contain colons (``Show bundle detail: ...``) which break
YAML mapping parsing. Both are escaped at write-time below so the docs site
build stays green regardless of what authors write in Click ``help=`` /
``epilog=`` fields.
"""
from __future__ import annotations

from pathlib import Path

import click


_DOCS_EXPORT_EPILOG = """\b
Examples:

```bash
verlet docs export --out frontend/docs/content/cli/
```
"""


_MIRROR_CHANGELOG_EPILOG = """\b
Examples:

```bash
verlet docs mirror-changelog \\
  --in ../verlet-cli/CHANGELOG.md \\
  --out frontend/docs/content/docs/changelog/index.mdx
```
"""


@click.command("export", epilog=_DOCS_EXPORT_EPILOG)
@click.option(
    "--out",
    required=True,
    type=click.Path(path_type=Path),
    help="Output directory for the MDX tree (e.g. frontend/docs/content/cli/).",
)
def docs_export(out: Path) -> None:
    """Regenerate MDX reference pages from the live Click command tree."""
    from verlet.cli import cli  # local import to avoid circular at module load

    out.mkdir(parents=True, exist_ok=True)
    _export_group(cli, [], out)
    click.echo(f"Wrote MDX tree to {out}")


@click.command("mirror-changelog", epilog=_MIRROR_CHANGELOG_EPILOG)
@click.option(
    "--in",
    "in_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to verlet-cli/CHANGELOG.md.",
)
@click.option(
    "--out",
    "out_path",
    required=True,
    type=click.Path(path_type=Path),
    help="Path to the target changelog/index.mdx in the Fumadocs site.",
)
def mirror_changelog(in_path: Path, out_path: Path) -> None:
    """Mirror verlet-cli/CHANGELOG.md into a Fumadocs MDX page.

    Preserves a trailing footer block (everything from the last bare ``---``
    rule in the body of the existing file) so seeded milestone links survive
    release-bot regenerations.
    """
    changelog = in_path.read_text()
    footer = _extract_footer(out_path) if out_path.exists() else ""
    parts: list[str] = [
        "---",
        "title: Changelog",
        "description: "
        + _yaml_scalar(
            "Notable changes to the verlet CLI, mirrored from verlet-cli/CHANGELOG.md."
        ),
        "---",
        "",
        "> Auto-refreshed from `verlet-cli/CHANGELOG.md` on every release.",
        "> Edit the source file in verlet-cli, not this page.",
        "",
        _mdx_escape_prose(changelog).rstrip("\n"),
    ]
    if footer:
        parts.extend(["", footer.rstrip("\n")])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(parts) + "\n")
    click.echo(f"Mirrored CHANGELOG to {out_path}")


def _export_group(grp: click.Group, path: list[str], out_dir: Path) -> None:
    for name, cmd in sorted(grp.commands.items()):
        # Hidden commands stay out of the published docs tree.
        if getattr(cmd, "hidden", False):
            continue
        if isinstance(cmd, click.Group):
            _export_group(cmd, path + [name], out_dir)
        else:
            _export_command(cmd, path + [name], out_dir)


def _export_command(cmd: click.Command, path: list[str], out_dir: Path) -> None:
    title = " ".join(["verlet"] + path)
    rel = "/".join(path) + ".mdx"
    target = out_dir / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    desc = (cmd.help or "").split("\n")[0].strip() or "Verlet CLI command."
    body_desc = _mdx_escape_prose(cmd.help or "_No description._")

    lines: list[str] = [
        "---",
        f"title: {title}",
        f"description: {_yaml_scalar(desc)}",
        "---",
        "",
        f"# `{title}`",
        "",
        "## Synopsis",
        "",
        "```bash",
        f"{title} [OPTIONS]" + _arg_signature(cmd),
        "```",
        "",
        "## Description",
        "",
        body_desc,
        "",
        "## Options",
        "",
        _options_table(cmd),
    ]
    if cmd.epilog:
        lines.extend(
            [
                "",
                "## Examples",
                "",
                _mdx_escape_prose(_normalize_epilog_recipes(cmd.epilog)),
            ]
        )
    target.write_text("\n".join(lines).rstrip() + "\n")


def _arg_signature(cmd: click.Command) -> str:
    """Render positional arguments as ``ARG1 ARG2`` for the synopsis line."""
    args = [p for p in cmd.params if isinstance(p, click.Argument)]
    if not args:
        return ""
    return " " + " ".join(p.name.upper() for p in args)


def _options_table(cmd: click.Command) -> str:
    """Render the ``--name``/type/default/help table for a command's options."""
    rows = [
        "| Name | Type | Default | Description |",
        "|------|------|---------|-------------|",
    ]
    had_option = False
    for p in cmd.params:
        if not isinstance(p, click.Option):
            continue
        had_option = True
        opts = ", ".join(f"`{o}`" for o in p.opts)
        type_name = getattr(p.type, "name", p.type.__class__.__name__)
        # ``False`` is the default for boolean flags; render as empty so the
        # table doesn't shout "False" at the reader. ``None`` and the empty
        # tuple (multi-value defaults) also collapse to empty. Click 8.3+
        # uses a private ``Sentinel.UNSET`` for required-with-no-default; we
        # type-check by class name to avoid importing the private symbol.
        is_unset = type(p.default).__name__ == "Sentinel"
        if is_unset or p.default in (None, (), False):
            default = ""
        else:
            default = f"`{p.default!r}`"
        help_text = (p.help or "").replace("|", "\\|")
        # MDX parses `<word>` inside table cells the same as in prose; escape
        # any bare `<` to stop angle-bracket placeholders (`<bundle_id>` etc.)
        # blowing up the docs build.
        help_text = _escape_prose_line(help_text)
        rows.append(f"| {opts} | {type_name} | {default} | {help_text} |")
    if not had_option:
        rows.append("| _no options_ | | | |")
    return "\n".join(rows)


def _normalize_epilog_recipes(epilog: str) -> str:
    """Promote ```bash code fences to ``bash recipe`` so recipe-CI picks them up.

    Convention (Pitfall 5): runnable blocks are tagged ``bash recipe``;
    illustrative fragments stay plain ``bash``. Authors who write
    ``\\`\\`\\`bash`` in an epilog mean "runnable" — every existing usage in
    this CLI matches that pattern. If a future author needs an illustrative
    fragment they can write a different tag (``\\`\\`\\`shell-output`` etc.)
    or use indented code; this helper only touches the exact ``\\`\\`\\`bash``
    line.
    """
    out_lines: list[str] = []
    for line in epilog.splitlines():
        # Click strips leading indentation from epilogs in some renders, so
        # accept any leading whitespace. Tag is exact "```bash" with optional
        # trailing whitespace; if it already has " recipe" or any other tag,
        # leave it alone.
        stripped = line.rstrip()
        leading = line[: len(line) - len(line.lstrip())]
        if stripped[len(leading):] == "```bash":
            out_lines.append(leading + "```bash recipe")
        else:
            out_lines.append(line)
    return "\n".join(out_lines)


# ---------------------------------------------------------------------------
# MDX / YAML safety helpers.
# ---------------------------------------------------------------------------


def _yaml_scalar(s: str) -> str:
    """Render ``s`` as a single-quoted YAML scalar.

    Single-quoted YAML doesn't interpret colons, hashes, angle brackets or
    backslashes — the only required escape is doubling embedded ``'``.
    Click ``help=`` strings are single-line, so newlines are not a concern.
    """
    return "'" + s.replace("'", "''") + "'"


def _mdx_escape_prose(text: str) -> str:
    """Escape ``<`` outside of code spans / fences so MDX renders it literally.

    MDX 3 parses ``<letter``, ``<digit``, ``</``, and ``<!`` as the start of a
    JSX/HTML tag. Click ``help=`` text and ``epilog=`` text routinely carry
    placeholder tokens like ``<slug>``, ``<bundle_uuid>``, ``<name-or-id>``,
    and prose like ``<24h old`` that trip the parser. Backslash-escaping the
    ``<`` (``\\<``) keeps the literal character in the rendered page without
    requiring authors to remember MDX rules.

    Content inside fenced code blocks (```` ``` ```` / ``~~~``) and inline
    code spans (`` ` ``) is passed through untouched — MDX does not parse
    code spans as JSX.
    """
    out: list[str] = []
    in_fence = False
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue
        out.append(_escape_prose_line(line))
    return "\n".join(out)


def _escape_prose_line(line: str) -> str:
    """Per-character pass: escape ``<`` outside of backtick inline-code spans."""
    chars: list[str] = []
    in_code = False
    for ch in line:
        if ch == "`":
            in_code = not in_code
            chars.append(ch)
            continue
        if ch == "<" and not in_code:
            chars.append("\\<")
            continue
        chars.append(ch)
    return "".join(chars)


def _extract_footer(path: Path) -> str:
    """Return the milestone-link footer block of an existing changelog MDX file.

    Walks past the frontmatter (the first two ``---`` rules) and looks for a
    LAST bare ``---`` line in the body. Everything from that line to EOF is
    the footer block to preserve. Returns an empty string when the file has
    no body-level ``---`` rule.
    """
    lines = path.read_text().splitlines()
    # Find the closing `---` of frontmatter (the second `---` line from top).
    seen = 0
    body_start = -1
    for i, line in enumerate(lines):
        if line == "---":
            seen += 1
            if seen == 2:
                body_start = i + 1
                break
    if body_start < 0:
        return ""
    body = lines[body_start:]
    # Find the last bare `---` in the body — that's the footer rule.
    last_idx = -1
    for j, line in enumerate(body):
        if line == "---":
            last_idx = j
    if last_idx < 0:
        return ""
    return "\n".join(body[last_idx:])
