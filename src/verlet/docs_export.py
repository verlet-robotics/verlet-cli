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


def _export_group(grp: click.Group, path: list[str], out_dir: Path) -> None:
    for name, cmd in sorted(grp.commands.items()):
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

    lines: list[str] = [
        "---",
        f"title: {title}",
        f"description: {desc}",
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
        cmd.help or "_No description._",
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
                _normalize_epilog_recipes(cmd.epilog),
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
