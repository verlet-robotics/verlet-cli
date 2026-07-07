"""`verlet update` — install-method-aware self-update (CLIDIST-04).

Replaces the broken pre-Phase-30 stub at ``cli.py:78-113`` (which ran
``pip install --upgrade verlet`` from inside whatever Python interpreter
was active — corrupting pipx envs, stomping brew-managed installs, and
silently no-op'ing under uvx).

Detection precedence (D-DIST2): pipx -> homebrew -> uvx -> unknown.
Each path uses the install method's own upgrade command. uvx is a
static-message path because uvx fetches the latest CLI on every
invocation; nothing to upgrade in place. Unknown method is a hard
refusal — Pitfall 4 invariant locks ``pip install --upgrade`` out
of every code path.

Locale-safe: the upgrade subprocess is invoked with ``LANG=LC_ALL=C.UTF-8``
so the no-op marker parsing works on corporate Macs whose default locale
isn't ``en_US.UTF-8``. Output is streamed live (merged stdout+stderr) while
being accumulated for the marker parser — a silent capture made slow brew
rebuilds look like a hang.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import click

from verlet.display import console


InstallMethod = str  # "pipx" | "homebrew" | "uvx" | "unknown"


def detect_install_method() -> tuple[InstallMethod, list[str] | None]:
    """Inspect ``sys.executable`` to determine the install method.

    Returns ``(method, upgrade_argv | None)``. ``argv is None`` means there
    is no auto-upgrade path (uvx fetches latest on each invocation; unknown
    method must be reinstalled by the user).

    Detection order matters — pipx has the most-specific pattern, so it
    goes first. Homebrew patterns can overlap with system Python on
    Apple Silicon (``/opt/homebrew/bin/python3``) so we lock the order.
    """
    exe_str = str(Path(sys.executable).resolve())

    # 1. pipx — venvs under ~/.local/share/pipx/venvs/verlet/ on Linux/macOS
    #    or ~\pipx\venvs\verlet\ on Windows. Pattern is package-name-specific.
    if "pipx/venvs/verlet" in exe_str or "pipx\\venvs\\verlet" in exe_str:
        return ("pipx", ["pipx", "upgrade", "verlet"])

    # 2. Homebrew (Apple Silicon /opt/homebrew, Intel/Linuxbrew /usr/local/Cellar)
    if "/Cellar/verlet/" in exe_str or "/opt/homebrew/" in exe_str:
        return (
            "homebrew",
            ["brew", "upgrade", "verlet-robotics/verlet/verlet"],
        )

    # 3. uvx — caches under ~/.cache/uv/archive-v0/.../bin/{pkg} or
    #    %LOCALAPPDATA%\uv\cache\... on Windows. uvx fetches latest on
    #    every invocation, so there's nothing to upgrade in place.
    if "/uv/" in exe_str and ("/cache/" in exe_str or "/.cache/" in exe_str or "uv\\cache" in exe_str):
        return ("uvx", None)

    # 4. Unknown — refuse destructive `pip install --upgrade` (Pitfall 4
    #    invariant). The user is told how to reinstall via a clean method.
    return ("unknown", None)


def _run_streaming(argv: list[str], env: dict[str, str]) -> tuple[int, str]:
    """Run the upgrade command, echoing its output live while accumulating it.

    ``capture_output=True`` left the terminal silent for the whole run — a
    brew upgrade that rebuilds the formula virtualenv from source (e.g. after
    a python@3.12 revision bump) looks like a hang for several minutes. Stream
    stdout+stderr merged so the user sees brew/pipx progress; the accumulated
    transcript still feeds the no-op-marker parser.
    """
    proc = subprocess.Popen(
        argv,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    chunks: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        chunks.append(line)
        click.echo(line, nl=False)
    return (proc.wait(), "".join(chunks))


def _is_no_op_marker(output: str) -> bool:
    """Locale-safe up-to-date detection. pipx + brew both pass.

    pipx prints ``"... is already at the latest version"`` on a no-op.
    brew prints ``"... is already installed and up-to-date"``.

    Both are wrapped in ``output.lower()`` so a future tool that prints
    e.g. ``"ALREADY at the LATEST version"`` still matches.
    """
    lower = output.lower()
    return (
        ("already" in lower and "latest" in lower)
        or "already installed and up-to-date" in lower
    )


def _report_check_status() -> None:
    """Synchronous PyPI check for `verlet update --check`.

    Prints current-vs-latest to stdout and refreshes the background-notice
    cache as a side effect. Never raises; a PyPI outage is reported, not fatal.
    """
    from verlet import __version__ as current
    from verlet.version_check import fetch_latest_version, is_newer, _write_cache, _now

    latest = fetch_latest_version()
    if latest is None:
        console.print(
            f"verlet {current} — could not reach PyPI to check for updates."
        )
        return

    # Warm the cache so the passive notice is immediately consistent.
    try:
        _write_cache(latest, _now())
    except Exception:
        pass

    if is_newer(latest, current):
        console.print(
            f"verlet {current} — update available: [green]{latest}[/green]. "
            "Run `verlet update` to upgrade."
        )
    else:
        console.print(f"verlet {current} is up to date.")


@click.command("update")
@click.option(
    "--check",
    "check_only",
    is_flag=True,
    help="Report whether a newer release exists on PyPI; do not upgrade.",
)
def update(check_only: bool):
    """Upgrade verlet to the latest PyPI release using the detected install method.

    \b
    Detection precedence: pipx -> homebrew -> uvx -> unknown.
    pipx and homebrew run their respective `upgrade` subcommand.
    uvx prints a notice (uvx fetches latest on each invocation; nothing to do).
    Unknown install method exits 1 with a reinstall hint.

    With --check, query PyPI synchronously and report status without
    upgrading (also refreshes the background-notice cache).
    """
    if check_only:
        _report_check_status()
        return

    method, argv = detect_install_method()

    if method == "uvx":
        console.print(
            "uvx fetches the latest CLI on each invocation; "
            "nothing to upgrade."
        )
        return  # exit 0

    if method == "unknown":
        click.echo(
            "Cannot auto-update; install method unknown. "
            "Reinstall with one of: pipx install verlet, "
            "brew install verlet-robotics/verlet/verlet, or uvx verlet",
            err=True,
        )
        raise SystemExit(1)

    # pipx or homebrew — run subprocess with locale-safe env so the
    # no-op-marker parser sees English regardless of corporate locale.
    assert argv is not None  # type guard — pipx/homebrew always carry argv
    console.print(
        f"[dim]running:[/dim] [cyan]{' '.join(argv)}[/cyan]"
    )
    env = {**os.environ, "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    returncode, combined = _run_streaming(argv, env)

    if returncode == 0:
        if _is_no_op_marker(combined):
            console.print("already up to date")
        else:
            console.print("[green]upgrade complete[/green]")
        return

    click.echo(f"upgrade failed (exit {returncode}):", err=True)
    raise SystemExit(returncode)
