"""Plan 30-10 Task 1 — `verlet update` install-method-aware (CLIDIST-04).

11 behavior tests covering:

  * detect_install_method() returns the correct (method, argv) tuple for
    pipx / homebrew / uvx / unknown sys.executable path patterns.
  * `verlet update` from pipx env runs `pipx upgrade verlet`.
  * `verlet update` from brew env runs `brew upgrade verlet-robotics/verlet/verlet`.
  * `verlet update` on uvx prints the verbatim "uvx fetches the latest CLI on
    each invocation; nothing to upgrade." line and exits 0.
  * `verlet update` on unknown method prints the reinstall hint to stderr
    and exits 1.
  * Locale-safe stdout: subprocess.run is called with LANG=LC_ALL=C.UTF-8.
  * Pitfall 4 invariant: `verlet update` NEVER calls `pip install --upgrade`.
  * already-up-to-date marker handling for both pipx + brew (locale-safe).
"""
from __future__ import annotations

import subprocess

from verlet.cli import cli


# ---------------------------------------------------------------------------
# detect_install_method() unit tests (path-pattern based)
# ---------------------------------------------------------------------------


def test_detect_install_method_pipx(monkeypatch):
    from verlet.update import detect_install_method

    monkeypatch.setattr(
        "sys.executable",
        "/home/alice/.local/share/pipx/venvs/verlet/bin/python",
    )
    method, argv = detect_install_method()
    assert method == "pipx"
    assert argv == ["pipx", "upgrade", "verlet"]


def test_detect_install_method_homebrew_apple_silicon(monkeypatch):
    from verlet.update import detect_install_method

    monkeypatch.setattr(
        "sys.executable",
        "/opt/homebrew/Cellar/verlet/0.8.0/bin/python",
    )
    method, argv = detect_install_method()
    assert method == "homebrew"
    assert argv == ["brew", "upgrade", "verlet-robotics/verlet/verlet"]


def test_detect_install_method_uvx(monkeypatch):
    from verlet.update import detect_install_method

    monkeypatch.setattr(
        "sys.executable",
        "/home/alice/.cache/uv/archive-v0/abcd/bin/python",
    )
    method, argv = detect_install_method()
    assert method == "uvx"
    assert argv is None


def test_detect_install_method_unknown(monkeypatch):
    from verlet.update import detect_install_method

    monkeypatch.setattr("sys.executable", "/usr/bin/python3")
    method, argv = detect_install_method()
    assert method == "unknown"
    assert argv is None


# ---------------------------------------------------------------------------
# verlet update CLI command tests (mocked subprocess.run)
# ---------------------------------------------------------------------------


def test_update_pipx_runs_pipx_upgrade(monkeypatch, cli_runner):
    """When sys.executable is in a pipx venv, run `pipx upgrade verlet`."""
    monkeypatch.setattr(
        "sys.executable",
        "/home/alice/.local/share/pipx/venvs/verlet/bin/python",
    )
    completed = subprocess.CompletedProcess(
        args=["pipx", "upgrade", "verlet"],
        returncode=0,
        stdout="upgraded verlet 0.7.0 -> 0.8.0",
        stderr="",
    )

    captured: dict = {}

    def fake_run(argv, **kw):
        captured["argv"] = argv
        captured["env"] = kw.get("env", {})
        return completed

    monkeypatch.setattr("subprocess.run", fake_run)

    result = cli_runner.invoke(cli, ["update"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert captured["argv"] == ["pipx", "upgrade", "verlet"]
    # Locale-safe env
    assert captured["env"].get("LANG") == "C.UTF-8"
    assert captured["env"].get("LC_ALL") == "C.UTF-8"


def test_update_pipx_already_up_to_date(monkeypatch, cli_runner):
    """pipx no-op marker `is already at the latest version` -> 'already up to date'."""
    monkeypatch.setattr(
        "sys.executable",
        "/home/alice/.local/share/pipx/venvs/verlet/bin/python",
    )
    completed = subprocess.CompletedProcess(
        args=["pipx", "upgrade", "verlet"],
        returncode=0,
        stdout="verlet is already at the latest version 0.8.0",
        stderr="",
    )
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: completed)

    result = cli_runner.invoke(cli, ["update"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "already up to date" in result.output.lower()


def test_update_brew_already_up_to_date(monkeypatch, cli_runner):
    """brew no-op marker `already installed and up-to-date` -> 'already up to date'."""
    monkeypatch.setattr(
        "sys.executable",
        "/opt/homebrew/Cellar/verlet/0.8.0/bin/python",
    )
    completed = subprocess.CompletedProcess(
        args=["brew", "upgrade", "verlet-robotics/verlet/verlet"],
        returncode=0,
        stdout="Warning: verlet 0.8.0 is already installed and up-to-date.",
        stderr="",
    )
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: completed)

    result = cli_runner.invoke(cli, ["update"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "already up to date" in result.output.lower()


def test_update_uvx_prints_static_message(monkeypatch, cli_runner):
    """uvx detection: print verbatim message + exit 0; never call subprocess.run."""
    monkeypatch.setattr(
        "sys.executable",
        "/home/alice/.cache/uv/archive-v0/abcd/bin/python",
    )

    called = {"n": 0}

    def fake_run(*a, **kw):
        called["n"] += 1
        raise AssertionError("subprocess.run must not be called for uvx")

    monkeypatch.setattr("subprocess.run", fake_run)

    result = cli_runner.invoke(cli, ["update"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert (
        "uvx fetches the latest CLI on each invocation"
        in result.output
    )
    assert called["n"] == 0


def test_update_unknown_method_exits_one_with_reinstall_hint(
    monkeypatch, cli_runner
):
    """unknown install method -> exit 1 + stderr reinstall hint; no subprocess.run."""
    monkeypatch.setattr("sys.executable", "/usr/bin/python3")

    def fake_run(*a, **kw):
        raise AssertionError("subprocess.run must not be called when method=unknown")

    monkeypatch.setattr("subprocess.run", fake_run)

    result = cli_runner.invoke(cli, ["update"])
    assert result.exit_code == 1, (result.output, result.stderr)
    err = result.stderr or result.output
    assert "Cannot auto-update" in err
    assert "pipx install verlet" in err
    assert "brew install verlet-robotics/verlet/verlet" in err
    assert "uvx verlet" in err


def test_update_never_calls_pip_install_upgrade(monkeypatch, cli_runner):
    """Pitfall 4 invariant: `verlet update` NEVER runs `pip install --upgrade`.

    We exercise the pipx path and assert that no invocation passes ``pip`` as
    argv[0] or argv[1]. The pipx path runs `pipx upgrade verlet`; the unknown
    path runs nothing.
    """
    monkeypatch.setattr(
        "sys.executable",
        "/home/alice/.local/share/pipx/venvs/verlet/bin/python",
    )

    completed = subprocess.CompletedProcess(
        args=["pipx", "upgrade", "verlet"], returncode=0, stdout="ok", stderr=""
    )
    captured_pipx: dict = {}

    def fake_run_pipx(argv, **kw):
        captured_pipx["argv"] = argv
        return completed

    monkeypatch.setattr("subprocess.run", fake_run_pipx)
    cli_runner.invoke(cli, ["update"])
    assert captured_pipx["argv"][0] != "pip"
    joined = " ".join(captured_pipx["argv"])
    assert "pip install --upgrade" not in joined
    assert "pip install" not in joined


def test_update_subprocess_failure_propagates_exit_code(monkeypatch, cli_runner):
    """If subprocess returns non-zero, surface stderr + exit with same code."""
    monkeypatch.setattr(
        "sys.executable",
        "/home/alice/.local/share/pipx/venvs/verlet/bin/python",
    )
    completed = subprocess.CompletedProcess(
        args=["pipx", "upgrade", "verlet"],
        returncode=2,
        stdout="",
        stderr="pipx: not authenticated to upgrade",
    )
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: completed)

    result = cli_runner.invoke(cli, ["update"])
    assert result.exit_code == 2, (result.output, result.stderr)
    err = result.stderr or result.output
    assert "pipx: not authenticated to upgrade" in err


def test_update_help_exits_zero(cli_runner):
    """`verlet update --help` should always succeed regardless of install method."""
    result = cli_runner.invoke(cli, ["update", "--help"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "update" in result.output.lower()
