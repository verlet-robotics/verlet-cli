"""Pre-download confirmation + positional output dir for `datasets download`.

Two new download ergonomics:

  * a confirmation prompt ("About to download N units (~X GB) ... Proceed?")
    that gates the actual transfer, skippable with ``--yes`` and auto-skipped
    when stdin isn't a TTY (so scripts/CI never block);
  * an optional positional OUTPUT argument as an alias for ``-o/--output``.

Confirmation tests drive interactivity through ``_is_interactive`` (monkeypatched
True) because Click's CliRunner replaces ``sys.stdin`` during ``invoke``.
"""
from __future__ import annotations

from pathlib import Path

from verlet.auth.credentials import upsert_profile
from verlet.datasets import commands


def _seed_showcase_profile(_tmp_home, *, token: str = "sc-t0k3n") -> None:
    upsert_profile(
        "default",
        kind="showcase_access_code",
        access_token=token,
        api_url="https://api.verlet.co",
    )


def _ego_detail(respx_mock, *, episode_count: int = 10) -> None:
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds"
    ).respond(
        200,
        json={
            "id": "granted-ego-ds",
            "slug": "granted-ego-ds",
            "title": "Granted Ego DS",
            "modality": "ego",
            "task_type": "ego",
            "robot_embodiment": "human-ego",
            "episode_count": episode_count,
            "total_hours": 1.0,
            "effective_grants": [
                {"variant": "raw", "scope": "full", "expires_at": None,
                 "quota_remaining": None}
            ],
        },
    )


def _ego_manifest(respx_mock, *, bytes_estimate: int | None = None) -> None:
    respx_mock.get(
        "https://api.verlet.co/api/v1/showcase/datasets/granted-ego-ds/download"
    ).respond(
        200,
        json={
            "dataset_title": "Granted Ego DS",
            "dataset_slug": "granted-ego-ds",
            "format": "ego-segments-raw",
            "modality": "ego",
            "variant": "raw",
            "scope": "full",
            "episodes": [],
            "segments": [
                {
                    "segment_id": "seg-1",
                    "dataset_index": 0,
                    "duration_s": 10.0,
                    "is_free_sample": False,
                    "files": [
                        {
                            "role": "rgb",
                            "key": "segments/ep/seg-1/rgb.mp4",
                            "url": "https://signed.example/rgb.mp4",
                        }
                    ],
                }
            ],
            "bytes_estimate": bytes_estimate,
            "quota_remaining": None,
        },
    )


# ── _human_size ────────────────────────────────────────────────────────────


def test_human_size_unknown_returns_none():
    assert commands._human_size(None) is None
    assert commands._human_size(0) is None
    assert commands._human_size(-5) is None


def test_human_size_scales_units():
    assert commands._human_size(512) == "512 B"
    assert commands._human_size(1536) == "1.5 KB"
    assert commands._human_size(4 * 1024**3) == "4.0 GB"


# ── _confirm_download gating ────────────────────────────────────────────────


def test_confirm_skipped_when_assume_yes(monkeypatch):
    monkeypatch.setattr(commands, "_is_interactive", lambda: True)
    called = {"confirm": False}
    monkeypatch.setattr(
        commands.click, "confirm",
        lambda *a, **k: called.__setitem__("confirm", True) or True,
    )
    assert commands._confirm_download(
        units=5, unit_word="episodes", n_files=10, size_bytes=None,
        dest=Path("/x"), assume_yes=True,
    ) is True
    assert called["confirm"] is False  # never prompted


def test_confirm_skipped_when_not_interactive(monkeypatch):
    monkeypatch.setattr(commands, "_is_interactive", lambda: False)
    monkeypatch.setattr(
        commands.click, "confirm",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("prompted")),
    )
    assert commands._confirm_download(
        units=5, unit_word="episodes", n_files=10, size_bytes=None,
        dest=Path("/x"), assume_yes=False,
    ) is True


def test_confirm_prompts_and_returns_choice(monkeypatch):
    monkeypatch.setattr(commands, "_is_interactive", lambda: True)
    seen = {}
    monkeypatch.setattr(
        commands.click, "confirm",
        lambda msg, default: seen.update(default=default) or False,
    )
    out = commands._confirm_download(
        units=50, unit_word="episodes", n_files=100,
        size_bytes=4 * 1024**3, dest=Path("/x"), assume_yes=False,
    )
    assert out is False
    assert seen["default"] is True  # Enter == proceed


# ── CLI: positional output dir ──────────────────────────────────────────────


def test_positional_output_dir_used(cli_runner, respx_mock, tmp_home):
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    _ego_detail(respx_mock)
    _ego_manifest(respx_mock)

    result = cli_runner.invoke(
        cli,
        [
            "datasets", "download", "granted-ego-ds",
            "/tmp/custom-out", "--variant", "raw", "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "/tmp/custom-out" in result.output


def test_output_positional_and_flag_conflict_errors(cli_runner, tmp_home):
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    result = cli_runner.invoke(
        cli,
        ["datasets", "download", "granted-ego-ds", "/a", "-o", "/b",
         "--variant", "raw"],
    )
    assert result.exit_code != 0
    assert "not both" in result.output


# ── CLI: confirmation gates the transfer ────────────────────────────────────


def _stub_download(monkeypatch):
    """Replace the real transfer with a recorder. Mirrors test_download.py."""
    rec = {"called": False}

    async def fake_download_resolved(items, parallel, skip_existing):  # noqa: ARG001
        rec["called"] = True
        return commands.DownloadResult(downloaded=len(items), skipped=0, failed=0)

    monkeypatch.setattr(commands, "download_resolved", fake_download_resolved)
    return rec


def test_confirm_abort_skips_download(cli_runner, respx_mock, tmp_home, monkeypatch):
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    _ego_detail(respx_mock)
    _ego_manifest(respx_mock, bytes_estimate=4 * 1024**3)
    monkeypatch.setattr(commands, "_is_interactive", lambda: True)
    # Pre-accept the license so the only prompt under test is the confirmation.
    monkeypatch.setattr(commands, "check_license_accepted", lambda: True)
    rec = _stub_download(monkeypatch)

    result = cli_runner.invoke(
        cli,
        ["datasets", "download", "granted-ego-ds", "--variant", "raw"],
        input="n\n",
    )
    assert result.exit_code == 0, result.output
    assert "cancelled" in result.output.lower()
    assert rec["called"] is False  # answering "n" aborts before transfer


def test_yes_flag_bypasses_prompt(cli_runner, respx_mock, tmp_home, monkeypatch):
    from verlet.cli import cli

    _seed_showcase_profile(tmp_home)
    _ego_detail(respx_mock)
    _ego_manifest(respx_mock, bytes_estimate=1024)
    monkeypatch.setattr(commands, "_is_interactive", lambda: True)
    monkeypatch.setattr(commands, "check_license_accepted", lambda: True)
    rec = _stub_download(monkeypatch)

    # No stdin input: if the prompt fired it would abort on EOF; --yes skips it.
    result = cli_runner.invoke(
        cli,
        ["datasets", "download", "granted-ego-ds", "--variant", "raw", "--yes"],
    )
    assert result.exit_code == 0, result.output
    assert rec["called"] is True  # transfer proceeded without a prompt
