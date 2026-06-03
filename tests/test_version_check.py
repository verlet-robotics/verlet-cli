"""CLIDIST-04 (alert half) — proactive "newer verlet available" notice.

Covers version parsing/comparison, the explicit opt-out (env + config), the
cached-read + background-refresh orchestration, the synchronous
`verlet update --check` path, and the root-group wiring (notice on stderr,
suppressed for `verlet update`).
"""
from __future__ import annotations

import httpx
import pytest
import respx

from verlet import version_check as vc
from verlet.cli import cli


# ---------------------------------------------------------------------------
# version parsing / comparison (pure)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("0.11.0", (0, 11, 0)),
        ("v1.2", (1, 2)),
        ("0.11.0rc1", (0, 11, 0)),
        ("  0.10.1  ", (0, 10, 1)),
        ("garbage", ()),
        (None, ()),
    ],
)
def test_parse_version(raw, expected):
    assert vc._parse_version(raw) == expected


@pytest.mark.parametrize(
    "latest,current,expected",
    [
        ("0.11.0", "0.10.1", True),
        ("0.11.0", "0.11.0", False),
        ("0.10.1", "0.11.0", False),
        ("0.11", "0.11.0", False),   # 0.11 == 0.11.0 after padding
        ("1.0", "0.99.99", True),
        (None, "0.11.0", False),
        ("0.11.0", None, True),
        ("garbage", "0.11.0", False),
    ],
)
def test_is_newer(latest, current, expected):
    assert vc.is_newer(latest, current) is expected


def test_compute_notice_when_newer():
    notice = vc.compute_notice("0.10.1", "0.11.0")
    assert notice is not None
    assert "0.10.1 → 0.11.0" in notice
    assert "verlet update" in notice


def test_compute_notice_when_up_to_date():
    assert vc.compute_notice("0.11.0", "0.11.0") is None
    assert vc.compute_notice("0.11.0", "0.10.1") is None


# ---------------------------------------------------------------------------
# opt-out (env + config)
# ---------------------------------------------------------------------------


def test_check_disabled_via_env(tmp_home, monkeypatch):
    monkeypatch.setenv("VERLET_NO_UPDATE_CHECK", "1")
    assert vc.check_disabled() is True


def test_check_disabled_via_config(tmp_home, monkeypatch):
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)
    from verlet.config import save_config

    save_config({"update_check_enabled": False})
    assert vc.check_disabled() is True


def test_check_enabled_by_default(tmp_home, monkeypatch):
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)
    assert vc.check_disabled() is False


# ---------------------------------------------------------------------------
# cache read/write round-trip
# ---------------------------------------------------------------------------


def test_cache_roundtrip(tmp_home):
    assert vc._read_cache() == {}
    vc._write_cache("0.11.0", 1234.5)
    cache = vc._read_cache()
    assert cache["latest"] == "0.11.0"
    assert cache["checked_at"] == 1234.5
    # Does not clobber sibling config keys.
    from verlet.config import load_config, save_config

    cfg = load_config()
    cfg["telemetry_enabled"] = True
    save_config(cfg)
    vc._write_cache("0.12.0", 9999.0)
    assert load_config()["telemetry_enabled"] is True
    assert vc._read_cache()["latest"] == "0.12.0"


# ---------------------------------------------------------------------------
# notify_if_outdated orchestration
# ---------------------------------------------------------------------------


def test_notify_prints_when_cache_has_newer(tmp_home, monkeypatch, capsys):
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)
    monkeypatch.setattr("verlet.__version__", "0.10.1", raising=False)
    # Fresh cache -> no refresh spawned.
    monkeypatch.setattr(vc, "_now", lambda: 1000.0)
    vc._write_cache("0.11.0", 1000.0)

    spawned = {"n": 0}
    monkeypatch.setattr(vc, "_spawn_background_refresh", lambda: spawned.__setitem__("n", spawned["n"] + 1))

    vc.notify_if_outdated()
    err = capsys.readouterr().err
    assert "A new release of verlet is available" in err
    assert "0.10.1 → 0.11.0" in err
    assert spawned["n"] == 0  # cache fresh -> no background refresh


def test_notify_silent_when_up_to_date(tmp_home, monkeypatch, capsys):
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)
    monkeypatch.setattr("verlet.__version__", "0.11.0", raising=False)
    monkeypatch.setattr(vc, "_now", lambda: 1000.0)
    vc._write_cache("0.11.0", 1000.0)
    monkeypatch.setattr(vc, "_spawn_background_refresh", lambda: None)

    vc.notify_if_outdated()
    assert "new release" not in capsys.readouterr().err


def test_notify_silent_when_disabled(tmp_home, monkeypatch, capsys):
    monkeypatch.setenv("VERLET_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr("verlet.__version__", "0.10.1", raising=False)
    vc._write_cache("0.11.0", 0.0)
    spawned = {"n": 0}
    monkeypatch.setattr(vc, "_spawn_background_refresh", lambda: spawned.__setitem__("n", 1))

    vc.notify_if_outdated()
    assert capsys.readouterr().err == ""
    assert spawned["n"] == 0  # disabled -> no refresh either


def test_notify_spawns_refresh_when_stale(tmp_home, monkeypatch):
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)
    monkeypatch.setattr("verlet.__version__", "0.11.0", raising=False)
    # checked_at far in the past relative to _now -> stale.
    monkeypatch.setattr(vc, "_now", lambda: 10 * vc.CHECK_INTERVAL_SECONDS)
    vc._write_cache("0.11.0", 0.0)

    spawned = {"n": 0}
    monkeypatch.setattr(vc, "_spawn_background_refresh", lambda: spawned.__setitem__("n", spawned["n"] + 1))

    vc.notify_if_outdated()
    assert spawned["n"] == 1
    # checked_at is NOT advanced here — only a successful fetch does that, so a
    # failed/slow refresh self-heals on the next run instead of freezing.
    cache = vc._read_cache()
    assert cache["checked_at"] == 0.0
    assert cache["last_spawn"] == 10 * vc.CHECK_INTERVAL_SECONDS


def test_notify_self_heals_when_refresh_never_succeeded(tmp_home, monkeypatch):
    """A spawned-but-failed refresh (checked_at untouched) retries next run,
    once the burst-dedupe window has elapsed — the old optimistic-stamp bug
    would have frozen the stale cache for a full interval."""
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)
    monkeypatch.setattr("verlet.__version__", "0.11.0", raising=False)
    vc._write_cache("0.11.0", 0.0)  # never-succeeded check

    spawned = {"n": 0}
    monkeypatch.setattr(
        vc, "_spawn_background_refresh",
        lambda: spawned.__setitem__("n", spawned["n"] + 1),
    )

    # First stale invocation spawns; records last_spawn but not checked_at.
    monkeypatch.setattr(vc, "_now", lambda: 10 * vc.CHECK_INTERVAL_SECONDS)
    vc.notify_if_outdated()
    assert spawned["n"] == 1

    # A second invocation within the dedupe window does NOT re-spawn.
    monkeypatch.setattr(
        vc, "_now",
        lambda: 10 * vc.CHECK_INTERVAL_SECONDS + vc.SPAWN_DEDUPE_SECONDS - 1,
    )
    vc.notify_if_outdated()
    assert spawned["n"] == 1

    # Past the dedupe window, the still-stale cache retries (self-heal).
    monkeypatch.setattr(
        vc, "_now",
        lambda: 10 * vc.CHECK_INTERVAL_SECONDS + vc.SPAWN_DEDUPE_SECONDS + 1,
    )
    vc.notify_if_outdated()
    assert spawned["n"] == 2


def test_notify_never_raises(tmp_home, monkeypatch):
    """Any internal failure is swallowed."""
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)

    def boom():
        raise RuntimeError("config exploded")

    monkeypatch.setattr(vc, "_read_cache", boom)
    vc.notify_if_outdated()  # must not raise


# ---------------------------------------------------------------------------
# network fetch + background refresh entry point
# ---------------------------------------------------------------------------


def test_fetch_latest_version_parses_pypi():
    with respx.mock(base_url="https://pypi.org") as router:
        router.get("/pypi/verlet/json").mock(
            return_value=httpx.Response(200, json={"info": {"version": "0.11.0"}})
        )
        assert vc.fetch_latest_version(timeout=1.0) == "0.11.0"


def test_fetch_latest_version_network_error_returns_none():
    with respx.mock(base_url="https://pypi.org") as router:
        router.get("/pypi/verlet/json").mock(side_effect=httpx.ConnectError("down"))
        assert vc.fetch_latest_version(timeout=1.0) is None


def test_refresh_main_writes_cache(tmp_home, monkeypatch):
    monkeypatch.setattr(vc, "fetch_latest_version", lambda timeout=5.0: "0.12.0")
    monkeypatch.setattr(vc, "_now", lambda: 555.0)
    vc._refresh_main()
    assert vc._read_cache() == {"latest": "0.12.0", "checked_at": 555.0}


def test_refresh_main_no_write_on_failure(tmp_home, monkeypatch):
    monkeypatch.setattr(vc, "fetch_latest_version", lambda timeout=5.0: None)
    vc._refresh_main()
    assert vc._read_cache() == {}


# ---------------------------------------------------------------------------
# `verlet update --check` synchronous path
# ---------------------------------------------------------------------------


def test_update_check_reports_available(tmp_home, monkeypatch, cli_runner):
    monkeypatch.setattr("verlet.__version__", "0.10.1", raising=False)
    monkeypatch.setattr(
        "verlet.version_check.fetch_latest_version", lambda timeout=4.0: "0.11.0"
    )
    result = cli_runner.invoke(cli, ["update", "--check"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "update available" in result.output
    assert "0.11.0" in result.output


def test_update_check_reports_up_to_date(tmp_home, monkeypatch, cli_runner):
    monkeypatch.setattr("verlet.__version__", "0.11.0", raising=False)
    monkeypatch.setattr(
        "verlet.version_check.fetch_latest_version", lambda timeout=4.0: "0.11.0"
    )
    result = cli_runner.invoke(cli, ["update", "--check"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "up to date" in result.output


def test_update_check_pypi_unreachable(tmp_home, monkeypatch, cli_runner):
    monkeypatch.setattr(
        "verlet.version_check.fetch_latest_version", lambda timeout=4.0: None
    )
    result = cli_runner.invoke(cli, ["update", "--check"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "could not reach PyPI" in result.output


# ---------------------------------------------------------------------------
# root-group wiring
# ---------------------------------------------------------------------------


def test_root_group_emits_notice_on_other_commands(tmp_home, monkeypatch, cli_runner):
    """A non-update subcommand surfaces the cached notice on stderr."""
    monkeypatch.delenv("VERLET_NO_UPDATE_CHECK", raising=False)
    monkeypatch.setattr("verlet.__version__", "0.10.1", raising=False)
    monkeypatch.setattr(vc, "_now", lambda: 1000.0)
    monkeypatch.setattr(vc, "_spawn_background_refresh", lambda: None)
    vc._write_cache("0.11.0", 1000.0)  # fresh -> no spawn

    result = cli_runner.invoke(cli, ["config", "telemetry", "status"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "A new release of verlet is available" in result.stderr


def test_root_group_suppresses_notice_for_update_command(tmp_home, monkeypatch):
    """`verlet update` must NOT trigger the passive notice (avoid double-nag)."""
    called = {"n": 0}
    monkeypatch.setattr(
        vc, "notify_if_outdated", lambda: called.__setitem__("n", called["n"] + 1)
    )
    # Drive the update command on the uvx path so it's a no-network no-op.
    monkeypatch.setattr("sys.executable", "/home/x/.cache/uv/archive-v0/a/bin/python")
    from click.testing import CliRunner

    result = CliRunner().invoke(cli, ["update"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert called["n"] == 0
