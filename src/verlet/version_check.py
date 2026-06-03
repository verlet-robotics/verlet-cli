"""Best-effort "a newer verlet is available" notifier (CLIDIST-04, alert half).

Companion to ``update.py`` (which performs the install-method-aware upgrade).
This module decides *when to nag* — it never upgrades anything.

Design (the npm / gh / brew pattern): the foreground command NEVER waits on a
network call. We read a cached "latest version" from ``~/.verlet/config.json``
and print the notice instantly. When that cache is older than
``CHECK_INTERVAL_SECONDS``, we refresh it in a *detached background process*
(``python -m verlet.version_check``) so the NEXT invocation is current. First
run on a fresh machine therefore prints nothing and silently warms the cache.

The notice always goes to **stderr** so it can never corrupt stdout (JSON
manifests, ``| jq`` pipelines, ``verlet datasets ... > file``).

Suppression (per product decision) is by EXPLICIT opt-out only:
  * env ``VERLET_NO_UPDATE_CHECK`` set to any non-empty value, or
  * config ``update_check_enabled`` set to ``false`` (``verlet config
    update-check disable``).
There is intentionally no CI gate and no TTY gate.

Every failure path is swallowed: a broken update check must never break a real
command.
"""
from __future__ import annotations

import os
import re
import sys
import time

from verlet.config import load_config, save_config

# PyPI's JSON API. ``info.version`` is the latest non-yanked release.
PYPI_JSON_URL = "https://pypi.org/pypi/verlet/json"

# How long a cached "latest" stays fresh before we kick off a background
# refresh. 24h matches npm/gh; the nag itself fires every run while stale.
CHECK_INTERVAL_SECONDS = 24 * 60 * 60

# Burst guard: how long after spawning a background refresh we suppress
# spawning another. This dedupes a flurry of commands (or a tight scripted
# loop) into a single refresh process without freezing the staleness clock —
# unlike ``checked_at``, this is NOT how we decide a cache is current, so a
# failed refresh still retries on the next invocation past this window.
SPAWN_DEDUPE_SECONDS = 5 * 60

# Background refresh can afford a generous timeout (it blocks nothing).
# The synchronous `verlet update --check` path uses a tighter one.
_BG_FETCH_TIMEOUT = 5.0
_SYNC_FETCH_TIMEOUT = 4.0


def _now() -> float:
    """Wall clock as epoch seconds. Indirected so tests can freeze it."""
    return time.time()


# ---------------------------------------------------------------------------
# Version parsing / comparison (no `packaging` dependency)
# ---------------------------------------------------------------------------


def _parse_version(s: str | None) -> tuple[int, ...]:
    """Parse the leading dotted-numeric release segment of a version string.

    ``"0.11.0"`` -> ``(0, 11, 0)``; ``"v1.2"`` -> ``(1, 2)``;
    ``"0.11.0rc1"`` -> ``(0, 11, 0)`` (pre/post/dev/local suffixes ignored).
    Unparseable input -> ``()``. We only ever publish stable releases, so
    dropping the suffix is safe and keeps the comparator dependency-free.
    """
    m = re.match(r"\s*v?(\d+(?:\.\d+)*)", s or "")
    if not m:
        return ()
    return tuple(int(p) for p in m.group(1).split("."))


def is_newer(latest: str | None, current: str | None) -> bool:
    """True iff ``latest`` is a strictly higher release than ``current``."""
    lt = _parse_version(latest)
    ct = _parse_version(current)
    if not lt:
        return False
    width = max(len(lt), len(ct))
    lt += (0,) * (width - len(lt))
    ct += (0,) * (width - len(ct))
    return lt > ct


def compute_notice(current: str | None, latest: str | None) -> str | None:
    """Return the user-facing notice string, or ``None`` when up to date."""
    if is_newer(latest, current):
        return (
            f"A new release of verlet is available: {current} → {latest}\n"
            "Run `verlet update` to upgrade."
        )
    return None


# ---------------------------------------------------------------------------
# Opt-out + cache (lives under the existing ~/.verlet/config.json)
# ---------------------------------------------------------------------------


def check_disabled() -> bool:
    """True when the update check is explicitly opted out (env or config)."""
    if os.environ.get("VERLET_NO_UPDATE_CHECK"):
        return True
    # Explicit ``false`` only — absent/other values leave the check enabled.
    return load_config().get("update_check_enabled") is False


def _read_cache() -> dict:
    cache = load_config().get("update_check")
    return cache if isinstance(cache, dict) else {}


def _write_cache(latest: str | None, checked_at: float) -> None:
    """Merge the update-check cache back into config.json (read-modify-write).

    ``checked_at`` is the *last successful check* timestamp and is the sole
    input to the staleness decision — only callers that actually fetched a
    ``latest`` should advance it (``_refresh_main`` / ``update --check``).
    """
    cfg = load_config()
    cache = cfg.get("update_check")
    if not isinstance(cache, dict):
        cache = {}
    if latest is not None:
        cache["latest"] = latest
    cache["checked_at"] = checked_at
    cfg["update_check"] = cache
    save_config(cfg)


def _stamp_spawn(when: float) -> None:
    """Record that a background refresh was just spawned (burst dedupe only).

    Deliberately does NOT touch ``checked_at`` — a spawned refresh that later
    fails must not look like a successful check, or the cache would freeze a
    stale ``latest`` for a full ``CHECK_INTERVAL_SECONDS`` with no retry.
    """
    cfg = load_config()
    cache = cfg.get("update_check")
    if not isinstance(cache, dict):
        cache = {}
    cache["last_spawn"] = when
    cfg["update_check"] = cache
    save_config(cfg)


# ---------------------------------------------------------------------------
# Network (best-effort; all failures -> None)
# ---------------------------------------------------------------------------


def fetch_latest_version(timeout: float = _SYNC_FETCH_TIMEOUT) -> str | None:
    """GET the latest verlet version from PyPI, or ``None`` on any failure."""
    try:
        import httpx

        try:
            from verlet.telemetry import current_user_agent

            ua = current_user_agent()
        except Exception:
            ua = "verlet-cli"

        resp = httpx.get(
            PYPI_JSON_URL,
            timeout=timeout,
            headers={"User-Agent": ua, "Accept": "application/json"},
        )
        resp.raise_for_status()
        version = resp.json().get("info", {}).get("version")
        return str(version) if version else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Notice + background refresh orchestration
# ---------------------------------------------------------------------------


def _print_notice(notice: str) -> None:
    """Print the notice to stderr (so stdout stays clean for pipes/JSON)."""
    try:
        from rich.console import Console

        Console(stderr=True).print(f"\n[yellow]{notice}[/yellow]\n")
    except Exception:
        sys.stderr.write(f"\n{notice}\n\n")


def _spawn_background_refresh() -> None:
    """Detach a ``python -m verlet.version_check`` process to warm the cache.

    Fully fire-and-forget: stdio is sent to DEVNULL and the child is detached
    from our process group / console so it outlives this command and prints
    nothing. Any failure to spawn is swallowed.
    """
    try:
        import subprocess

        kwargs: dict = dict(
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
        if os.name == "nt":
            flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
                subprocess, "CREATE_NO_WINDOW", 0
            )
            kwargs["creationflags"] = flags
        else:
            kwargs["start_new_session"] = True
        subprocess.Popen([sys.executable, "-m", "verlet.version_check"], **kwargs)
    except Exception:
        pass


def notify_if_outdated() -> None:
    """Print an upgrade notice from cache; refresh the cache if it's stale.

    Called once per CLI invocation from the root group. Never raises, never
    blocks on the network, and never touches stdout.
    """
    try:
        if check_disabled():
            return

        from verlet import __version__ as current

        cache = _read_cache()
        latest = cache.get("latest")
        latest = latest if isinstance(latest, str) else None

        notice = compute_notice(current, latest)
        if notice:
            _print_notice(notice)

        try:
            checked_at = float(cache.get("checked_at") or 0)
        except (TypeError, ValueError):
            checked_at = 0.0

        if (_now() - checked_at) >= CHECK_INTERVAL_SECONDS:
            # Staleness is measured from the last SUCCESSFUL check (checked_at),
            # which only a completed fetch advances. We do NOT stamp checked_at
            # here — doing so optimistically (the old bug) froze a stale
            # `latest` for a full interval whenever the background refresh
            # failed or lagged. Instead a short-lived `last_spawn` debounces a
            # burst of commands; once it elapses, a still-stale cache simply
            # retries the refresh on the next invocation.
            try:
                last_spawn = float(cache.get("last_spawn") or 0)
            except (TypeError, ValueError):
                last_spawn = 0.0
            if (_now() - last_spawn) >= SPAWN_DEDUPE_SECONDS:
                _stamp_spawn(_now())
                _spawn_background_refresh()
    except Exception:
        # A broken update check must never break a real command.
        pass


def _refresh_main() -> None:
    """Entry point for the detached ``python -m verlet.version_check`` child."""
    latest = fetch_latest_version(timeout=_BG_FETCH_TIMEOUT)
    if latest:
        try:
            _write_cache(latest, _now())
        except Exception:
            pass


if __name__ == "__main__":
    _refresh_main()
