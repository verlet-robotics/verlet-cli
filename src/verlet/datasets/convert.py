"""Format conversion polling for ``verlet datasets download --format <fmt>``.

CLIDATA-07 (Plan 30-04 Task 1). The CLI hits ``/downloads/{slug}/manifest?format=<fmt>``;
when the server responds 202 + ``Manifest202Response{job_id, status, poll_url, …}``
this module's ``poll_conversion_job`` polls ``/downloads/jobs/{job_id}`` every
``POLL_INTERVAL_SECONDS`` until terminal:

* ``completed`` → return the inlined ``manifest`` dict (commands.py drives the
  download from there).
* ``failed`` → write the verbatim ``error_message`` (and ``failed_stage`` if
  present) to stderr and ``raise SystemExit(1)``.

Per **D-FORMAT3** the failure path **does not** auto-retry — conversion failures
are usually user-data issues (corrupt episode, format mismatch) where retry
just re-burns CPU. CI scripts implement their own retry policy.

Per **D-FORMAT4** the default UX is a single Rich progress line; ``--verbose``
additionally echoes ``progress.log_lines`` from the server to stderr;
``--quiet`` hides the progress bar entirely (errors still go to stderr).
"""
from __future__ import annotations

import asyncio
import sys
from typing import Any

import click
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from verlet.api_client import AuthenticatedClient
from verlet.display import console


SUPPORTED_FORMATS: tuple[str, ...] = (
    "lerobot-v2",
    "lerobot-v3",
    "hdf5",
    "zarr",
    "rlds",
    "rosbag",
    "robodm",
    "egomimic",
)
"""All 8 writable formats from ``backend.core.domains.format_registry.WRITABLE_FORMATS``.

The CLI is conservative: a format that is not in this tuple will be rejected
client-side **before** any HTTP call fires, so a typo never burns a server
round-trip. The list is mirror-asserted in tests against the verbatim 8 from
the server-side ``WRITABLE_FORMATS`` tuple.
"""

POLL_INTERVAL_SECONDS: float = 3.0
"""Default poll cadence for ``/downloads/jobs/{id}``.

Locked at 3.0 to match D-FORMAT4's "every 2-3s" target. Tests freeze
``asyncio.sleep`` rather than actually sleeping so the unit suite stays fast.
"""


def validate_format(value: str | None) -> str | None:
    """Click ``callback=`` for the ``--format`` option.

    Returns ``None`` when no flag was passed (the native LeRobot v2 default).
    Otherwise validates the value against ``SUPPORTED_FORMATS`` and either
    returns it unchanged or raises ``click.BadParameter`` listing every valid
    format verbatim — the message is what the user sees on misuse, so it must
    not require a follow-up ``verlet datasets download --help`` to discover
    the alternatives.
    """
    if value is None:
        return None
    if value not in SUPPORTED_FORMATS:
        raise click.BadParameter(
            f"--format must be one of: {', '.join(SUPPORTED_FORMATS)}",
        )
    return value


async def poll_conversion_job(
    client: AuthenticatedClient,
    job_id: str,
    *,
    verbose: bool = False,
    quiet: bool = False,
) -> dict[str, Any]:
    """Poll ``/api/platform/v1/downloads/jobs/{job_id}`` until terminal.

    Returns the inlined manifest dict on ``status == "completed"``. On
    ``status == "failed"`` writes the server's ``error_message`` (plus the
    optional ``failed_stage``) to stderr and raises ``SystemExit(1)`` — D-FORMAT3.

    The Rich progress bar is created inside this function so the lifetime is
    bounded by the poll loop; the ``finally`` block always stops the live
    display even when the failure path raises ``SystemExit``.

    ``quiet=True`` suppresses the progress bar entirely (used in tests and
    CI where stdout is captured). ``verbose=True`` additionally streams any
    ``progress.log_lines`` the server emits between polls.
    """
    progress: Progress | None = None
    task_id: int | None = None
    if not quiet:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total} episodes"),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        )
        progress.start()
        task_id = progress.add_task(
            f"converting [cyan]{job_id}[/cyan]", total=None
        )

    try:
        while True:
            resp = client.get(
                f"/api/platform/v1/downloads/jobs/{job_id}"
            )
            resp.raise_for_status()
            body = resp.json()
            status = body["status"]

            if status == "completed":
                if progress is not None and task_id is not None:
                    progress.update(task_id, completed=1, total=1)
                # ``manifest`` is the inlined DownloadManifest; commands.py
                # drives the actual download from this dict.
                return body["manifest"]

            if status == "failed":
                err = body.get("error_message") or "conversion failed"
                stage = body.get("failed_stage")
                msg = f"conversion failed: {err}"
                if stage:
                    msg += f" (stage: {stage})"
                sys.stderr.write(msg + "\n")
                raise SystemExit(1)

            # status is "pending" or "processing" — update the bar + sleep.
            prog = body.get("progress") or {}
            cur = prog.get("current_episode") or 0
            total = prog.get("total_episodes")
            if (
                progress is not None
                and task_id is not None
                and total
            ):
                progress.update(task_id, completed=cur, total=total)

            if verbose:
                # D-FORMAT4: server log lines surface verbatim on stderr in
                # addition to the (still-running) progress bar.
                log_lines = prog.get("log_lines") or []
                for line in log_lines:
                    sys.stderr.write(f"[server] {line}\n")

            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        if progress is not None:
            progress.stop()
