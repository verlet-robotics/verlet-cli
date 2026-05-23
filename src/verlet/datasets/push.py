"""verlet datasets push <slug> --to huggingface://org/repo (CLIDATA-07).

Drives the Plan 30-01 server endpoint:

    POST /api/platform/v1/downloads/{slug}/push
    body: {"destination_url": "huggingface://org/repo", "hf_token": "...", "format": null}
    → 202 + {"push_id": "...", "status": "pending"}

After the 202, the CLI polls the existing Phase 9 endpoint:

    GET /api/platform/v1/downloads/pushes/recent
    → {"pushes": [{"push_id": "...", "status": "...", "progress": ..., "error_message": ...?}, ...]}

Polling cadence is locked at ``POLL_INTERVAL_SECONDS`` (3.0s, mirroring
``verlet/datasets/convert.py``) so the unit tests can assert call_args.args[0]
without freezegun and the UX matches the format-conversion poll loop.

D-FORMAT2 token precedence (profile > HF_TOKEN env > UsageError) is enforced
in ``verlet.datasets._validation.resolve_hf_token``; this module just calls it.
"""
from __future__ import annotations

import asyncio
import sys
from typing import Any

import click

from verlet.api_client import AuthenticatedClient
from verlet.auth.profiles import (
    ProfileNotFoundError,
    require_profile,
    resolve_profile_name,
)
from verlet.datasets._validation import parse_hf_url, resolve_hf_token
from verlet.display import console


POLL_INTERVAL_SECONDS: float = 3.0
"""Seconds between /pushes/recent polls. Locked at 3.0 to match D-FORMAT4
"every 2-3s" cadence and the format-conversion poll loop in convert.py."""


_PUSH_EPILOG = """\b
Examples:

```bash
verlet datasets push imitate-cube --to huggingface://acme/imitate-cube
```

\b
Set a HuggingFace token first if not already configured:

```bash
verlet auth tokens set hf hf_xxx
```

\b
Convert before push (server-side rosetta):

```bash
verlet datasets push imitate-cube --to huggingface://acme/imitate-cube --format hdf5
```
"""


@click.command("push", epilog=_PUSH_EPILOG)
@click.argument("slug")
@click.option(
    "--to",
    "to_url",
    default=None,
    help="Ad-hoc destination URL, e.g. huggingface://my-org/my-dataset.",
)
@click.option(
    "--destination",
    "-d",
    "dest_ref",
    default=None,
    help="A saved cloud destination, by name or id (see `verlet destinations`).",
)
@click.option(
    "--format",
    "fmt",
    default=None,
    help="Convert to this format before push (forwarded to /push).",
)
@click.pass_context
def push(
    ctx: click.Context,
    slug: str,
    to_url: str | None,
    dest_ref: str | None,
    fmt: str | None,
) -> None:
    """Push a purchased dataset to HuggingFace or a saved cloud destination.

    \b
    Provide exactly one of:
      --to huggingface://org/repo   ad-hoc HuggingFace push (needs an HF token)
      --destination <name-or-id>    a destination saved via `verlet destinations`

    \b
    Examples:
      verlet datasets push imitate-cube --to huggingface://acme/imitate-cube
      verlet datasets push imitate-cube --destination my-s3-bucket
    """
    # 1. Exactly one destination shape — mirrors the backend PushTriggerRequest
    # XOR. UsageError exits 2 with zero network traffic.
    if bool(to_url) == bool(dest_ref):
        raise click.UsageError("Provide exactly one of --to or --destination.")

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    # Auth gate — fail fast pre-HTTP with a tailored message, matching every
    # other authed command in 0.10.0 (library, jobs, destinations, showcase).
    try:
        require_profile(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` to push datasets."
        )

    if to_url:
        # Ad-hoc HuggingFace push. parse_hf_url raises BadParameter (exit 2)
        # on malformed input pre-HTTP. resolve_hf_token enforces D-FORMAT2
        # precedence (profile > HF_TOKEN env > UsageError).
        parse_hf_url(to_url)
        token = resolve_hf_token(profile_name)
        body: dict[str, Any] = {"destination_url": to_url, "hf_token": token}
    else:
        # Saved cloud destination. Resolve name→id; the HF-token gate does NOT
        # apply here — an S3/GCS destination has nothing to do with HF.
        from verlet.destinations._validation import resolve_destination_ref

        dest_id = asyncio.run(
            resolve_destination_ref(dest_ref or "", profile_name)
        )
        body = {"destination_id": dest_id}

    if fmt:
        body["format"] = fmt

    asyncio.run(_drive_push(profile_name, slug, body))


async def _drive_push(
    profile_name: str,
    slug: str,
    body: dict[str, Any],
) -> None:
    """POST /push then poll /pushes/recent until terminal."""
    client = AuthenticatedClient(profile_name)
    try:
        resp = client.post(
            f"/api/platform/v1/downloads/{slug}/push", json=body
        )
        if resp.status_code >= 400:
            try:
                detail = (resp.json() or {}).get("detail") or resp.text
            except ValueError:
                detail = resp.text
            sys.stderr.write(f"push failed: {detail}\n")
            raise SystemExit(1)
        data = resp.json()
        push_id = data["push_id"]
        console.print(
            f"[green]push enqueued[/green] push_id=[cyan]{push_id}[/cyan]; polling…"
        )
        await _poll_push(client, push_id)
    finally:
        client.close()


async def _poll_push(client: AuthenticatedClient, push_id: str) -> None:
    """Poll /pushes/recent until the matching push_id reaches a terminal status.

    On ``status == "completed"`` prints a success line and returns. On
    ``status == "failed"`` writes the verbatim ``error_message`` (or a default
    string) to stderr and raises ``SystemExit(1)`` — D-FORMAT3 no-auto-retry.
    """
    while True:
        resp = client.get("/api/platform/v1/downloads/pushes/recent")
        resp.raise_for_status()
        body = resp.json() or {}
        pushes = body.get("pushes") or []
        entry = next((p for p in pushes if p.get("push_id") == push_id), None)
        if entry is None:
            # Not yet visible in the recent feed — treat as in-flight.
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            continue
        status = entry.get("status")
        if status == "completed":
            console.print("[green]push complete[/green]")
            return
        if status == "failed":
            err = entry.get("error_message") or "push failed"
            sys.stderr.write(f"push failed: {err}\n")
            raise SystemExit(1)
        # status is "pending" / "running" / "queued" — keep polling.
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
