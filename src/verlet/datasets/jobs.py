"""verlet datasets jobs — single-job reattach (CLIDATA-07 SC4, Plan 30-06).

D-FORMAT1 reattach behavior:

  * `verlet datasets jobs <job_id>` — drives the same poll loop as foreground
    `verlet datasets download --format <fmt>`. On completion, downloads the
    inlined manifest. On failure, prints the verbatim server error and exits
    non-zero. On 404, prints "job not found" and exits 1.

  * `verlet datasets jobs` (no argument) — prints a deferred-feature notice
    and exits 0 WITHOUT making any HTTP call. Listing is intentionally NOT
    shipped in Phase 30 because the backend doesn't expose
    `GET /api/platform/v1/downloads/jobs?account_id=<self>` yet (verified
    during planning). A future phase can add the endpoint without changing
    this CLI invocation contract — the bare command keeps working.

The reattach path REUSES Plan 30-04's ``poll_conversion_job`` from
``verlet.datasets.convert``; the Rich progress UX + stderr-on-failure
contract live in one place.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click
import httpx

from verlet.api_client import AuthenticatedClient
from verlet.auth.profiles import (
    ProfileNotFoundError,
    require_profile,
    resolve_profile_name,
)
from verlet.datasets.convert import poll_conversion_job
from verlet.display import console
from verlet.download import DownloadPlanItem, download_resolved


LISTING_DEFERRED_MSG = (
    "Listing not yet supported by server. "
    "Pass <job_id> to reattach to a specific job."
)
"""D-FORMAT1 deferred-feature notice. Byte-asserted in
``tests/datasets/test_jobs_reattach.py::test_jobs_no_id_prints_listing_deferred_notice_no_http``
so that any future copy-paste drift surfaces as a test failure. The wording
points the user at the supported single-job reattach path."""


@click.command("jobs")
@click.argument("job_id", required=False)
@click.option(
    "-v", "--verbose", is_flag=True,
    help="Stream server-side conversion log lines on stderr (D-FORMAT4).",
)
@click.option(
    "--quiet", is_flag=True,
    help="Suppress the Rich progress bar; errors still go to stderr.",
)
@click.option(
    "-o", "--output", default="./verlet-data",
    help="Output directory (per-dataset subdir rooted at {output}/{slug}/).",
)
@click.pass_context
def jobs(
    ctx: click.Context,
    job_id: str | None,
    verbose: bool,
    quiet: bool,
    output: str,
) -> None:
    """Reattach to a specific conversion job (listing not yet available).

    \b
    Examples:
      verlet datasets jobs job-abc123     # reattach + download when ready
      verlet datasets jobs                # listing-deferred notice (exit 0)
    """
    if job_id is None:
        # Listing not yet supported — print the deferred-feature notice and
        # exit 0 with ZERO HTTP work. Future server work can ship the listing
        # without changing this CLI's invocation contract.
        click.echo(LISTING_DEFERRED_MSG)
        return

    # 1. Auth gate — single-job reattach requires an active profile.
    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)
    try:
        require_profile(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` to reattach to jobs."
        )

    # 2. Drive the async runner. Async lives in a helper so the Click entry
    # stays small and the test can patch ``asyncio.sleep`` on the convert
    # module's reference.
    asyncio.run(_run_reattach(profile_name, job_id, verbose, quiet, output))


async def _run_reattach(
    profile_name: str,
    job_id: str,
    verbose: bool,
    quiet: bool,
    output: str,
) -> None:
    """Single GET on the jobs endpoint, then branch by status.

    completed → download immediately. failed → verbatim error + exit 1.
    pending/processing → drive ``poll_conversion_job`` (which writes its own
    failure path on stderr per D-FORMAT3). 404 → "job not found" + exit 1.
    """
    client = AuthenticatedClient(profile_name)
    try:
        try:
            resp = client.get(
                f"/api/platform/v1/downloads/jobs/{job_id}"
            )
        except httpx.HTTPError as e:
            sys.stderr.write(f"failed to reach jobs endpoint: {e}\n")
            raise SystemExit(1)

        if resp.status_code == 404:
            sys.stderr.write(f"job not found: {job_id}\n")
            raise SystemExit(1)
        if resp.status_code >= 400:
            try:
                detail = (resp.json() or {}).get("detail") or resp.text
            except ValueError:
                detail = resp.text
            sys.stderr.write(f"jobs endpoint error: {detail}\n")
            raise SystemExit(1)

        first = resp.json()
        status = first["status"]

        if status == "completed":
            manifest = first["manifest"]
        elif status == "failed":
            err = first.get("error_message") or "conversion failed"
            stage = first.get("failed_stage")
            msg = f"conversion failed: {err}"
            if stage:
                msg += f" (stage: {stage})"
            sys.stderr.write(msg + "\n")
            raise SystemExit(1)
        else:
            # pending or processing — drive the same poll loop the foreground
            # download path uses. poll_conversion_job handles the failure path
            # itself (stderr + SystemExit(1)) per D-FORMAT3.
            manifest = await poll_conversion_job(
                client, job_id, verbose=verbose, quiet=quiet,
            )

        # 3. Drive the download. The manifest's path field is the canonical
        # relative layout (CONTEXT.md Discretion: no client-side restructuring).
        slug = manifest.get("dataset_slug") or job_id
        output_root = Path(output) / slug
        items = [
            DownloadPlanItem(
                url=f["url"],
                local_path=output_root / f["path"],
            )
            for f in manifest.get("files") or []
        ]
        result = await download_resolved(items, parallel=8, skip_existing=True)

        # 4. Summary line + non-zero exit on partial failure (mirrors the
        # download command's behavior).
        summary_parts = [f"[green]{result.downloaded}[/green] downloaded"]
        if result.skipped:
            summary_parts.append(f"[dim]{result.skipped} skipped[/dim]")
        if result.failed:
            summary_parts.append(f"[red]{result.failed} failed[/red]")
        if not quiet:
            console.print(f"\n{', '.join(summary_parts)} -> {output_root}")
        if result.failed > 0:
            raise SystemExit(1)
    finally:
        client.close()
