"""verlet datasets jobs — single-job reattach (CLIDATA-07 SC4, Plan 30-06).

D-FORMAT1 reattach behavior:

  * `verlet datasets jobs <job_id>` — drives the same poll loop as foreground
    `verlet datasets download --format <fmt>`. On completion, downloads the
    inlined manifest. On failure, prints the verbatim server error and exits
    non-zero. On 404, prints "job not found" and exits 1.

  * `verlet datasets jobs --slug <ds>` — lists conversion jobs for one
    dataset via `GET /api/platform/v1/downloads/{slug}/conversions`.

  * `verlet datasets jobs` (no argument) — lists every conversion job the
    account has triggered via `GET /api/platform/v1/downloads/jobs` (G-P5).

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


@click.command("jobs")
@click.argument("job_id", required=False)
@click.option(
    "--slug",
    default=None,
    help="List conversion jobs for one dataset instead of reattaching.",
)
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
    slug: str | None,
    verbose: bool,
    quiet: bool,
    output: str,
) -> None:
    """Reattach to a conversion job, or list conversion jobs (G-P5).

    \b
    Examples:
      verlet datasets jobs job-abc123     # reattach + download when ready
      verlet datasets jobs --slug my-ds   # list one dataset's conversion jobs
      verlet datasets jobs                # list every job your account has
    """
    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    # Reattach path — a job_id was given.
    if job_id is not None:
        try:
            require_profile(profile_name)
        except ProfileNotFoundError:
            raise click.ClickException(
                "Not authenticated. Run `verlet auth login` to reattach to jobs."
            )
        # Async lives in a helper so the Click entry stays small and the test
        # can patch ``asyncio.sleep`` on the convert module's reference.
        asyncio.run(_run_reattach(profile_name, job_id, verbose, quiet, output))
        return

    # Listing path — no job_id. `--slug` lists one dataset's conversions;
    # otherwise list every conversion job the account has triggered.
    try:
        require_profile(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` to list conversion jobs."
        )

    from verlet.datasets._api import fetch_all_jobs, fetch_dataset_conversions
    from verlet.datasets._render import conversions_table

    if slug:
        items = asyncio.run(fetch_dataset_conversions(profile_name, slug))
    else:
        items = asyncio.run(fetch_all_jobs(profile_name))

    if not items:
        scope = f"dataset '{slug}'" if slug else "your account"
        console.print(f"[dim]No conversion jobs for {scope}.[/dim]")
        return
    console.print(conversions_table(items))


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
