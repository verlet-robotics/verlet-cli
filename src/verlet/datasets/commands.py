"""Click group + list/info/download subcommands for `verlet datasets`.

This is the Phase 29 user-facing surface. Helpers live in sibling modules:

- ``_api.py`` — httpx wrappers for the platform catalog + downloads endpoints.
- ``_validation.py`` — pre-flight flag matrix (D-MOD2, D-FL1).
- ``_render.py`` — Rich table builders.

Modality is a property of the catalog row, not of the command; ``download``
auto-detects via ``is_ego_row(detail)`` and dispatches to the right manifest
endpoint (D-MOD2). Anonymous browse is supported for ``list`` and ``info``;
``download`` requires an active profile (D-MOD4) and fails fast pre-HTTP if
none is configured.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import click

from verlet.auth.profiles import (
    ProfileNotFoundError,
    require_profile,
    resolve_profile_name,
)
from verlet.datasets._api import (
    build_list_params,
    fetch_arm_manifest,
    fetch_catalog_detail,
    fetch_catalog_list,
    fetch_ego_manifest,
    is_ego_row,
)
from verlet.datasets._render import (
    dataset_info_json,
    dataset_info_text,
    dataset_list_table,
    list_truncation_footer,
)
from verlet.datasets._validation import (
    validate_download_flags,
    validate_kind_category,
)
from verlet.datasets.convert import (
    SUPPORTED_FORMATS,
    poll_conversion_job,
    validate_format,
)
from verlet.display import console
from verlet.download import DownloadPlanItem, DownloadResult, download_resolved
from verlet.license import (
    check_license_accepted,
    prompt_license_acceptance,
    write_license_file,
)


@click.group("datasets")
def datasets_group() -> None:
    """Browse, inspect, and download Verlet datasets (arm + ego)."""


@datasets_group.command("list")
@click.option(
    "--task",
    "task_type",
    multiple=True,
    help="Filter by task name (repeatable).",
)
@click.option(
    "--robot",
    "robot_embodiment",
    multiple=True,
    help="Filter by robot embodiment (repeatable).",
)
@click.option(
    "--category",
    default=None,
    help="Filter by ego segment category (ego-only).",
)
@click.option(
    "--since",
    default=None,
    help="ISO-8601 timestamp (e.g. 2026-04-01); filters on published_at.",
)
@click.option("--limit", default=20, type=int, help="Page size (max 100).")
@click.option(
    "--kind",
    type=click.Choice(["all", "teleop", "ego"]),
    default="all",
    help="Filter by modality (teleop maps to arm internally).",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit JSON array (raw CatalogDatasetListItem[]) instead of a Rich table.",
)
@click.pass_context
def datasets_list(
    ctx: click.Context,
    task_type: tuple[str, ...],
    robot_embodiment: tuple[str, ...],
    category: str | None,
    since: str | None,
    limit: int,
    kind: str,
    as_json: bool,
) -> None:
    """List datasets matching the given filters."""
    # Pre-flight: --category is ego-only when --kind is set explicitly to teleop.
    validate_kind_category(kind=kind, category=category)

    profile_name = ctx.obj.get("profile") if ctx.obj else None
    params = build_list_params(
        task_type=task_type,
        robot_embodiment=robot_embodiment,
        category=category,
        since=since,
        limit=limit,
        kind=kind,
    )
    body = asyncio.run(fetch_catalog_list(profile_name, params))
    items = body.get("items") or []
    total = body.get("total")
    if total is None:
        total = len(items)

    if as_json:
        click.echo(json.dumps(items, indent=2, default=str))
        return

    if not items:
        console.print("[dim]No datasets found.[/dim]")
        return

    console.print(dataset_list_table(items))
    footer = list_truncation_footer(total=total, returned=len(items))
    if footer:
        console.print(f"[dim]{footer}[/dim]")


@datasets_group.command("info")
@click.argument("slug")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Emit raw CatalogDatasetDetail JSON.",
)
@click.pass_context
def datasets_info(ctx: click.Context, slug: str, as_json: bool) -> None:
    """Inspect a dataset by slug or UUID.

    Resolution is slug-primary with full-UUID fallback (D-MOD3). Anonymous
    callers see public rows; authenticated callers additionally see
    restricted-to-namespace rows.
    """
    profile_name = ctx.obj.get("profile") if ctx.obj else None
    detail = asyncio.run(fetch_catalog_detail(profile_name, slug))

    if as_json:
        click.echo(dataset_info_json(detail))
        return

    meta_table, bottom_table = dataset_info_text(detail)
    console.print(meta_table)
    console.print()
    console.print(bottom_table)


_DOWNLOAD_EPILOG = """\b
Examples:

```bash
verlet datasets download imitate-cube
```

\b
Convert to HDF5 before download (server-side conversion + foreground polling):

```bash
verlet datasets download imitate-cube --format hdf5
```

\b
Queue conversion + return immediately; reattach later:

```bash
verlet datasets download imitate-cube --format hdf5 --detach
verlet datasets jobs <job_id>
```

\b
Ego dataset processed-variant download:

```bash
verlet datasets download stanford-cooking-ego --variant processed
```
"""


@datasets_group.command("download", epilog=_DOWNLOAD_EPILOG)
@click.argument("slug")
@click.option(
    "--variant",
    type=click.Choice(["raw", "processed"]),
    default=None,
    help="REQUIRED for ego datasets; rejected for teleop.",
)
@click.option(
    "--episode-ids",
    default=None,
    help="CSV of integer episode IDs (arm or ego raw).",
)
@click.option(
    "--segment-ids",
    default=None,
    help="CSV of segment IDs (ego processed only).",
)
@click.option(
    "-o",
    "--output",
    default="./verlet-data",
    help="Output directory (per-dataset subdir rooted at {output}/{slug}/).",
)
@click.option(
    "--format",
    default=None,
    callback=lambda ctx, p, v: validate_format(v),
    help=(
        "Convert before download. One of: "
        + ", ".join(SUPPORTED_FORMATS)
        + ". Native (lerobot-v2) returns immediately; non-native enqueues a "
        "server-side conversion and the CLI polls until ready (D-FORMAT1)."
    ),
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Stream server-side conversion log lines on stderr (D-FORMAT4).",
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Suppress the Rich progress bar; errors still go to stderr.",
)
@click.option(
    "--detach",
    is_flag=True,
    help=(
        "Queue conversion + return job_id immediately (D-FORMAT1). Requires "
        "--format. Reattach later with `verlet datasets jobs <job_id>`."
    ),
)
@click.option("--parallel", default=8, type=int, help="Max concurrent downloads.")
@click.option(
    "--resume/--no-resume",
    default=True,
    help="Skip files already on disk with nonzero size.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="List files that would be downloaded; no writes.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Re-download files even when --resume would skip them.",
)
@click.pass_context
def datasets_download(
    ctx: click.Context,
    slug: str,
    variant: str | None,
    episode_ids: str | None,
    segment_ids: str | None,
    output: str,
    format: str | None,
    verbose: bool,
    quiet: bool,
    detach: bool,
    parallel: int,
    resume: bool,
    dry_run: bool,
    force: bool,
) -> None:
    """Download a dataset by slug. Auto-detects modality from the catalog row.

    With ``--format``, server-side conversion runs first; the CLI polls
    ``/downloads/jobs/{id}`` until ready (D-FORMAT1 foreground default). On
    failure the verbatim ``error_message`` (and ``failed_stage`` if present)
    is printed to stderr and the CLI exits non-zero (D-FORMAT3 no auto-retry).

    With ``--detach``, the CLI POSTs the conversion request, prints the
    server-issued ``job_id``, and exits 0 immediately. The user can later run
    ``verlet datasets jobs <job_id>`` to reattach to the polling loop. ``--detach``
    is a no-op for native (200) manifests — there is no async job to background,
    so the CLI errors out instead of silently downloading.
    """
    # 0. --detach requires --format (D-FORMAT1). Foreground native (no --format)
    # is already synchronous + has no server-side job to background — detaching
    # is meaningless.
    if detach and not format:
        raise click.UsageError("--detach requires --format")

    # 1. Auth gate (D-MOD4) — fail fast pre-HTTP. Resolve the profile name from
    # the root --profile flag / VERLET_PROFILE env / credentials.json default,
    # then require_profile() to confirm an entry exists.
    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)
    try:
        require_profile(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` to download datasets."
        )

    # 2. Resolve catalog row to determine modality (D-MOD2). The same
    # /catalog/datasets/{slug_or_id} endpoint backs `verlet datasets info`.
    detail = asyncio.run(fetch_catalog_detail(profile_name, slug))
    modality = "ego" if is_ego_row(detail) else "arm"

    # 3. Pre-flight flag validation.
    validate_download_flags(
        modality=modality,
        variant=variant,
        episode_ids=episode_ids,
        segment_ids=segment_ids,
        format=format,
    )

    # 4. License acceptance (license file persists on first acceptance).
    # --detach (no download) skips the prompt the same way --dry-run does.
    if not dry_run and not detach and not check_license_accepted():
        if not prompt_license_acceptance():
            console.print("[dim]Download cancelled.[/dim]")
            return

    # 5. Fetch manifest. Arm endpoint may return 202 + job_id for non-native
    # formats — branch into the conversion-poll loop in that case (CLIDATA-07).
    if modality == "arm":
        status_code, body = asyncio.run(
            fetch_arm_manifest(
                profile_name,
                slug,
                episode_ids=episode_ids,
                format=format or "lerobot-v2",
            )
        )
        if status_code == 202:
            # Server enqueued a conversion job. With --detach, we print the
            # job_id and exit 0 immediately (D-FORMAT1 background mode); the
            # user reattaches later via `verlet datasets jobs <id>`. Without
            # --detach, drive the poll loop to completion (D-FORMAT1 foreground
            # default) — poll_conversion_job writes the failure path to stderr
            # + raises SystemExit(1) per D-FORMAT3.
            job_id = body["job_id"]
            if detach:
                if quiet:
                    # --detach --quiet: just the bare job_id on stdout, nothing
                    # else (test_detach_quiet_prints_only_job_id).
                    click.echo(job_id)
                else:
                    console.print(
                        f"[green]queued[/green] job_id=[cyan]{job_id}[/cyan]"
                    )
                    console.print(
                        f"reattach: [dim]verlet datasets jobs {job_id}[/dim]"
                    )
                return  # exit 0 — no polling, no download.

            from verlet.api_client import AuthenticatedClient

            poll_client = AuthenticatedClient(profile_name)
            try:
                manifest = asyncio.run(
                    poll_conversion_job(
                        poll_client,
                        job_id,
                        verbose=verbose,
                        quiet=quiet,
                    )
                )
            finally:
                poll_client.close()
        else:
            # 200 — native format; manifest is inlined in the response body.
            # --detach against a native response is meaningless (no job to
            # background); fail loudly instead of silently downloading.
            if detach:
                raise click.UsageError(
                    "no conversion job to detach from; native format ready",
                )
            manifest = body
    else:
        # variant is non-None here (validate_download_flags would have raised).
        assert variant is not None
        manifest = asyncio.run(
            fetch_ego_manifest(
                profile_name,
                slug,
                variant=variant,
                episode_ids=episode_ids,
                segment_ids=segment_ids,
            )
        )

    # 7. Build plan from manifest's signed URLs + paths. The manifest's path
    # field is treated as the canonical relative layout — no client-side
    # restructuring (CONTEXT.md Discretion §"Output directory layout").
    output_root = Path(output) / manifest["dataset_slug"]
    items = [
        DownloadPlanItem(
            url=f["url"],
            local_path=output_root / f["path"],
        )
        for f in manifest.get("files") or []
    ]

    if dry_run:
        console.print(
            f"[bold]Would download {len(items)} files to {output_root}[/bold]"
        )
        for it in items[:20]:
            console.print(f"  {it.local_path}")
        if len(items) > 20:
            console.print(f"  ... and {len(items) - 20} more")
        return

    # 8. Dispatch.
    result: DownloadResult = asyncio.run(
        download_resolved(
            items,
            parallel=parallel,
            skip_existing=resume and not force,
        )
    )
    if result.downloaded > 0:
        write_license_file(output_root)

    # 9. Partial-failure exit (ROADMAP §29 SC3 — exit non-zero on any failed
    # file, but always emit the summary line so the user can see what landed
    # on disk).
    summary_parts = [f"[green]{result.downloaded}[/green] downloaded"]
    if result.skipped:
        summary_parts.append(f"[dim]{result.skipped} skipped[/dim]")
    if result.failed:
        summary_parts.append(f"[red]{result.failed} failed[/red]")
    console.print(f"\n{', '.join(summary_parts)} -> {output_root}")
    if result.failed > 0:
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# Plan 30-05 (CLIDATA-07): register `verlet datasets push <slug> --to ...`.
#
# Implementation lives in ``verlet.datasets.push`` — keeping the command
# definition out-of-band leaves this file focused on list/info/download.
# ---------------------------------------------------------------------------

from verlet.datasets.push import push as push_command  # noqa: E402

datasets_group.add_command(push_command)


# ---------------------------------------------------------------------------
# Plan 30-06 (CLIDATA-07 SC4): register `verlet datasets jobs <job_id>` for
# single-job reattach (listing deferred — backend listing endpoint absent).
# ---------------------------------------------------------------------------

from verlet.datasets.jobs import jobs as jobs_command  # noqa: E402

datasets_group.add_command(jobs_command)
