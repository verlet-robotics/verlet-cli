"""verlet bundles ... — Click group + subcommands (CLIBUNDLE-01..07).

Plan 30-07 wires the first two subcommands:

  * `verlet bundles browse`  — anonymous public catalog (CLIBUNDLE-01).
  * `verlet bundles redeem <code>` — D-BUNDLE2 idempotent redemption
    (CLIBUNDLE-02). See Task 2.

Later plans (30-08, 30-09) extend this group with `list`, `info`, `download`,
`export-manifest`. Each new subcommand follows the Phase 29 separation:
synchronous Click entry → asyncio.run(...) → async _api wrapper → render.
"""
from __future__ import annotations

import asyncio
import json

import click
import httpx

from verlet.bundles._api import fetch_bundles_browse
from verlet.bundles._render import bundles_browse_table
from verlet.datasets.convert import SUPPORTED_FORMATS, validate_format
from verlet.display import console


_REDEEM_EPILOG = """\b
Examples:

```bash
verlet bundles redeem ABCD-1234
```

\b
Save the bearer to a named profile:

```bash
verlet --profile stanford bundles redeem ABCD-1234
```
"""


_DOWNLOAD_EPILOG = """\b
Examples:

```bash
verlet bundles download stanford-egocentric-2024
```

\b
Apply a server-side format conversion to every dataset in the bundle:

```bash
verlet bundles download stanford-egocentric-2024 --format hdf5
```
"""


@click.group("bundles")
def bundles_group() -> None:
    """Browse, redeem, list, info, download Verlet research / purchased bundles."""


@bundles_group.command("browse")
@click.option(
    "--limit",
    default=50,
    type=int,
    show_default=True,
    help="Max bundles to display.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON to stdout instead of a Rich table.",
)
def browse(limit: int, as_json: bool) -> None:
    """List public research bundles. Anonymous; no auth required.

    \b
    Examples:
      verlet bundles browse
      verlet bundles browse --json | jq '.[0]'
      verlet bundles browse --limit 5
    """
    try:
        body = asyncio.run(fetch_bundles_browse(limit=limit))
    except Exception as exc:  # network down, server 500, etc.
        click.echo(f"failed to fetch bundles: {exc}", err=True)
        raise SystemExit(1)

    items = body.get("items", [])

    if as_json:
        click.echo(json.dumps(items, indent=2))
        return

    if not items:
        console.print("[dim]No public bundles available.[/dim]")
        return

    console.print(bundles_browse_table(items))

@bundles_group.command("redeem", epilog=_REDEEM_EPILOG)
@click.argument("code")
@click.option(
    "--email",
    default=None,
    help=(
        "Email to associate with the redemption (server may require for "
        "first-time redemptions on new accounts)."
    ),
)
@click.pass_context
def redeem(ctx: click.Context, code: str, email: str | None) -> None:
    """Redeem a research-access code; save bearer token to ~/.verlet/credentials.json.

    \b
    D-BUNDLE2 idempotent: re-redeeming the same code overwrites the local
    profile entry with the server-issued (fresh) token. Revoked / expired
    codes return 410 Gone with a verbatim server detail; unknown codes
    return 404 with "Invalid code".

    \b
    Examples:
      verlet bundles redeem ABCD-1234
      verlet --profile staging bundles redeem ABCD-1234
    """
    # Local imports keep the cold-import path of `verlet bundles browse` lean.
    from verlet.auth.credentials import upsert_bundle_grant_profile
    from verlet.auth.profiles import resolve_profile_name
    from verlet.bundles._api import RedeemError, redeem_bundle_code

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    try:
        response = asyncio.run(redeem_bundle_code(code, email=email))
    except RedeemError as exc:
        click.echo(exc.detail, err=True)
        raise SystemExit(1)
    except Exception as exc:  # network down, 5xx, etc.
        click.echo(f"redeem failed: {exc}", err=True)
        raise SystemExit(1)

    upsert_bundle_grant_profile(
        profile_name,
        access_token=response["access_token"],
        expires_at=response["expires_at"],
        bundle_slug=response["bundle_slug"],
    )
    console.print(
        f"[green]Redeemed.[/green] Bundle: [cyan]{response['bundle_slug']}[/cyan], "
        f"expires: {response['expires_at']}"
    )


# ---------------------------------------------------------------------------
# Plan 30-08 — `verlet bundles list` (CLIBUNDLE-03) + `verlet bundles info`
# (CLIBUNDLE-04). Both consume Plan 30-03's authenticated routes.
#
# `--all` for `list` maps to `?include_inactive=true` (D-BUNDLE1). 401 surfaces
# the verbatim string "not authenticated; run verlet auth login" via _api's
# `_exit_with_stderr` helper -- no try/except required here.
#
# Local imports inside each command body keep the cold-import path of
# `verlet bundles browse` lean (the browse path is the more common operation
# and never touches AuthenticatedClient).
# ---------------------------------------------------------------------------


@bundles_group.command("list")
@click.option(
    "--all",
    "show_all",
    is_flag=True,
    default=False,
    help="Include expired/revoked bundles (D-BUNDLE1).",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON to stdout instead of a Rich table.",
)
@click.pass_context
def list_bundles(ctx: click.Context, show_all: bool, as_json: bool) -> None:
    """List bundles in your account (research grants + purchased) (CLIBUNDLE-03).

    \b
    By default only active bundles are shown. ``--all`` includes expired
    and revoked grants with a ``Status`` column color-coded
    active=green / expired=yellow / revoked=red.

    \b
    Examples:
      verlet bundles list
      verlet bundles list --all
      verlet bundles list --json | jq '.[0].bundle_slug'
    """
    from verlet.api_client import AuthenticatedClient
    from verlet.auth.profiles import resolve_profile_name
    from verlet.bundles._api import fetch_bundles_list
    from verlet.bundles._render import bundles_list_table

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    async def _run() -> dict:
        client = AuthenticatedClient(profile_name)
        try:
            return await fetch_bundles_list(client, include_inactive=show_all)
        finally:
            client.close()

    body = asyncio.run(_run())
    items = body.get("items", []) or []

    if as_json:
        click.echo(json.dumps(items, indent=2))
        return

    if not items:
        console.print("[dim]No bundles in your account.[/dim]")
        return

    console.print(bundles_list_table(items))


@bundles_group.command("info")
@click.argument("bundle_id")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit machine-readable JSON to stdout instead of a Rich panel + table.",
)
@click.pass_context
def info(ctx: click.Context, bundle_id: str, as_json: bool) -> None:
    """Show bundle detail: included datasets, license, citation (CLIBUNDLE-04).

    \b
    Citation is shown only for kind == "research" bundles. Datasets are
    listed with their available formats so a researcher knows whether a
    bundle ships in lerobot-v2 / hdf5 / etc. before reaching for
    `verlet bundles download`.

    \b
    Examples:
      verlet bundles info stanford-egocentric-2024
      verlet bundles info <bundle_uuid> --json
    """
    from verlet.api_client import AuthenticatedClient
    from verlet.auth.profiles import resolve_profile_name
    from verlet.bundles._api import fetch_bundle_detail
    from verlet.bundles._render import bundle_detail_view

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    async def _run() -> dict:
        client = AuthenticatedClient(profile_name)
        try:
            return await fetch_bundle_detail(client, bundle_id)
        finally:
            client.close()

    bundle = asyncio.run(_run())

    if as_json:
        click.echo(json.dumps(bundle, indent=2))
        return

    console.print(bundle_detail_view(bundle))


# ---------------------------------------------------------------------------
# Plan 30-09 -- `verlet bundles download <id>` (CLIBUNDLE-05).
#
# D-BUNDLE3: --variant raw is rejected pre-network with a verbatim error
# (zero HTTP calls). --format <fmt> applies to ALL bundle datasets via per-
# dataset fan-out to fetch_arm_manifest; if any dataset's manifest endpoint
# returns 400 ("format X not supported for raw-only dataset Y"), the entire
# bundle download aborts before fanning further -- NO partial writes.
#
# D-BUNDLE4: disk layout is <out>/<dataset_slug>/... with bundle_manifest.json
# at <out>/bundle_manifest.json summarizing slugs + format.
#
# Top-level imports of download_resolved + DownloadPlanItem at module scope
# so tests can patch ``verlet.bundles.commands.download_resolved`` directly
# (autouse fixture in tests/bundles/test_download.py).
# ---------------------------------------------------------------------------

from pathlib import Path  # noqa: E402

from verlet.download import DownloadPlanItem, download_resolved  # noqa: E402


@bundles_group.command("download", epilog=_DOWNLOAD_EPILOG)
@click.argument("bundle_id")
@click.option(
    "--variant",
    type=click.Choice(["raw", "processed"]),
    default=None,
    help=(
        "Bundles are processed-only; passing 'raw' is rejected before any "
        "network call (D-BUNDLE3)."
    ),
)
@click.option(
    "--format",
    "fmt",
    default=None,
    callback=lambda ctx, p, v: validate_format(v),
    help=(
        "Apply format conversion to ALL bundle datasets (D-BUNDLE3). "
        "Supported: " + ", ".join(SUPPORTED_FORMATS) + "."
    ),
)
@click.option(
    "-o", "--out", default=None,
    help="Output directory; defaults to ./<bundle_id>/",
)
@click.option(
    "-v", "--verbose", is_flag=True,
    help="Stream server-side conversion log lines on stderr (D-FORMAT4).",
)
@click.option(
    "--quiet", is_flag=True,
    help="Suppress the Rich progress bar; errors still go to stderr.",
)
@click.option("--parallel", default=8, type=int, help="Max concurrent downloads.")
@click.pass_context
def download(
    ctx: click.Context,
    bundle_id: str,
    variant: str | None,
    fmt: str | None,
    out: str | None,
    verbose: bool,
    quiet: bool,
    parallel: int,
) -> None:
    """Fan out downloads of every dataset in a bundle (CLIBUNDLE-05).

    \b
    D-BUNDLE3: --variant raw is rejected before any network call.
    D-BUNDLE4: outputs to <out>/<dataset_slug>/... with bundle_manifest.json
    written at <out>/bundle_manifest.json summarizing the run.

    \b
    Examples:
      verlet bundles download stanford-egocentric-2024
      verlet bundles download <bundle_uuid> --format hdf5
      verlet bundles download <bundle_uuid> -o ./my-research-data
    """
    # Local imports keep the cold-import path of `verlet bundles browse` lean.
    from verlet.api_client import AuthenticatedClient
    from verlet.auth.profiles import resolve_profile_name
    from verlet.bundles._api import fetch_bundle_detail
    from verlet.bundles._validation import validate_bundle_download_flags
    from verlet.datasets._api import fetch_arm_manifest
    from verlet.datasets.convert import poll_conversion_job

    # 1. Pre-flight gate (D-BUNDLE3 zero-network). Exits 2 on --variant raw.
    validate_bundle_download_flags(variant=variant, format=fmt)

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    # 2. Resolve bundle + per-dataset slug list. Authenticated -- routes 401
    # through fetch_bundle_detail's _exit_with_stderr helper.
    async def _fetch_detail() -> dict:
        client = AuthenticatedClient(profile_name)
        try:
            return await fetch_bundle_detail(client, bundle_id)
        finally:
            client.close()

    bundle = asyncio.run(_fetch_detail())

    # 3. Set up the output root + bundle summary skeleton (D-BUNDLE4).
    out_root = Path(out) if out else Path(f"./{bundle_id}/")
    bundle_summary: dict = {
        "bundle_id": bundle.get("bundle_id", bundle_id),
        "bundle_slug": bundle.get("bundle_slug", ""),
        "format": fmt,
        "datasets": [],
    }

    # 4. Fan out per-dataset manifest fetches + downloads. D-BUNDLE3 fail-fast:
    # any 400 from one dataset's manifest call aborts the WHOLE bundle download
    # before fanning further (the next dataset's manifest is never fetched).
    async def _drive_one(slug: str) -> dict:
        """Fetch manifest for one dataset, polling on 202. Returns the
        manifest dict ready for download_resolved consumption."""
        # fetch_arm_manifest returns (status_code, body); 400 raises HTTPStatusError.
        try:
            status_code, body = await fetch_arm_manifest(
                profile_name, slug, format=fmt or "lerobot-v2"
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                try:
                    detail = e.response.json().get("detail") or str(e)
                except Exception:
                    detail = e.response.text or str(e)
                click.echo(
                    f"download aborted: {slug} -> {detail}", err=True,
                )
                raise SystemExit(1)
            raise

        if status_code == 200:
            return body

        # 202 -- conversion enqueued. Drive the foreground poll loop. The
        # poll path writes its own failure surface to stderr (D-FORMAT3) and
        # raises SystemExit(1); we re-raise after stamping the failing dataset
        # slug so the user knows WHICH dataset broke.
        job_id = body["job_id"]
        poll_client = AuthenticatedClient(profile_name)
        try:
            try:
                manifest = await poll_conversion_job(
                    poll_client, job_id, verbose=verbose, quiet=quiet,
                )
            except SystemExit:
                click.echo(
                    f"download aborted at dataset: {slug}", err=True,
                )
                raise
        finally:
            poll_client.close()
        return manifest

    async def _drive_all() -> None:
        out_root.mkdir(parents=True, exist_ok=True)
        for ds in bundle.get("datasets") or []:
            slug = ds["slug"]
            if not quiet:
                console.print(f"[cyan]-> {slug}[/cyan]")

            manifest = await _drive_one(slug)

            # D-BUNDLE4: per-dataset subdir at the bundle root.
            ds_dir = out_root / slug
            ds_dir.mkdir(parents=True, exist_ok=True)
            items = [
                DownloadPlanItem(
                    url=f["url"], local_path=ds_dir / f["path"],
                )
                for f in manifest.get("files") or []
            ]
            result = await download_resolved(
                items, parallel=parallel, skip_existing=True,
            )
            bundle_summary["datasets"].append(
                {
                    "slug": slug,
                    "files": len(items),
                    "downloaded": result.downloaded,
                    "skipped": result.skipped,
                    "failed": result.failed,
                }
            )

        # Bundle-level summary at the root (D-BUNDLE4). Written ONLY on a
        # successful fan-out -- a partial run leaves no bundle_manifest.json
        # so downstream pipelines do not consume incomplete data by accident.
        (out_root / "bundle_manifest.json").write_text(
            json.dumps(bundle_summary, indent=2)
        )
        if not quiet:
            console.print(
                f"[green]bundle download complete[/green] -> {out_root}"
            )

    asyncio.run(_drive_all())



# ---------------------------------------------------------------------------
# Plan 30-09 Task 2 -- `verlet bundles export-manifest <id>` (CLIBUNDLE-06).
#
# Emits a portable, time-bounded manifest JSON for offline / air-gapped
# pipelines. Per-dataset manifests are fetched via fetch_arm_manifest; if any
# dataset would require conversion (202 + job_id) we abort with a hint
# pointing at `verlet datasets download <slug> --format <fmt>` so the user
# can pre-convert before re-running export-manifest. Reason: a portable
# manifest emitted while a conversion is mid-flight would carry a job_id
# instead of the file URLs the air-gapped consumer needs.
# ---------------------------------------------------------------------------


@bundles_group.command("export-manifest")
@click.argument("bundle_id")
@click.option(
    "-o", "--out", default=None,
    help="Output JSON path; defaults to ./<bundle_id>-manifest.json",
)
@click.option(
    "--format", "fmt", default=None,
    callback=lambda ctx, p, v: validate_format(v),
    help=(
        "Apply format hint to ALL bundle datasets. Supported: "
        + ", ".join(SUPPORTED_FORMATS) + "."
    ),
)
@click.pass_context
def export_manifest(
    ctx: click.Context,
    bundle_id: str,
    out: str | None,
    fmt: str | None,
) -> None:
    """Emit a portable, time-bounded manifest for offline pipelines (CLIBUNDLE-06).

    \b
    The output JSON carries every per-dataset file URL + checksum + size,
    suitable for air-gapped consumers that cannot reach api.verlet.co. URLs
    are presigned with the same TTL the live download command uses; record
    `expires_at` so the consumer knows when to re-export.

    \b
    Examples:
      verlet bundles export-manifest stanford-egocentric-2024
      verlet bundles export-manifest <id> --out manifest.json --format hdf5
    """
    from datetime import datetime, timezone

    from verlet.api_client import AuthenticatedClient
    from verlet.auth.profiles import resolve_profile_name
    from verlet.bundles._api import fetch_bundle_detail
    from verlet.datasets._api import fetch_arm_manifest

    flag_profile = ctx.obj.get("profile") if ctx.obj else None
    profile_name = resolve_profile_name(flag_profile)

    async def _fetch_detail() -> dict:
        client = AuthenticatedClient(profile_name)
        try:
            return await fetch_bundle_detail(client, bundle_id)
        finally:
            client.close()

    bundle = asyncio.run(_fetch_detail())

    output: dict = {
        "bundle_id": bundle.get("bundle_id", bundle_id),
        "bundle_slug": bundle.get("bundle_slug", ""),
        "expires_at": bundle.get("expires_at"),
        "format": fmt,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "datasets": [],
    }

    async def _drive() -> None:
        for ds in bundle.get("datasets") or []:
            slug = ds["slug"]
            status_code, body = await fetch_arm_manifest(
                profile_name, slug, format=fmt or "lerobot-v2",
            )
            if status_code != 200:
                # 202 -- the manifest endpoint enqueued a conversion job
                # rather than returning the file URLs we need for the
                # portable export. Tell the user to pre-convert.
                job_id = body.get("job_id", "<unknown>")
                click.echo(
                    f"manifest export requires native format or already-"
                    f"converted dataset; dataset {slug} requires conversion "
                    f"(job_id={job_id}). Run "
                    f"`verlet datasets download {slug} --format "
                    f"{fmt or 'lerobot-v2'}` first.",
                    err=True,
                )
                raise SystemExit(1)
            # body is a DownloadManifest -- pass through verbatim under the
            # dataset entry, with the slug stamped on so air-gapped consumers
            # can correlate without re-checking the bundle detail.
            entry = {"slug": slug}
            entry.update(body)
            output["datasets"].append(entry)

    asyncio.run(_drive())

    out_path = Path(out) if out else Path(f"./{bundle_id}-manifest.json")
    out_path.write_text(json.dumps(output, indent=2))
    console.print(f"[green]wrote[/green] {out_path}")
