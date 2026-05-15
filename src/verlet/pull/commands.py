"""``verlet pull <slug>`` — fetch a dataset manifest and download its files.

Branches on the active profile's ``kind``:

  showcase_access_code
    Hits ``GET /api/v1/ego/showcase/datasets/{slug}/download?variant=&scope=``.
    The backend gates by ``ego_showcase_access_grant`` rows attached to the
    authed access code. 404 means either the dataset doesn't exist OR the
    caller has no grant for it (the backend deliberately conflates these to
    prevent enumeration; we render the same message either way).

  device_flow / pat
    Hits the platform's public free-samples endpoint. Full-dataset downloads
    for platform accounts are out of scope for this command — we surface a
    clear hint that they should buy/push from verlet.co/catalog/<slug>.

  (no profile)
    Exits with a non-zero status and a hint pointing at ``verlet auth login``.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click

from verlet._http_errors import friendly_http
from verlet.api_client import AuthenticatedClient
from verlet.auth.profiles import ProfileNotFoundError
from verlet.download import DownloadPlanItem, download_resolved

SHOWCASE_DOWNLOAD_PATH = "/api/v1/ego/showcase/datasets/{slug}/download"
PLATFORM_SAMPLES_PATH = "/api/platform/v1/catalog/datasets/{slug}/samples/download"


def _plan_items(slug: str, dest_root: Path, manifest: dict) -> list[DownloadPlanItem]:
    """Project a manifest response into (url, local_path) pairs.

    The showcase and platform responses share the same field names
    (``episodes[].parquet_url``, ``episodes[].video_urls[]``,
    ``episodes[].meta_urls[]``) — same parser works for both. Meta files
    are stamped per-episode in the response but logically dataset-global;
    we de-duplicate by filename so we only write each meta file once.
    """
    out: list[DownloadPlanItem] = []
    dataset_dir = dest_root / slug
    seen_meta: set[str] = set()

    for ep in manifest.get("episodes", []):
        ep_idx = ep["episode_index"]
        ep_dir = dataset_dir / f"episode_{ep_idx:06d}"
        if ep.get("parquet_url"):
            out.append(
                DownloadPlanItem(
                    url=ep["parquet_url"],
                    local_path=ep_dir / f"episode_{ep_idx:06d}.parquet",
                )
            )
        for v in ep.get("video_urls", []):
            out.append(
                DownloadPlanItem(
                    url=v["url"],
                    local_path=ep_dir / "videos" / f"{v['camera']}.mp4",
                )
            )
        for m in ep.get("meta_urls", []):
            if m["filename"] in seen_meta:
                continue
            seen_meta.add(m["filename"])
            out.append(
                DownloadPlanItem(
                    url=m["url"],
                    local_path=dataset_dir / "meta" / m["filename"],
                )
            )
    return out


def _print_plan(manifest: dict, items: list[DownloadPlanItem]) -> None:
    click.echo(f"Dataset: {manifest.get('dataset_title')} ({manifest.get('dataset_slug')})")
    click.echo(f"Format:  {manifest.get('format')}")
    click.echo(f"Variant: {manifest.get('variant')}, scope: {manifest.get('scope')}")
    click.echo(f"Episodes: {len(manifest.get('episodes', []))}, files: {len(items)}")
    quota = manifest.get("quota_remaining")
    if quota:
        b = quota.get("bytes")
        e = quota.get("episodes")
        parts = []
        if b is not None:
            parts.append(f"{b} bytes remaining")
        if e is not None:
            parts.append(f"{e} episodes remaining")
        if parts:
            click.echo(f"Quota:   {', '.join(parts)}")
    click.echo("")
    for it in items[:10]:
        click.echo(f"  {it.local_path}")
    if len(items) > 10:
        click.echo(f"  … and {len(items) - 10} more")


@click.command("pull")
@click.argument("slug")
@click.option(
    "--variant",
    type=click.Choice(["raw", "processed"]),
    default="processed",
    show_default=True,
    help="Data variant to pull. 'processed' = HaWoR overlays + hand pose; 'raw' = RGB + depth pre-annotation.",
)
@click.option(
    "--scope",
    type=click.Choice(["samples", "full"]),
    default="full",
    show_default=True,
    help="'samples' = the dataset's free-sample episode subset; 'full' = every episode (subject to grant).",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(path_type=Path, file_okay=False),
    default=Path("./verlet-data"),
    show_default=True,
    help="Destination directory. Files land under <output>/<slug>/.",
)
@click.option(
    "--parallel",
    type=int,
    default=8,
    show_default=True,
    help="Concurrent downloads.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print the plan and exit without downloading.",
)
@click.pass_context
def pull_command(
    ctx: click.Context,
    slug: str,
    variant: str,
    scope: str,
    output: Path,
    parallel: int,
    dry_run: bool,
) -> None:
    """Download a dataset by slug.

    With a showcase access-code profile, you can only pull datasets your
    code has a grant for — anything else returns "No access". For platform
    accounts (``verlet auth login`` device flow), this command currently
    serves only the public free-samples surface; for full-dataset pulls
    visit https://verlet.co/catalog/{slug}.
    """
    profile_name = ctx.obj.get("profile") if ctx.obj else None
    try:
        client = AuthenticatedClient(profile_name)
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login --kind showcase` with your "
            "access code, or `verlet auth login` for a platform account."
        )

    try:
        kind = client.kind
        if kind == "showcase_access_code":
            path = SHOWCASE_DOWNLOAD_PATH.format(slug=slug)
            params = {"variant": variant, "scope": scope}
        elif kind in ("device_flow", "pat"):
            if scope != "samples":
                raise click.ClickException(
                    "Platform accounts can only pull the public free-samples tier "
                    "from the CLI today. Pass `--scope samples`, or visit "
                    f"https://verlet.co/catalog/{slug} to buy or push to cloud."
                )
            path = PLATFORM_SAMPLES_PATH.format(slug=slug)
            params = {}
        elif kind == "bundle_grant":
            raise click.ClickException(
                "Bundle-grant profiles cannot use `verlet pull`. Use `verlet bundles` "
                "to redeem and download research bundles."
            )
        else:
            raise click.ClickException(
                f"Profile kind '{kind}' is not supported by `verlet pull`."
            )

        with friendly_http(f"fetching download manifest for '{slug}'"):
            resp = client.get(path, params=params)
            if resp.status_code == 404:
                raise click.ClickException(
                    f"No access to dataset '{slug}'. "
                    "Either the dataset does not exist, or your access code "
                    "has no grant for the requested variant/scope. "
                    "Contact your Verlet rep to request access."
                )
            if resp.status_code == 429:
                raise click.ClickException(
                    "Rate-limited or quota exhausted for this grant. "
                    "Try again later or contact your Verlet rep."
                )
            resp.raise_for_status()
            manifest = resp.json()
    finally:
        client.close()

    items = _plan_items(slug, output, manifest)
    if not items:
        click.echo(
            f"No files in manifest for '{slug}' (variant={variant}, scope={scope}). "
            "If you expected samples, the admin may not have configured them yet."
        )
        return

    if dry_run:
        _print_plan(manifest, items)
        return

    _print_plan(manifest, items)
    click.echo("")
    result = asyncio.run(download_resolved(items, parallel=parallel))
    click.echo(
        f"Done. downloaded={result.downloaded} skipped={result.skipped} "
        f"failed={result.failed}"
    )
    if result.failed:
        sys.exit(1)
