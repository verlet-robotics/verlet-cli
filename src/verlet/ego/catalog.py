"""Ego catalog fetcher."""
import click
import httpx

from verlet._http_errors import friendly_http
from verlet.api_client import AuthenticatedClient, auth_headers_for_profile
from verlet.auth.profiles import ProfileNotFoundError

SHOWCASE_PREFIX = "/api/v1/ego/showcase"

# Training-bundle assets (video + depth + poses) are the default for
# `verlet ego download --training`. The legacy types (overlay, rrd, clean,
# egodex) remain for 0.3.x back-compat.
TRAINING_ASSET_TYPES = ("video", "depth", "poses")
LEGACY_ASSET_TYPES = ("overlay", "rrd", "egodex", "clean")
ASSET_TYPES = TRAINING_ASSET_TYPES + LEGACY_ASSET_TYPES


def _auth_headers() -> dict[str, str]:
    """Return ``Authorization: Bearer …`` for the active profile.

    Routes through ``api_client.auth_headers_for_profile()`` so any of the
    three profile kinds (device_flow / pat / showcase_access_code) and the
    opportunistic-refresh logic in AuthenticatedClient stay in one place.
    Plan 28-04 / Research §13.6 Plan B.
    """
    try:
        return auth_headers_for_profile()
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` first."
        )


def _api_url() -> str:
    """Return the active profile's api_url (replaces 0.4.0 ``get_api_url``)."""
    try:
        client = AuthenticatedClient()
        try:
            return client.api_url
        finally:
            client.close()
    except ProfileNotFoundError:
        raise click.ClickException(
            "Not authenticated. Run `verlet auth login` first."
        )


async def fetch_ego_catalog(category: str | None = None) -> dict:
    url = f"{_api_url()}{SHOWCASE_PREFIX}/catalog"
    params = {}
    if category:
        params["category"] = category

    with friendly_http("fetching ego catalog"):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                url,
                params=params,
                headers=_auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()


async def presign_ego_asset(segment_id: str, asset: str = "overlay") -> str:
    url = f"{_api_url()}{SHOWCASE_PREFIX}/segments/{segment_id}/presign"
    with friendly_http(f"presigning {asset} for segment {segment_id[:8]}"):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                url,
                params={"asset": asset},
                headers=_auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()["url"]


async def fetch_training_bundle(
    client: httpx.AsyncClient, segment_id: str
) -> dict:
    """Fetch the full training-bundle manifest for one segment.

    Returns the /training-bundle endpoint payload with presigned URLs
    (video_url, depth_url, poses_url — any may be null), plus inline
    camera_info and metadata. Callers reuse a shared httpx client to
    amortize the TCP/TLS handshake across many segments.
    """
    url = (
        f"{_api_url()}{SHOWCASE_PREFIX}/segments/{segment_id}/training-bundle"
    )
    with friendly_http(f"fetching training bundle for {segment_id[:8]}"):
        resp = await client.get(url, headers=_auth_headers(), timeout=30.0)
        resp.raise_for_status()
        return resp.json()
