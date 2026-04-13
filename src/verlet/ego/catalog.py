"""Ego catalog fetcher."""
import click
import httpx

from verlet.config import get_api_url, get_token

SHOWCASE_PREFIX = "/api/v1/ego/showcase"

ASSET_TYPES = ("overlay", "rrd", "egodex", "clean")


def _auth_headers() -> dict[str, str]:
    token = get_token()
    if not token:
        raise click.ClickException("Not authenticated. Run `verlet login` first.")
    return {"Authorization": f"Bearer {token}"}


def _raise_http(exc: httpx.HTTPStatusError, context: str) -> None:
    detail = f"HTTP {exc.response.status_code}"
    try:
        body = exc.response.json()
        if isinstance(body, dict) and body.get("detail"):
            detail = body["detail"]
    except Exception:
        pass
    raise click.ClickException(f"{context}: {detail}")


async def fetch_ego_catalog(category: str | None = None) -> dict:
    url = f"{get_api_url()}{SHOWCASE_PREFIX}/catalog"
    params = {}
    if category:
        params["category"] = category

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                url,
                params=params,
                headers=_auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        _raise_http(e, "Failed to fetch ego catalog")
    except httpx.RequestError as e:
        raise click.ClickException(f"Network error fetching ego catalog: {e}")


async def presign_ego_asset(segment_id: str, asset: str = "overlay") -> str:
    url = f"{get_api_url()}{SHOWCASE_PREFIX}/segments/{segment_id}/presign"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                url,
                params={"asset": asset},
                headers=_auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()["url"]
    except httpx.HTTPStatusError as e:
        _raise_http(e, f"Failed to presign {asset} for segment {segment_id[:8]}")
    except httpx.RequestError as e:
        raise click.ClickException(f"Network error presigning asset: {e}")
