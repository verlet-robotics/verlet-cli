"""Ego catalog fetcher."""
import httpx

from verlet.config import get_api_url, get_token

SHOWCASE_PREFIX = "/api/v1/ego/showcase"


async def fetch_ego_catalog(category: str | None = None) -> dict:
    token = get_token()
    if not token:
        raise RuntimeError("Not authenticated. Run `verlet login` first.")

    url = f"{get_api_url()}{SHOWCASE_PREFIX}/catalog"
    params = {}
    if category:
        params["category"] = category

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        return resp.json()


async def presign_ego_asset(segment_id: str, asset: str = "overlay") -> str:
    token = get_token()
    if not token:
        raise RuntimeError("Not authenticated. Run `verlet login` first.")

    url = f"{get_api_url()}{SHOWCASE_PREFIX}/segments/{segment_id}/presign"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            url,
            params={"asset": asset},
            headers={"Authorization": f"Bearer {token}"},
        )
        resp.raise_for_status()
        return resp.json()["url"]
