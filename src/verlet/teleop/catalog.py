"""Teleop catalog and file listing fetcher."""
import httpx

from verlet.config import get_api_url, get_token

SHOWCASE_PREFIX = "/api/v1/ego/showcase"


def _auth_headers() -> dict[str, str]:
    token = get_token()
    if not token:
        raise RuntimeError("Not authenticated. Run `verlet login` first.")
    return {"Authorization": f"Bearer {token}"}


async def fetch_teleop_catalog() -> dict:
    """Fetch showcase-ready teleop datasets."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{get_api_url()}{SHOWCASE_PREFIX}/teleop/catalog",
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_teleop_dataset(dataset_id: str) -> dict:
    """Fetch dataset detail with episodes and camera names."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{get_api_url()}{SHOWCASE_PREFIX}/teleop/datasets/{dataset_id}",
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_teleop_files(dataset_id: str) -> dict:
    """Fetch all file keys under a dataset's S3 prefix."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{get_api_url()}{SHOWCASE_PREFIX}/teleop/datasets/{dataset_id}/files",
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def presign_teleop_file(dataset_id: str, key: str) -> str:
    """Presign a single file within a dataset's prefix."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{get_api_url()}{SHOWCASE_PREFIX}/teleop/presign-file",
            params={"dataset_id": dataset_id, "key": key},
            headers=_auth_headers(),
        )
        resp.raise_for_status()
        return resp.json()["url"]
