"""Teleop catalog and file listing fetcher."""
import click
import httpx

from verlet.config import get_api_url, get_token

SHOWCASE_PREFIX = "/api/v1/ego/showcase"


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


async def _get(path: str, context: str, params: dict | None = None) -> dict:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{get_api_url()}{SHOWCASE_PREFIX}{path}",
                params=params or {},
                headers=_auth_headers(),
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        _raise_http(e, context)
    except httpx.RequestError as e:
        raise click.ClickException(f"Network error ({context}): {e}")


async def fetch_teleop_catalog() -> dict:
    """Fetch showcase-ready teleop datasets."""
    return await _get("/teleop/catalog", "Failed to fetch teleop catalog")


async def fetch_teleop_dataset(dataset_id: str) -> dict:
    """Fetch dataset detail with episodes and camera names."""
    return await _get(
        f"/teleop/datasets/{dataset_id}",
        f"Failed to fetch teleop dataset {dataset_id[:8]}",
    )


async def fetch_teleop_files(dataset_id: str) -> dict:
    """Fetch all file keys under a dataset's S3 prefix."""
    return await _get(
        f"/teleop/datasets/{dataset_id}/files",
        f"Failed to list files for dataset {dataset_id[:8]}",
    )


async def presign_teleop_file(dataset_id: str, key: str) -> str:
    """Presign a single file within a dataset's prefix."""
    data = await _get(
        "/teleop/presign-file",
        f"Failed to presign {key}",
        params={"dataset_id": dataset_id, "key": key},
    )
    return data["url"]
