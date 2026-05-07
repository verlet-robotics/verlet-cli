"""Teleop catalog and file listing fetcher."""
import click
import httpx

from verlet.api_client import AuthenticatedClient, auth_headers_for_profile
from verlet.auth.profiles import ProfileNotFoundError

SHOWCASE_PREFIX = "/api/v1/ego/showcase"


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
                f"{_api_url()}{SHOWCASE_PREFIX}{path}",
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
