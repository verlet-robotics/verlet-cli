"""Httpx wrappers for the gated showcase surface (G-S3)."""
from __future__ import annotations

from verlet._http_errors import friendly_http
from verlet.api_client import AuthenticatedClient

OPERATION_STATS_PATH = "/api/v1/showcase/operation-stats"


async def fetch_operation_stats(profile_name: str | None) -> dict:
    """Authenticated. Fleet-aggregate operation stats for a showcase code.

    Returns the raw ``OperationStatsResponse`` body. Requires a showcase
    access-code profile — ``commands.stats`` enforces the credential kind
    before this is called.
    """
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("fetching fleet operation stats"):
            resp = client.get(OPERATION_STATS_PATH)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()
