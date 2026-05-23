"""Httpx wrappers for the cloud-destinations API (G-P4).

Base path: ``/api/platform/v1/downloads/destinations``.

``fetch_providers`` is anonymous — the provider list is public. Every other
wrapper is authenticated via ``AuthenticatedClient``. All wrappers are async
for parity with the rest of the verlet ``_api`` modules; ``commands.py`` drives
them through ``asyncio.run``. HTTP errors surface via ``friendly_http`` (FastAPI
``{detail}`` rendered cleanly, no traceback).
"""
from __future__ import annotations

from typing import Any

import httpx

from verlet._http_errors import friendly_http
from verlet.api_client import DEFAULT_API_URL, AuthenticatedClient

_BASE = "/api/platform/v1/downloads/destinations"
PROVIDERS_PATH = f"{_BASE}/providers"
CONNECT_INIT_PATH = f"{_BASE}/connect/init"
CONNECT_CALLBACK_PATH = f"{_BASE}/connect/callback"
TEST_CONNECTION_PATH = f"{_BASE}/test-connection"


async def fetch_providers() -> list[dict[str, Any]]:
    """Anonymous. GET /destinations/providers — the public provider list."""
    from verlet.telemetry import current_user_agent

    with friendly_http("listing destination providers"):
        async with httpx.AsyncClient(
            timeout=30.0, headers={"User-Agent": current_user_agent()}
        ) as client:
            resp = await client.get(f"{DEFAULT_API_URL}{PROVIDERS_PATH}")
            resp.raise_for_status()
            return resp.json()


async def fetch_destinations(profile_name: str | None) -> list[dict[str, Any]]:
    """Authenticated. GET /destinations — the caller's saved destinations."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("listing destinations"):
            resp = client.get(_BASE)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def create_destination(profile_name: str | None, body: dict) -> dict:
    """Authenticated. POST /destinations — create with manual credentials."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("creating destination"):
            resp = client.post(_BASE, json=body)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def connect_init(profile_name: str | None, body: dict) -> dict:
    """Authenticated. POST /destinations/connect/init — begin a connect flow."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("starting destination connect"):
            resp = client.post(CONNECT_INIT_PATH, json=body)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def connect_callback(profile_name: str | None, body: dict) -> dict:
    """Authenticated. POST /destinations/connect/callback — finish a flow."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("completing destination connect"):
            resp = client.post(CONNECT_CALLBACK_PATH, json=body)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def test_connection(profile_name: str | None, body: dict) -> dict:
    """Authenticated. POST /destinations/test-connection — verify creds."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("testing destination connection"):
            resp = client.post(TEST_CONNECTION_PATH, json=body)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def delete_destination(profile_name: str | None, dest_id: str) -> None:
    """Authenticated. DELETE /destinations/{id} — idempotent (204)."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("deleting destination"):
            resp = client.delete(f"{_BASE}/{dest_id}")
            resp.raise_for_status()
    finally:
        client.close()
