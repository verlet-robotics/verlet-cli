"""Httpx wrappers for verlet bundles platform endpoints.

Routes consumed in Plan 30-07:

  * GET /api/platform/v1/catalog/research-bundles  — anonymous browse
    (Phase 23 endpoint; no Authorization header).
    NOTE Pitfall 3 (30-RESEARCH.md): the actual route is
    /catalog/research-bundles. ROADMAP §30 SC5 references
    /catalog/ego/research, which is the Next.js page route, not the API.

Routes consumed in later plans (30-08, 30-09) but not yet wired:

  * GET  /api/platform/v1/bundles                  — unified list (CLIBUNDLE-03)
  * GET  /api/platform/v1/bundles/{id}             — bundle detail (CLIBUNDLE-04)
  * POST /api/platform/v1/bundles/redeem           — Plan 30-03 D-BUNDLE2 redeem
"""
from __future__ import annotations

import os
from typing import Any

import httpx

DEFAULT_BASE = os.environ.get("VERLET_API_URL", "https://api.verlet.co")

CATALOG_RESEARCH_BUNDLES_PATH = "/api/platform/v1/catalog/research-bundles"


async def fetch_bundles_browse(*, limit: int = 50) -> dict[str, Any]:
    """Anonymous public bundle catalog (CLIBUNDLE-01).

    No Authorization header. Returns the deserialized JSON body
    ``{"items": [{slug, name, dataset_count, license, citation, ...}, ...]}``.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{DEFAULT_BASE}{CATALOG_RESEARCH_BUNDLES_PATH}",
            params={"limit": limit},
        )
        resp.raise_for_status()
        return resp.json()
