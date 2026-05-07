"""httpx wrappers for platform catalog + downloads endpoints.

Anonymous list/info path: bare ``httpx.AsyncClient`` (NO Authorization header).
Authenticated path: ``AuthenticatedClient`` (Bearer + opportunistic refresh).
Download path: ``AuthenticatedClient`` only — anonymous download is rejected
pre-HTTP in commands.py via ``require_profile()``.

KEY INSIGHTS:

- ``--kind teleop`` translates to ``?modality=arm`` (KIND_TO_MODALITY). Backend
  ``ModalityFilter`` is ``Literal["all","arm","ego"]`` — does NOT include
  ``"teleop"``. Sending ``?modality=teleop`` would silently return ALL rows
  (Pitfall 1 in 29-RESEARCH.md). The mapping must be applied in this module
  before any HTTP fires.

- **Step 1 — Modality discrimination on list items / details.** The backend
  ``CatalogDatasetListItem`` and ``CatalogDatasetDetail`` schemas
  (``backend/services/platform_catalog/schema_platform_catalog.py:156, 212``)
  expose ``ego_task_dataset_id: str | None``. Phase 29 prefers this explicit
  discriminator: ``is_ego = item.get("ego_task_dataset_id") is not None``.
  Fallback: arm rows are hardcoded ``["processed"]`` per Phase 27 D-TS1; ego
  rows always carry ``"raw"`` (or both tiers) in ``data_tiers``. The fallback
  catches older payloads / staging shapes where the discriminator may not be
  populated.

- **Step 2 — ``--category`` mechanics.** As of 2026-05-07, ``category`` is NOT
  a Query parameter on ``GET /api/platform/v1/catalog/datasets``
  (verified: ``grep -nE 'category' backend/services/platform_catalog/routes.py``
  returned only QC/episode references at lines 757, 910, 1011, 1023, 1026,
  1028, 1035 — none on ``list_catalog_datasets``). Branch A applies: the CLI
  sends ``?category=<value>`` unconditionally for forward-compat; the backend
  silently ignores it. Phase 31 will add the backend extension at the segment
  level.
"""
from __future__ import annotations

from typing import Any

import httpx

from verlet.api_client import DEFAULT_API_URL, AuthenticatedClient
from verlet.auth.credentials import get_profile
from verlet.auth.profiles import resolve_profile_name


# User-facing ``--kind`` → backend ``?modality=`` mapping. CRITICAL: do NOT
# pass ``?modality=teleop`` to the backend (silent no-op — Pitfall 1).
KIND_TO_MODALITY: dict[str, str] = {"all": "all", "teleop": "arm", "ego": "ego"}


CATALOG_LIST_PATH = "/api/platform/v1/catalog/datasets"
CATALOG_DETAIL_PATH = "/api/platform/v1/catalog/datasets/{slug_or_id}"
ARM_MANIFEST_PATH = "/api/platform/v1/downloads/{slug}/manifest"
EGO_MANIFEST_PATH = "/api/platform/v1/downloads/ego/datasets/{slug}/manifest"


def _api_url_and_headers(profile_name: str | None) -> tuple[str, dict[str, str]]:
    """Resolve api_url + Authorization header for catalog list/info endpoints.

    Anonymous-OK: returns ``(DEFAULT_API_URL, {})`` when no profile is active.
    Never raises ``ProfileNotFoundError`` (use ``AuthenticatedClient`` directly
    for download — that path requires auth and ``require_profile()`` fails fast
    in commands.py before this helper is even called).
    """
    name = resolve_profile_name(profile_name)
    profile = get_profile(name)
    if profile is None:
        # Anonymous path — no Authorization header. Use the api_client's
        # canonical default URL so dev/staging overrides still flow through
        # one constant.
        return (DEFAULT_API_URL, {})
    client = AuthenticatedClient(name)
    try:
        return (client.api_url, client.headers())
    finally:
        client.close()


def is_ego_row(item: dict[str, Any]) -> bool:
    """Detect modality from a CatalogDatasetListItem or Detail.

    Single source of truth — both ``_render.py`` and Plan 03's ``commands.py``
    import this helper rather than duplicating the heuristic.

    Heuristic (see module docstring Step 1 for the full justification):

    1. Prefer the explicit discriminator: ``ego_task_dataset_id is not None``.
    2. Fallback: ``"raw" in (data_tiers or [])`` — arm rows are hardcoded
       ``["processed"]`` per Phase 27 D-TS1; ego rows always carry ``"raw"``
       or both tiers.
    """
    if item.get("ego_task_dataset_id") is not None:
        return True
    return "raw" in (item.get("data_tiers") or [])


def build_list_params(
    *,
    task_type: tuple[str, ...] = (),
    robot_embodiment: tuple[str, ...] = (),
    category: str | None = None,
    since: str | None = None,
    limit: int = 20,
    kind: str = "all",
) -> dict[str, Any]:
    """Build the query dict for ``GET /api/platform/v1/catalog/datasets``.

    Translates user-facing flag names to backend Query param names. Repeatable
    flags (``--task``, ``--robot``) flow through as Python lists; httpx
    encodes them as repeated query params (``?task_type=a&task_type=b``).
    """
    params: dict[str, Any] = {
        "page": 1,
        "page_size": min(limit, 100),
        "modality": KIND_TO_MODALITY[kind],
    }
    if task_type:
        params["task_type"] = list(task_type)
    if robot_embodiment:
        params["robot_embodiment"] = list(robot_embodiment)
    if category:
        # See module docstring Step 2: backend currently ignores this param.
        # TODO Phase 31: backend `category` Query param missing on
        # /catalog/datasets — currently ignored server-side. Wire through for
        # forward-compat; Phase 31 adds backend extension at the segment
        # level.
        params["category"] = category
    if since:
        params["since"] = since
    return params


async def fetch_catalog_list(
    profile_name: str | None, params: dict[str, Any]
) -> dict:
    """Anonymous-OK list fetch — anonymous callers omit the Bearer header."""
    api_url, headers = _api_url_and_headers(profile_name)
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            f"{api_url}{CATALOG_LIST_PATH}", params=params, headers=headers
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_catalog_detail(
    profile_name: str | None, slug_or_id: str
) -> dict:
    """Anonymous-OK detail fetch — slug-primary with full-UUID fallback."""
    api_url, headers = _api_url_and_headers(profile_name)
    path = CATALOG_DETAIL_PATH.format(slug_or_id=slug_or_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{api_url}{path}", headers=headers)
        resp.raise_for_status()
        return resp.json()


async def fetch_arm_manifest(
    profile_name: str,
    slug: str,
    *,
    episode_ids: str | None = None,
    format: str = "lerobot-v2",
) -> tuple[int, dict]:
    """Authenticated. Returns ``(status_code, body)``.

    Phase 30 (CLIDATA-07): the manifest endpoint may return either:

    * **200** + ``DownloadManifest`` — native format, no conversion needed.
    * **202** + ``Manifest202Response{job_id, status, poll_url, …}`` — server
      enqueued a conversion job; the caller polls ``/downloads/jobs/{id}``.

    Returning the status code lets ``commands.py`` branch without a
    second-round-trip introspection on the body shape. ``commands.py``
    calls ``require_profile()`` before invoking this helper so the
    AuthenticatedClient constructor's ``ProfileNotFoundError`` will never
    fire here in practice.
    """
    client = AuthenticatedClient(profile_name)
    try:
        params: dict[str, Any] = {"format": format}
        if episode_ids:
            params["episode_ids"] = episode_ids
        resp = client.request(
            "GET", ARM_MANIFEST_PATH.format(slug=slug), params=params
        )
        # Tolerate both 200 (native) and 202 (conversion enqueued); any other
        # status raises so the caller sees the wire-level error verbatim.
        if resp.status_code not in (200, 202):
            resp.raise_for_status()
        return (resp.status_code, resp.json())
    finally:
        client.close()


async def fetch_job_poll(profile_name: str, job_id: str) -> dict:
    """Authenticated. Single ``GET /downloads/jobs/{job_id}`` round-trip.

    Phase 30 helper exposed for parity with ``fetch_arm_manifest`` — the
    actual polling loop lives in ``verlet.datasets.convert.poll_conversion_job``
    so the Rich progress UX + stderr-on-failure contract stays in one place.
    """
    client = AuthenticatedClient(profile_name)
    try:
        resp = client.request(
            "GET", f"/api/platform/v1/downloads/jobs/{job_id}"
        )
        resp.raise_for_status()
        return resp.json()
    finally:
        client.close()


async def fetch_ego_manifest(
    profile_name: str,
    slug: str,
    *,
    variant: str,                       # "raw" or "processed" — REQUIRED
    episode_ids: str | None = None,
    segment_ids: str | None = None,
) -> dict:
    """Authenticated. ``variant`` is REQUIRED (Phase 27 D-EE4 — no default)."""
    client = AuthenticatedClient(profile_name)
    try:
        params: dict[str, Any] = {"variant": variant}
        if episode_ids:
            params["episode_ids"] = episode_ids
        if segment_ids:
            params["segment_ids"] = segment_ids
        resp = client.request(
            "GET", EGO_MANIFEST_PATH.format(slug=slug), params=params
        )
        resp.raise_for_status()
        return resp.json()
    finally:
        client.close()
