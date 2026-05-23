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

from verlet._http_errors import friendly_http
from verlet.api_client import DEFAULT_API_URL, AuthenticatedClient
from verlet.auth.credentials import get_profile
from verlet.auth.profiles import resolve_profile_name


# User-facing ``--kind`` → backend ``?modality=`` mapping. CRITICAL: do NOT
# pass ``?modality=teleop`` to the backend (silent no-op — Pitfall 1).
KIND_TO_MODALITY: dict[str, str] = {"all": "all", "teleop": "arm", "ego": "ego"}


CATALOG_LIST_PATH = "/api/platform/v1/catalog/datasets"
CATALOG_DETAIL_PATH = "/api/platform/v1/catalog/datasets/{slug_or_id}"
CATALOG_EPISODES_PATH = "/api/platform/v1/catalog/datasets/{slug_or_id}/episodes"
CATALOG_SEGMENTS_PATH = "/api/platform/v1/catalog/datasets/{slug_or_id}/segments"
CATALOG_QC_DISTRIBUTIONS_PATH = (
    "/api/platform/v1/catalog/datasets/{slug_or_id}/qc-distributions"
)
CATALOG_ANALYTICS_PATH = (
    "/api/platform/v1/catalog/datasets/{slug_or_id}/analytics"
)
ARM_MANIFEST_PATH = "/api/platform/v1/downloads/{slug}/manifest"
EGO_MANIFEST_PATH = "/api/platform/v1/downloads/ego/datasets/{slug}/manifest"
LIBRARY_PATH = "/api/platform/v1/downloads/library"

# Gated showcase endpoints. Showcase access codes (``kind=showcase_access_code``)
# route to these instead of the platform catalog: every row and every download
# is filtered against the access code's grants.
SHOWCASE_LIST_PATH = "/api/v1/showcase/datasets"
SHOWCASE_DETAIL_PATH = "/api/v1/showcase/datasets/{slug_or_id}"
SHOWCASE_DOWNLOAD_PATH = "/api/v1/showcase/datasets/{slug_or_id}/download"


def _api_url_and_headers(profile_name: str | None) -> tuple[str, dict[str, str]]:
    """Resolve api_url + Authorization header for catalog list/info endpoints.

    Anonymous-OK: returns ``(DEFAULT_API_URL, {})`` when no profile is active.
    Never raises ``ProfileNotFoundError`` (use ``AuthenticatedClient`` directly
    for download — that path requires auth and ``require_profile()`` fails fast
    in commands.py before this helper is even called).
    """
    from verlet.telemetry import current_user_agent

    name = resolve_profile_name(profile_name)
    profile = get_profile(name)
    if profile is None:
        # Anonymous path — no Authorization header. Use the api_client's
        # canonical default URL so dev/staging overrides still flow through
        # one constant. We DO send the User-Agent on every request per
        # D-DIST1; default is bare ``verlet-cli/<v>`` until the user opts
        # in via ``verlet config telemetry enable``.
        return (DEFAULT_API_URL, {"User-Agent": current_user_agent()})
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
    with friendly_http("listing datasets"):
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
    with friendly_http(f"fetching dataset '{slug_or_id}'"):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{api_url}{path}", headers=headers)
            resp.raise_for_status()
            return resp.json()


async def _fetch_catalog_anon(
    profile_name: str | None, path: str, context: str, params: dict | None = None
) -> dict:
    """Anonymous-OK GET against a catalog sub-resource.

    Shared by the episode/segment browse + QC/analytics inspect commands —
    all hang off ``/catalog/datasets/{slug}/…`` and accept optional auth.
    """
    api_url, headers = _api_url_and_headers(profile_name)
    with friendly_http(context):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{api_url}{path}", params=params or {}, headers=headers
            )
            resp.raise_for_status()
            return resp.json()


async def fetch_dataset_episodes(
    profile_name: str | None,
    slug_or_id: str,
    *,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """Anonymous-OK. Paginated episode listing for a dataset (G-P7)."""
    return await _fetch_catalog_anon(
        profile_name,
        CATALOG_EPISODES_PATH.format(slug_or_id=slug_or_id),
        f"listing episodes for '{slug_or_id}'",
        {"page": page, "page_size": page_size},
    )


async def fetch_dataset_segments(
    profile_name: str | None,
    slug_or_id: str,
    *,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """Anonymous-OK. Paginated segment listing for an ego dataset (G-P7)."""
    return await _fetch_catalog_anon(
        profile_name,
        CATALOG_SEGMENTS_PATH.format(slug_or_id=slug_or_id),
        f"listing segments for '{slug_or_id}'",
        {"page": page, "page_size": page_size},
    )


async def fetch_dataset_qc_distributions(
    profile_name: str | None, slug_or_id: str
) -> dict:
    """Anonymous-OK. QC-metric distributions for a dataset (G-P6)."""
    return await _fetch_catalog_anon(
        profile_name,
        CATALOG_QC_DISTRIBUTIONS_PATH.format(slug_or_id=slug_or_id),
        f"fetching QC distributions for '{slug_or_id}'",
    )


async def fetch_dataset_analytics(
    profile_name: str | None, slug_or_id: str
) -> dict:
    """Anonymous-OK. Aggregate analytics for a dataset (G-P6)."""
    return await _fetch_catalog_anon(
        profile_name,
        CATALOG_ANALYTICS_PATH.format(slug_or_id=slug_or_id),
        f"fetching analytics for '{slug_or_id}'",
    )


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
        with friendly_http(f"fetching manifest for dataset '{slug}'"):
            resp = client.request(
                "GET", ARM_MANIFEST_PATH.format(slug=slug), params=params
            )
            # Tolerate both 200 (native) and 202 (conversion enqueued); any
            # other status raises so the caller sees the wire-level error.
            if resp.status_code not in (200, 202):
                resp.raise_for_status()
            return (resp.status_code, resp.json())
    finally:
        client.close()


CONVERSIONS_PATH = "/api/platform/v1/downloads/{slug}/conversions"
JOBS_LIST_PATH = "/api/platform/v1/downloads/jobs"


async def fetch_dataset_conversions(
    profile_name: str | None, slug: str
) -> list[dict]:
    """Authenticated. List conversion jobs for one dataset (G-P5)."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http(f"listing conversions for '{slug}'"):
            resp = client.get(CONVERSIONS_PATH.format(slug=slug))
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def fetch_all_jobs(profile_name: str | None) -> list[dict]:
    """Authenticated. List every conversion job the account has triggered (G-P5)."""
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("listing conversion jobs"):
            resp = client.get(JOBS_LIST_PATH)
            resp.raise_for_status()
            return resp.json()
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
        with friendly_http(f"polling conversion job {job_id}"):
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
        with friendly_http(f"fetching ego manifest for dataset '{slug}'"):
            resp = client.request(
                "GET", EGO_MANIFEST_PATH.format(slug=slug), params=params
            )
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def fetch_library(profile_name: str | None) -> dict:
    """Authenticated. List the caller's purchased datasets + bundles (G-P1).

    Returns the raw ``LibraryListResponse`` body
    ``{datasets: [...], count: N, bundles: [...]}``. ``commands`` calls
    ``require_profile()`` before this helper, so the AuthenticatedClient
    constructor's ``ProfileNotFoundError`` never fires here in practice.
    """
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("listing your library"):
            resp = client.request("GET", LIBRARY_PATH)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Credential-kind dispatch — showcase access codes route to the gated
# ``/api/v1/showcase/datasets/*`` surface instead of the platform catalog.
# ---------------------------------------------------------------------------


def resolve_credential_kind(profile_name: str | None) -> str | None:
    """Return the active profile's ``kind`` (e.g. ``showcase_access_code``,
    ``device_flow``, ``pat``), or ``None`` when no profile is configured."""
    name = resolve_profile_name(profile_name)
    profile = get_profile(name)
    return None if profile is None else profile.get("kind")


def normalize_showcase_list_item(item: dict[str, Any]) -> dict[str, Any]:
    """Project a ``/showcase/datasets`` row onto the shape ``_render.py``'s
    ``dataset_list_table`` expects.

    Sets ``ego_task_dataset_id`` for ego rows so ``is_ego_row`` reports the
    right modality; the showcase endpoint exposes ``modality`` /
    ``variants_available`` instead of the platform's discriminator fields.
    """
    is_ego = item.get("modality") == "ego"
    variants = item.get("variants_available") or []
    return {
        "slug": item["slug"],
        "title": item.get("title"),
        "task_type": item.get("task_type"),
        "robot_embodiment": item.get("robot_embodiment"),
        "episode_count": item.get("segment_count")
        if is_ego and item.get("segment_count") is not None
        else item.get("episode_count"),
        "total_hours": item.get("total_hours"),
        "total_bytes": None,  # not exposed by the showcase endpoint
        "available_variants": variants,
        "data_tiers": variants,
        "ego_task_dataset_id": item["id"] if is_ego else None,
    }


async def fetch_showcase_list(profile_name: str | None) -> dict:
    """Authenticated. Lists the datasets the showcase access code is granted.

    Returns the raw ``{datasets: [...], total: N}`` body. The showcase
    endpoint takes no filter params — task/robot filtering is applied
    client-side by the caller.
    """
    client = AuthenticatedClient(profile_name)
    try:
        with friendly_http("listing datasets"):
            resp = client.request("GET", SHOWCASE_LIST_PATH)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def fetch_showcase_detail(profile_name: str | None, slug_or_id: str) -> dict:
    """Authenticated. Showcase dataset detail incl. ``effective_grants``.

    404 means the dataset does not exist OR the access code has no grant for
    it — the backend conflates these to prevent enumeration.
    """
    client = AuthenticatedClient(profile_name)
    try:
        path = SHOWCASE_DETAIL_PATH.format(slug_or_id=slug_or_id)
        with friendly_http(f"fetching dataset '{slug_or_id}'"):
            resp = client.request("GET", path)
            if resp.status_code == 404:
                raise _showcase_not_found(slug_or_id)
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


async def fetch_showcase_download(
    profile_name: str | None,
    slug: str,
    *,
    variant: str | None = None,
    scope: str = "full",
) -> dict:
    """Authenticated. Gated showcase download manifest for one dataset.

    The backend derives variant/scope from the grant; ``variant`` is sent
    only when explicitly provided so it can select among multiple grants.
    404 = no grant / no dataset; 429 = quota exhausted or rate-limited.
    """
    import click

    client = AuthenticatedClient(profile_name)
    try:
        params: dict[str, Any] = {"scope": scope}
        if variant is not None:
            params["variant"] = variant
        path = SHOWCASE_DOWNLOAD_PATH.format(slug_or_id=slug)
        with friendly_http(f"fetching download manifest for '{slug}'"):
            resp = client.request("GET", path, params=params)
            if resp.status_code == 404:
                raise _showcase_not_found(slug)
            if resp.status_code == 429:
                raise click.ClickException(
                    "Rate-limited or quota exhausted for this grant. "
                    "Try again later or contact your Verlet rep."
                )
            resp.raise_for_status()
            return resp.json()
    finally:
        client.close()


def _showcase_not_found(slug: str) -> Exception:
    import click

    return click.ClickException(
        f"No access to dataset '{slug}'. Either it does not exist, or your "
        "access code has no grant for it. Contact your Verlet rep to request "
        "access."
    )
