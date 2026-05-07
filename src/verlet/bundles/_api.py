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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from verlet.api_client import AuthenticatedClient

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

REDEEM_PATH = "/api/platform/v1/bundles/redeem"


class RedeemError(Exception):
    """Raised by :func:`redeem_bundle_code` for user-recoverable redeem failures.

    ``status_code`` holds the HTTP status (``404``, ``410``, etc.). ``detail``
    is the server-supplied error string suitable for stderr surface. The
    caller (``bundles.commands.redeem``) translates this into a click-friendly
    error + non-zero exit; we keep _api decoupled from Click so the layer
    stays test-isolatable.
    """

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(f"redeem failed [{status_code}]: {detail}")
        self.status_code = status_code
        self.detail = detail


async def redeem_bundle_code(
    code: str, *, email: str | None = None
) -> dict[str, Any]:
    """POST /api/platform/v1/bundles/redeem (CLIBUNDLE-02, D-BUNDLE2 idempotent).

    Returns the deserialized JSON response on 200:

        {
            "access_token": "...",
            "expires_at": "...",
            "bundle_slug": "...",
            "kind": "bundle_grant"
        }

    Raises :class:`RedeemError` with the server detail on 404 (unknown code)
    and 410 (revoked / expired). Other failures bubble up via
    ``raise_for_status()``.
    """
    body: dict[str, Any] = {"code": code}
    if email:
        body["email"] = email
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{DEFAULT_BASE}{REDEEM_PATH}", json=body)
        if resp.status_code in (404, 410):
            try:
                detail = resp.json().get("detail") or "redeem failed"
            except Exception:
                detail = resp.text or "redeem failed"
            raise RedeemError(resp.status_code, detail)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Plan 30-08 — authenticated bundle list / detail wrappers.
#
# Both routes 401 when the bearer is missing/expired. We surface the verbatim
# string ``"not authenticated; run verlet auth login"`` on stderr and exit 1
# directly from this module to keep the error path byte-stable. Other callers
# (commands.py) catch SystemExit only by re-raising; they never paper over.
#
# `AuthenticatedClient` is sync (httpx.Client under the hood). We keep these
# wrappers `async` for parity with the rest of bundles/_api.py + the broader
# verlet codebase pattern: commands.py calls ``asyncio.run(fetch_*(...))`` so
# the entry point is uniform whether the inner work is real async (browse,
# redeem) or sync-wrapped-in-async (list, detail).
# ---------------------------------------------------------------------------


BUNDLES_LIST_PATH = "/api/platform/v1/bundles"

NOT_AUTHENTICATED_MSG = "not authenticated; run verlet auth login"
"""Verbatim 401 stderr line for `bundles list` / `bundles info`.

Byte-asserted in ``tests/bundles/test_list.py::test_list_401_prints_verbatim_auth_error``
and ``tests/bundles/test_info.py::test_info_401_prints_verbatim_auth_error``.
Editing the message intentionally requires updating both tests.
"""

def _exit_with_stderr(msg: str) -> "Any":
    """Print ``msg`` to stderr and raise ``SystemExit(1)``.

    Local helper -- click is imported inside the body so _api.py stays
    free of the click-decoupling pattern from Plan 30-07 D-S1 (RedeemError).
    For terminal exits we accept a click import here because the surface is
    already terminal (no caller re-entry expected).
    """
    import click

    click.echo(msg, err=True)
    raise SystemExit(1)


async def fetch_bundles_list(
    client: "AuthenticatedClient", *, include_inactive: bool = False
) -> dict[str, Any]:
    """GET /api/platform/v1/bundles — authenticated list (CLIBUNDLE-03).

    ``include_inactive=True`` adds ``?include_inactive=true`` and surfaces
    expired/revoked rows (D-BUNDLE1). 401 -> stderr verbatim auth error +
    exit 1; everything else falls through ``raise_for_status``.
    """
    params = {"include_inactive": "true"} if include_inactive else None
    resp = client.get(BUNDLES_LIST_PATH, params=params)
    if resp.status_code == 401:
        _exit_with_stderr(NOT_AUTHENTICATED_MSG)
    resp.raise_for_status()
    return resp.json()
