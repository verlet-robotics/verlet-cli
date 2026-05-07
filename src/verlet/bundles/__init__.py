"""Plan 30-07: verlet bundles command group (CLIBUNDLE-01..07).

Subpackage layout (mirrors verlet/datasets/ — Phase 29 pattern):

  * commands.py — Click group + browse / redeem / list / info / download
    subcommands. Plan 30-07 ships `browse` + `redeem`; later plans (30-08,
    30-09) extend the same group.
  * _api.py     — httpx wrappers for the platform /catalog/research-bundles
    + /api/platform/v1/bundles endpoints.
  * _render.py  — Rich table builders.
  * _validation.py — verbatim error-string constants byte-asserted in tests
    (Phase 31 pattern); fail-fast flag validators.
"""
from verlet.bundles.commands import bundles_group

__all__ = ["bundles_group"]
