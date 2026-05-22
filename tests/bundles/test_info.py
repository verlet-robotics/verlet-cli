"""Plan 30-08 Task 2 — `verlet bundles info <id>` (CLIBUNDLE-04).

Five behavior tests:

  * test_info_calls_authenticated_detail_endpoint — GETs
    /api/platform/v1/bundles/<id> with the Bearer header from the active
    profile and renders a Rich panel + dataset table.
  * test_info_json_outputs_valid_json — `--json` emits the bundle detail
    document on stdout.
  * test_info_404_prints_bundle_not_found — server 404 -> stderr "bundle
    not found" verbatim + exit 1 (Plan 30-03 D-S5 masks unauthorized
    callers as 404 to avoid bundle-id enumeration).
  * test_info_research_kind_shows_citation — research-kind bundles render
    the citation row; purchased-kind bundles do NOT.
  * test_info_dataset_formats_inline — each dataset's `available_formats`
    list is rendered inline (comma-separated) in the datasets table.
  * test_info_401_prints_verbatim_auth_error — 401 -> stderr verbatim auth
    error + exit 1 (mirrors `bundles list` for parity).
"""
from __future__ import annotations

import json

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.bundles._render import bundle_detail_view
from verlet.cli import cli

from tests.conftest import combined_output


BUNDLE_DETAIL_PATH_FMT = "/api/platform/v1/bundles/{bundle_id}"

FAR_FUTURE = "2099-01-01T00:00:00+00:00"


def _seed_default_profile() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at=FAR_FUTURE,
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _research_detail(bundle_id: str = "stanford-egocentric-2024") -> dict:
    return {
        "bundle_id": bundle_id,
        "bundle_slug": "stanford-egocentric-2024",
        "bundle_name": "Stanford Egocentric 2024",
        "kind": "research",
        "expires_at": FAR_FUTURE,
        "license": "CC-BY-4.0",
        "citation": "Lee et al., 2024 — Stanford Egocentric Robotics Lab",
        "datasets": [
            {
                "slug": "kitchen-pickplace",
                "name": "Kitchen pick-place",
                "episode_count": 50,
                "available_formats": ["lerobot-v2", "hdf5"],
                "size_bytes": 2_500_000_000,
            },
            {
                "slug": "tabletop-stack",
                "name": "Tabletop stacking",
                "episode_count": 30,
                "available_formats": ["lerobot-v2"],
                "size_bytes": 1_200_000_000,
            },
        ],
    }


def _purchased_detail(bundle_id: str = "acme-corp-bundle") -> dict:
    return {
        "bundle_id": bundle_id,
        "bundle_slug": "acme-corp-bundle",
        "bundle_name": "Acme Corp Bundle",
        "kind": "purchased",
        "expires_at": FAR_FUTURE,
        "license": "commercial",
        "citation": None,
        "datasets": [
            {
                "slug": "industrial-pick",
                "name": "Industrial pick",
                "episode_count": 100,
                "available_formats": ["lerobot-v2"],
                "size_bytes": 4_000_000_000,
            }
        ],
    }


def test_info_calls_authenticated_detail_endpoint(
    tmp_home, cli_runner, respx_mock,
):
    """`verlet bundles info <id>` GETs /bundles/<id> with Authorization."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    route = respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_research_detail(bundle_id)))

    result = cli_runner.invoke(cli, ["bundles", "info", bundle_id])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called

    request = route.calls.last.request
    assert request.headers.get("Authorization") == "Bearer jwt.access.value", (
        f"expected Bearer header; got {request.headers.get('Authorization')!r}"
    )

    # Rendered output mentions the slug, name, license, datasets.
    assert "stanford-egocentric-2024" in result.output
    assert "Stanford Egocentric 2024" in result.output
    assert "kitchen-pickplace" in result.output


def test_info_json_outputs_valid_json(tmp_home, cli_runner, respx_mock):
    """`--json` writes the bundle detail document to stdout."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_research_detail(bundle_id)))

    result = cli_runner.invoke(cli, ["bundles", "info", bundle_id, "--json"])
    assert result.exit_code == 0, (result.output, result.stderr)

    parsed = json.loads(result.output)
    assert parsed["bundle_slug"] == "stanford-egocentric-2024"
    assert parsed["kind"] == "research"
    assert len(parsed["datasets"]) == 2


def test_info_404_prints_bundle_not_found(tmp_home, cli_runner, respx_mock):
    """Server 404 -> stderr 'bundle not found' verbatim + exit 1."""
    _seed_default_profile()
    bundle_id = "nonexistent-bundle"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(404, json={"detail": "Not Found"}))

    result = cli_runner.invoke(cli, ["bundles", "info", bundle_id])
    assert result.exit_code != 0, (result.output, result.stderr)
    # Verbatim string -- byte-asserted.
    assert "bundle not found" in combined_output(result)


def test_info_401_prints_verbatim_auth_error(tmp_home, cli_runner, respx_mock):
    """401 -> stderr 'not authenticated; run verlet auth login' verbatim."""
    _seed_default_profile()
    bundle_id = "any-bundle"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(401, json={"detail": "Token expired"}))

    result = cli_runner.invoke(cli, ["bundles", "info", bundle_id])
    assert result.exit_code != 0, (result.output, result.stderr)
    assert "not authenticated; run verlet auth login" in combined_output(result)


def test_info_research_kind_shows_citation(tmp_home, cli_runner, respx_mock):
    """research-kind bundle renders the citation row; purchased-kind does NOT.

    Asserts via the renderer directly so the conditional is exercised at
    the contract surface rather than via brittle ANSI-output sniffing.
    """
    research = _research_detail()
    purchased = _purchased_detail()

    # Research path -- citation renders.
    from io import StringIO

    from rich.console import Console

    research_buf = StringIO()
    Console(file=research_buf, force_terminal=False, width=120, no_color=True).print(
        bundle_detail_view(research)
    )
    research_text = research_buf.getvalue()
    assert "citation:" in research_text, research_text
    assert "Lee et al., 2024" in research_text, research_text

    # Purchased path -- citation row absent (the field is None or kind != research).
    purchased_buf = StringIO()
    Console(file=purchased_buf, force_terminal=False, width=120, no_color=True).print(
        bundle_detail_view(purchased)
    )
    purchased_text = purchased_buf.getvalue()
    assert "citation:" not in purchased_text, (
        "purchased bundles must NOT show citation row; got:\n" + purchased_text
    )


def test_info_dataset_formats_inline(tmp_home, cli_runner, respx_mock):
    """Each dataset row's `available_formats` renders inline as a CSV."""
    _seed_default_profile()
    bundle_id = "stanford-egocentric-2024"

    respx_mock.get(
        f"https://api.verlet.co{BUNDLE_DETAIL_PATH_FMT.format(bundle_id=bundle_id)}"
    ).mock(return_value=httpx.Response(200, json=_research_detail(bundle_id)))

    result = cli_runner.invoke(cli, ["bundles", "info", bundle_id])
    assert result.exit_code == 0, (result.output, result.stderr)

    # First dataset has both formats; second has only lerobot-v2. Both surface
    # in the rendered output.
    assert "lerobot-v2" in result.output
    assert "hdf5" in result.output
