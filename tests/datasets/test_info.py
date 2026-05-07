"""CLIDATA-05: verlet datasets info. Real assertions over respx mocks."""
from __future__ import annotations

import json


def _arm_detail(slug: str = "pick-and-place-yam-v3") -> dict:
    """Canonical arm detail payload — `data_tiers=["processed"]` per Phase 27 D-TS1."""
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "slug": slug,
        "title": "Pick and Place YAM v3",
        "task_type": "pick-and-place",
        "robot_embodiment": "yam",
        "episode_count": 120,
        "total_hours": 8.5,
        "total_bytes": 1_250_000_000,
        "available_variants": ["processed"],
        "data_tiers": ["processed"],
        "license_tier": "research",
        "price_per_hour_cents": 1500,
        "currency": "USD",
        "episodes": [],
    }


def test_info_by_slug(cli_runner, respx_mock, tmp_home):
    """`verlet datasets info pick-and-place-yam-v3` resolves slug-primary."""
    from verlet.cli import cli

    detail = _arm_detail("pick-and-place-yam-v3")
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=detail)

    result = cli_runner.invoke(cli, ["datasets", "info", "pick-and-place-yam-v3"])
    assert result.exit_code == 0, result.output
    assert "pick-and-place-yam-v3" in result.output
    # Modality column reads "teleop" for arm rows (D-MOD1).
    assert "teleop" in result.output, result.output


def test_info_by_uuid(cli_runner, respx_mock, tmp_home):
    """`verlet datasets info <uuid>` works (UUID fallback per D-MOD3)."""
    from verlet.cli import cli

    uuid = "00000000-0000-0000-0000-000000000001"
    detail = _arm_detail("pick-and-place-yam-v3")
    respx_mock.get(
        f"https://api.verlet.co/api/platform/v1/catalog/datasets/{uuid}",
    ).respond(200, json=detail)

    result = cli_runner.invoke(cli, ["datasets", "info", uuid])
    assert result.exit_code == 0, result.output
    # Title is in stdout — the slug doubles as identity but the title is the
    # human-readable label that surfaces in the metadata table.
    assert "Pick and Place YAM v3" in result.output


def test_info_json_output(cli_runner, respx_mock, tmp_home):
    """--json emits CatalogDatasetDetail payload directly."""
    from verlet.cli import cli

    detail = {
        "slug": "foo",
        "title": "Foo",
        "data_tiers": ["processed"],
        "available_variants": ["processed"],
    }
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/foo",
    ).respond(200, json=detail)

    result = cli_runner.invoke(cli, ["datasets", "info", "foo", "--json"])
    assert result.exit_code == 0, result.output

    loaded = json.loads(result.output)
    assert loaded == detail


def test_info_anonymous_public(cli_runner, respx_mock, tmp_home):
    """Anonymous works for public rows (D-MOD4)."""
    from verlet.cli import cli

    detail = _arm_detail("pick-and-place-yam-v3")
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/pick-and-place-yam-v3",
    ).respond(200, json=detail)

    # tmp_home has no credentials.json → anonymous path.
    result = cli_runner.invoke(cli, ["datasets", "info", "pick-and-place-yam-v3"])
    assert result.exit_code == 0, result.output

    last_req = respx_mock.calls.last.request
    assert "Authorization" not in last_req.headers, (
        f"anonymous info must not send Bearer, got headers={dict(last_req.headers)}"
    )
