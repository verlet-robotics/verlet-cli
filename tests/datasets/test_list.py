"""CLIDATA-04: verlet datasets list. Real assertions over respx mocks.

Each test mocks the catalog list endpoint and exercises a slice of D-FL/D-MOD
behavior. Assertions verify the request URL parameters that left the CLI
match the user's intent (kind→modality translation, repeatable flags, since
round-trip, page_size clamp, anonymous vs authenticated header dispatch).
"""
from __future__ import annotations

import json

from verlet.auth.credentials import upsert_profile


def _seed_default_profile(_tmp_home, *, kind: str = "device_flow", token: str = "t0k3n") -> None:
    """Write a credentials.json profile under the test's isolated HOME.

    ``tmp_home`` already redirects ``Path.home()``; ``upsert_profile`` walks
    through that and lands the file at ``$HOME/.verlet/credentials.json``.
    """
    upsert_profile(
        "default",
        kind=kind,
        access_token=token,
        api_url="https://api.verlet.co",
    )


def test_kind_teleop_translates_to_modality_arm(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response,
):
    """`--kind teleop` MUST send `?modality=arm` (KIND_TO_MODALITY mapping)."""
    from verlet.cli import cli

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets",
    ).respond(200, json=mock_catalog_list_response)

    result = cli_runner.invoke(cli, ["datasets", "list", "--kind", "teleop"])
    assert result.exit_code == 0, result.output
    last_req = respx_mock.calls.last.request
    assert last_req.url.params.get("modality") == "arm", last_req.url
    # Anonymous path — no profile seeded — must NOT send Authorization.
    assert "Authorization" not in last_req.headers, (
        "anonymous path must not send Bearer"
    )


def test_limit_clamps_at_100_and_prints_footer(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response,
):
    """--limit > 100 → ?page_size=100 AND truncation footer printed."""
    from verlet.cli import cli

    # Construct a response where total > returned so the footer fires.
    # 20 items returned (we copy from the fixture's two-row list and pad), but
    # the total is much larger.
    items = list(mock_catalog_list_response["items"])
    while len(items) < 20:
        items.append(items[0])
    big_resp = {
        "items": items,
        "total": 5000,
        "page": 1,
        "page_size": 100,
    }
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets",
    ).respond(200, json=big_resp)

    result = cli_runner.invoke(cli, ["datasets", "list", "--limit", "200"])
    assert result.exit_code == 0, result.output

    last_req = respx_mock.calls.last.request
    assert last_req.url.params.get("page_size") == "100", (
        f"--limit 200 should clamp to page_size=100, got "
        f"{last_req.url.params.get('page_size')!r}"
    )

    # The truncation footer is emitted by ``list_truncation_footer`` whenever
    # ``returned < total``. The exact wording lives in _render.py and starts
    # with "Showing N of M".
    assert "Showing" in result.output and "5000" in result.output, result.output
    assert "narrow with filters or use --json" in result.output, result.output


def test_since_round_trips_to_backend(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response,
):
    """--since 2026-04-01 reaches backend with `?since=2026-04-01`."""
    from verlet.cli import cli

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets",
    ).respond(200, json=mock_catalog_list_response)

    result = cli_runner.invoke(
        cli, ["datasets", "list", "--since", "2026-04-01"]
    )
    assert result.exit_code == 0, result.output

    last_req = respx_mock.calls.last.request
    assert last_req.url.params.get("since") == "2026-04-01", last_req.url


def test_anonymous_no_authorization(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response,
):
    """No active profile → NO Authorization header sent (D-MOD4)."""
    from verlet.cli import cli

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets",
    ).respond(200, json=mock_catalog_list_response)

    # tmp_home is empty: no credentials.json exists → resolve_profile_name
    # falls through to the literal "default", get_profile returns None,
    # _api_url_and_headers returns (DEFAULT_API_URL, {}).
    result = cli_runner.invoke(cli, ["datasets", "list"])
    assert result.exit_code == 0, result.output

    last_req = respx_mock.calls.last.request
    assert "Authorization" not in last_req.headers, (
        f"anonymous path must not send Bearer, got headers={dict(last_req.headers)}"
    )


def test_authenticated_sends_bearer(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response,
):
    """Active profile → `Authorization: Bearer <token>` header sent."""
    from verlet.cli import cli

    _seed_default_profile(tmp_home, token="abc123")

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets",
    ).respond(200, json=mock_catalog_list_response)

    result = cli_runner.invoke(cli, ["datasets", "list"])
    assert result.exit_code == 0, result.output

    last_req = respx_mock.calls.last.request
    auth = last_req.headers.get("Authorization", "")
    assert auth.startswith("Bearer "), (
        f"expected Bearer header, got Authorization={auth!r}"
    )
    assert auth == "Bearer abc123"


def test_json_output(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response,
):
    """--json emits CatalogDatasetListItem[] verbatim (no client-side reshape)."""
    from verlet.cli import cli

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets",
    ).respond(200, json=mock_catalog_list_response)

    result = cli_runner.invoke(cli, ["datasets", "list", "--json"])
    assert result.exit_code == 0, result.output

    loaded = json.loads(result.output)
    assert isinstance(loaded, list)
    assert len(loaded) == 2
    assert loaded == mock_catalog_list_response["items"]


def test_repeatable_task_flag(
    cli_runner, respx_mock, tmp_home, mock_catalog_list_response,
):
    """`--task pick --task push` → two `task_type=` query repetitions (D-FL3)."""
    from verlet.cli import cli

    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets",
    ).respond(200, json=mock_catalog_list_response)

    result = cli_runner.invoke(
        cli, ["datasets", "list", "--task", "pick", "--task", "push"]
    )
    assert result.exit_code == 0, result.output

    last_req = respx_mock.calls.last.request
    # httpx exposes repeated query params via get_list (or via the raw URL).
    task_values = last_req.url.params.get_list("task_type")
    assert sorted(task_values) == ["pick", "push"], (
        f"expected two task_type repetitions, got {task_values!r}; "
        f"raw URL={last_req.url}"
    )
