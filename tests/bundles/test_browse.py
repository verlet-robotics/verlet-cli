"""Plan 30-07 Task 1 — `verlet bundles browse` (CLIBUNDLE-01).

Five behavior tests:

  * test_browse_calls_research_bundles_path_anonymously — anonymous GET to
    /api/platform/v1/catalog/research-bundles, NO Authorization header.
  * test_browse_json_emits_valid_json — `--json` outputs a parseable JSON
    document of the items list.
  * test_browse_limit_query_param — `--limit 5` adds `?limit=5`.
  * test_bundles_help_lists_browse — `verlet bundles --help` shows `browse`.
  * test_browse_500_exits_nonzero_with_stderr — server 500 → exit 1, stderr
    carries the failure detail.
"""
from __future__ import annotations

import json

import httpx

from verlet.cli import cli

from tests.conftest import combined_output


SAMPLE_BUNDLES = {
    "items": [
        {
            "slug": "stanford-egocentric-2024",
            "name": "Stanford Egocentric 2024",
            "description": "Multi-task ego dataset",
            "dataset_count": 12,
            "license": "CC-BY-4.0",
            "citation": "Lee et al., 2024",
        },
        {
            "slug": "mit-pickplace",
            "name": "MIT PickPlace",
            "description": "Bimanual pick-and-place",
            "dataset_count": 4,
            "license": "research-only",
            "citation": "Smith et al., 2023",
        },
    ]
}


def test_browse_calls_research_bundles_path_anonymously(
    tmp_home, cli_runner, respx_mock
):
    """`verlet bundles browse` GETs /catalog/research-bundles with NO Authorization header."""
    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/research-bundles"
    ).mock(return_value=httpx.Response(200, json=SAMPLE_BUNDLES))

    result = cli_runner.invoke(cli, ["bundles", "browse"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called

    # Anonymous: no Authorization header on the request.
    request = route.calls.last.request
    assert "Authorization" not in request.headers, (
        f"browse must be anonymous; got Authorization={request.headers.get('Authorization')!r}"
    )

    # Output mentions one of the slugs.
    assert "stanford-egocentric-2024" in result.output


def test_browse_json_emits_valid_json(tmp_home, cli_runner, respx_mock):
    """`verlet bundles browse --json` writes parseable JSON to stdout."""
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/research-bundles"
    ).mock(return_value=httpx.Response(200, json=SAMPLE_BUNDLES))

    result = cli_runner.invoke(cli, ["bundles", "browse", "--json"])
    assert result.exit_code == 0, (result.output, result.stderr)

    parsed = json.loads(result.output)
    assert isinstance(parsed, list)
    assert len(parsed) == 2
    assert parsed[0]["slug"] == "stanford-egocentric-2024"
    assert parsed[0]["dataset_count"] == 12


def test_browse_limit_query_param(tmp_home, cli_runner, respx_mock):
    """`--limit 5` adds `?limit=5` to the GET."""
    route = respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/research-bundles"
    ).mock(return_value=httpx.Response(200, json={"items": []}))

    result = cli_runner.invoke(cli, ["bundles", "browse", "--limit", "5"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called

    qs = dict(route.calls.last.request.url.params)
    assert qs.get("limit") == "5", f"expected limit=5, got params={qs}"


def test_bundles_help_lists_browse(cli_runner):
    """`verlet bundles --help` enumerates the `browse` subcommand."""
    result = cli_runner.invoke(cli, ["bundles", "--help"])
    assert result.exit_code == 0, result.output
    assert "browse" in result.output


def test_browse_500_exits_nonzero_with_stderr(tmp_home, cli_runner, respx_mock):
    """Server 500 → CLI exits non-zero and prints a stderr error."""
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/research-bundles"
    ).mock(return_value=httpx.Response(500, text="upstream blew up"))

    result = cli_runner.invoke(cli, ["bundles", "browse"])
    assert result.exit_code != 0, (result.output, result.stderr)
    # Failure surfaces on stderr (not stdout) so JSON consumers don't choke.
    assert "failed to fetch bundles" in combined_output(result)
