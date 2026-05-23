"""Tests for `verlet destinations providers` (public; no auth)."""
from __future__ import annotations

import json

import httpx

from verlet.cli import cli

PROVIDERS_PATH = "/api/platform/v1/downloads/destinations/providers"

_PROVIDERS = [
    {
        "name": "aws_s3",
        "label": "Amazon S3",
        "auth_kind": "deeplink",
        "manual_fields": None,
        "deeplink_hint": "CloudFormation quick-create",
    },
    {
        "name": "r2",
        "label": "Cloudflare R2",
        "auth_kind": "manual",
        "manual_fields": None,
        "deeplink_hint": None,
    },
]


def test_providers_lists_auth_kinds(tmp_home, cli_runner, respx_mock):
    """`destinations providers` lists providers anonymously (no Bearer header)."""
    route = respx_mock.get(f"https://api.verlet.co{PROVIDERS_PATH}").mock(
        return_value=httpx.Response(200, json=_PROVIDERS)
    )

    result = cli_runner.invoke(cli, ["destinations", "providers"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert route.called
    # Public endpoint — the CLI must not attach an Authorization header.
    assert "Authorization" not in route.calls.last.request.headers
    assert "aws_s3" in result.output
    assert "r2" in result.output


def test_providers_json_flag(tmp_home, cli_runner, respx_mock):
    """`--json` emits the raw provider list."""
    respx_mock.get(f"https://api.verlet.co{PROVIDERS_PATH}").mock(
        return_value=httpx.Response(200, json=_PROVIDERS)
    )

    result = cli_runner.invoke(cli, ["destinations", "providers", "--json"])
    assert result.exit_code == 0, (result.output, result.stderr)
    parsed = json.loads(result.output)
    assert {p["name"] for p in parsed} == {"aws_s3", "r2"}


def test_providers_table_shows_credential_keys_from_static_fallback(
    tmp_home, cli_runner, respx_mock
):
    """When the server returns ``manual_fields=null``, the CLI's static
    fallback fills the Credentials column so users discover which keys
    each manual provider needs without leaving the terminal.
    """
    respx_mock.get(f"https://api.verlet.co{PROVIDERS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "name": "r2",
                    "label": "Cloudflare R2",
                    "auth_kind": "manual",
                    "manual_fields": None,
                    "deeplink_hint": None,
                },
                {
                    "name": "gcs",
                    "label": "Google Cloud Storage",
                    "auth_kind": "manual",
                    "manual_fields": None,
                    "deeplink_hint": None,
                },
            ],
        )
    )

    result = cli_runner.invoke(cli, ["destinations", "providers"])
    assert result.exit_code == 0, (result.output, result.stderr)
    # R2 keys appear — at least one of them, since the column is narrow
    # and Rich may abbreviate. account_id is the shortest and always wins.
    assert "account_id" in result.output
    # GCS gets the JSON-pointer string, not a bogus key list.
    assert "credentials-json" in result.output or "sa.json" in result.output


def test_providers_table_prefers_server_manual_fields_over_fallback(
    tmp_home, cli_runner, respx_mock
):
    """Server-advertised manual_fields drive the Credentials column. The
    fallback is a backstop, not an override."""
    respx_mock.get(f"https://api.verlet.co{PROVIDERS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "name": "r2",
                    "label": "Cloudflare R2",
                    "auth_kind": "manual",
                    "manual_fields": [
                        {"key": "server_authoritative_key", "label": "X"}
                    ],
                    "deeplink_hint": None,
                }
            ],
        )
    )

    result = cli_runner.invoke(cli, ["destinations", "providers"])
    assert result.exit_code == 0, (result.output, result.stderr)
    assert "server_authoritative_key" in result.output
    # Static-fallback keys must NOT bleed through when the server speaks.
    assert "account_id" not in result.output
