"""Tests for `verlet datasets push --destination` (saved cloud destination).

The HuggingFace `--to` path is covered by test_push_huggingface.py; this file
covers the saved-destination path added with the `verlet destinations` group.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

DEST_BASE = "https://api.verlet.co/api/platform/v1/downloads/destinations"
DEST_ID = "00000000-0000-0000-0000-0000000000d1"
SLUG = "imitate-cube"
PUSH_URL = f"https://api.verlet.co/api/platform/v1/downloads/{SLUG}/push"
RECENT_URL = "https://api.verlet.co/api/platform/v1/downloads/pushes/recent"


def _seed() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _dest(name: str = "my-s3") -> dict:
    return {
        "id": DEST_ID,
        "account_id": "acc-1",
        "name": name,
        "provider": "aws_s3",
        "auth_kind": "deeplink",
        "bucket": "b",
        "prefix": None,
        "region": None,
        "created_at": "2026-05-01T00:00:00+00:00",
        "updated_at": "2026-05-01T00:00:00+00:00",
    }


def _mock_push_completed(respx_mock, push_id: str = "p1") -> None:
    respx_mock.post(PUSH_URL).respond(
        202, json={"push_id": push_id, "status": "pending"}
    )
    respx_mock.get(RECENT_URL).respond(
        200, json={"pushes": [{"push_id": push_id, "status": "completed"}]}
    )


def test_push_with_destination_name_resolves_to_id(
    tmp_home, cli_runner, respx_mock
):
    """`--destination <name>` resolves via GET /destinations, then POSTs by id."""
    _seed()
    respx_mock.get(DEST_BASE).mock(
        return_value=httpx.Response(200, json=[_dest("my-s3")])
    )
    _mock_push_completed(respx_mock)

    result = cli_runner.invoke(
        cli, ["datasets", "push", SLUG, "--destination", "my-s3"]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    push_call = next(
        c for c in respx_mock.calls if c.request.url.path.endswith("/push")
    )
    sent = json.loads(push_call.request.read())
    assert sent["destination_id"] == DEST_ID
    assert "hf_token" not in sent
    assert "destination_url" not in sent


def test_push_with_destination_uuid_skips_lookup(
    tmp_home, cli_runner, respx_mock
):
    """A UUID `--destination` POSTs directly — no GET /destinations lookup."""
    _seed()
    listing = respx_mock.get(DEST_BASE).mock(
        return_value=httpx.Response(200, json=[_dest()])
    )
    _mock_push_completed(respx_mock)

    result = cli_runner.invoke(
        cli, ["datasets", "push", SLUG, "--destination", DEST_ID]
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert not listing.called


def test_push_to_and_destination_both_set_exit_2(
    tmp_home, cli_runner, respx_mock
):
    """`--to` + `--destination` together → UsageError (exit 2), no HTTP."""
    _seed()
    result = cli_runner.invoke(
        cli,
        [
            "datasets", "push", SLUG,
            "--to", "huggingface://acme/x",
            "--destination", "my-s3",
        ],
    )
    assert result.exit_code == 2, (result.output, result.stderr)
    assert len(respx_mock.calls) == 0


def test_push_neither_to_nor_destination_exit_2(
    tmp_home, cli_runner, respx_mock
):
    """Neither `--to` nor `--destination` → UsageError (exit 2), no HTTP."""
    _seed()
    result = cli_runner.invoke(cli, ["datasets", "push", SLUG])
    assert result.exit_code == 2, (result.output, result.stderr)
    assert len(respx_mock.calls) == 0


def test_push_destination_path_skips_hf_token_gate(
    tmp_home, cli_runner, respx_mock, monkeypatch
):
    """The `--destination` path succeeds with no HF token configured anywhere."""
    _seed()
    monkeypatch.delenv("HF_TOKEN", raising=False)
    respx_mock.get(DEST_BASE).mock(
        return_value=httpx.Response(200, json=[_dest("my-s3")])
    )
    _mock_push_completed(respx_mock)

    with patch("verlet.datasets.push.asyncio.sleep", new=lambda *_: None):
        result = cli_runner.invoke(
            cli, ["datasets", "push", SLUG, "--destination", "my-s3"]
        )
    assert result.exit_code == 0, (result.output, result.stderr)
