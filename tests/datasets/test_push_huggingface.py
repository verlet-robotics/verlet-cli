"""Plan 30-05 Task 2 — `verlet datasets push <slug> --to huggingface://org/repo`.

Eleven behavior tests for CLIDATA-07 (HF push half):

  Pure-function tests for parse_hf_url + resolve_hf_token (4):
    * test_parse_hf_url_well_formed_returns_org_repo
    * test_parse_hf_url_no_slash_raises_bad_parameter
    * test_parse_hf_url_non_hf_scheme_raises_bad_parameter
    * test_resolve_hf_token_profile_value_returned

  Token precedence (3):
    * test_resolve_hf_token_falls_back_to_env_var
    * test_resolve_hf_token_profile_wins_over_env
    * test_resolve_hf_token_neither_configured_raises_usage_error

  Command end-to-end with respx mocks (4):
    * test_push_posts_destination_url_and_hf_token
    * test_push_polls_recent_until_completed
    * test_push_400_prints_detail_and_exits_nonzero
    * test_push_malformed_url_exits_2_no_http
"""
from __future__ import annotations

import os
from unittest.mock import patch

import click
import httpx
import pytest

from verlet.auth.credentials import set_hf_token, upsert_profile


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_default_profile(*, hf_token: str | None = None) -> None:
    """Seed a logged-in device_flow profile (optionally with hf_token)."""
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at="2099-01-01T00:00:00+00:00",
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )
    if hf_token is not None:
        set_hf_token("default", hf_token)


async def _async_noop_sleep(_secs):
    """Patch target for asyncio.sleep so polling stays instant."""
    return None


# ---------------------------------------------------------------------------
# parse_hf_url
# ---------------------------------------------------------------------------


def test_parse_hf_url_well_formed_returns_org_repo():
    from verlet.datasets._validation import parse_hf_url

    assert parse_hf_url("huggingface://acme/test-ds") == ("acme", "test-ds")


def test_parse_hf_url_no_slash_raises_bad_parameter():
    from verlet.datasets._validation import parse_hf_url

    with pytest.raises(click.BadParameter) as exc:
        parse_hf_url("huggingface://no-slash")
    assert "huggingface://org/repo" in str(exc.value)


def test_parse_hf_url_non_hf_scheme_raises_bad_parameter():
    from verlet.datasets._validation import parse_hf_url

    with pytest.raises(click.BadParameter) as exc:
        parse_hf_url("s3://bucket/k")
    assert "only huggingface:// supported" in str(exc.value)


# ---------------------------------------------------------------------------
# resolve_hf_token — D-FORMAT2 precedence: profile > HF_TOKEN env
# ---------------------------------------------------------------------------


def test_resolve_hf_token_profile_value_returned(tmp_home, monkeypatch):
    from verlet.datasets._validation import resolve_hf_token

    _seed_default_profile(hf_token="hf_from_profile")
    monkeypatch.delenv("HF_TOKEN", raising=False)
    assert resolve_hf_token("default") == "hf_from_profile"


def test_resolve_hf_token_falls_back_to_env_var(tmp_home, monkeypatch):
    from verlet.datasets._validation import resolve_hf_token

    # Profile exists but has NO hf_token field.
    _seed_default_profile()
    monkeypatch.setenv("HF_TOKEN", "hf_from_env")
    assert resolve_hf_token("default") == "hf_from_env"


def test_resolve_hf_token_profile_wins_over_env(tmp_home, monkeypatch):
    """D-FORMAT2: profile-resolved value beats HF_TOKEN env var."""
    from verlet.datasets._validation import resolve_hf_token

    _seed_default_profile(hf_token="hf_from_profile")
    monkeypatch.setenv("HF_TOKEN", "hf_from_env")
    assert resolve_hf_token("default") == "hf_from_profile"


def test_resolve_hf_token_neither_configured_raises_usage_error(tmp_home, monkeypatch):
    """Verbatim error string from D-FORMAT2 / 30-RESEARCH.md Q2."""
    from verlet.datasets._validation import NO_HF_TOKEN_MSG, resolve_hf_token

    _seed_default_profile()
    monkeypatch.delenv("HF_TOKEN", raising=False)
    with pytest.raises(click.UsageError) as exc:
        resolve_hf_token("default")
    # Byte-asserted verbatim match (Phase 31 pattern).
    assert NO_HF_TOKEN_MSG == (
        "No HF token configured. "
        "Run `verlet auth tokens set hf <token>` or set HF_TOKEN env."
    )
    assert NO_HF_TOKEN_MSG in str(exc.value)


# ---------------------------------------------------------------------------
# verlet datasets push — end-to-end with respx mocks
# ---------------------------------------------------------------------------


def test_push_posts_destination_url_and_hf_token(
    cli_runner, respx_mock, tmp_home, monkeypatch
):
    """POST /downloads/{slug}/push body carries destination_url + hf_token."""
    from verlet.cli import cli

    _seed_default_profile(hf_token="hf_xxx")
    monkeypatch.delenv("HF_TOKEN", raising=False)
    slug = "my-slug"
    push_id = "11111111-2222-3333-4444-555555555555"

    push_route = respx_mock.post(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/push"
    ).respond(
        202,
        json={"push_id": push_id, "status": "pending"},
    )
    # /pushes/recent — return one completed entry on the very first poll.
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pushes/recent"
    ).respond(
        200,
        json={"pushes": [{"push_id": push_id, "status": "completed", "progress": 100}]},
    )

    with patch("verlet.datasets.push.asyncio.sleep", new=_async_noop_sleep):
        result = cli_runner.invoke(
            cli,
            ["datasets", "push", slug, "--to", f"huggingface://acme/test"],
        )

    assert result.exit_code == 0, result.output
    assert push_route.called, "push endpoint should have been hit"
    # Inspect the request body shape.
    push_call = push_route.calls.last
    import json as _json

    body = _json.loads(push_call.request.content)
    assert body["destination_url"] == "huggingface://acme/test"
    assert body["hf_token"] == "hf_xxx"


def test_push_polls_recent_until_completed(
    cli_runner, respx_mock, tmp_home, monkeypatch
):
    """After 202, the CLI polls /pushes/recent until matching push_id completes."""
    from verlet.cli import cli

    _seed_default_profile(hf_token="hf_xxx")
    monkeypatch.delenv("HF_TOKEN", raising=False)
    slug = "my-slug"
    push_id = "abcd1234"

    respx_mock.post(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/push"
    ).respond(202, json={"push_id": push_id, "status": "pending"})
    # First poll: status=pending. Second: completed.
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/downloads/pushes/recent"
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "pushes": [
                        {"push_id": push_id, "status": "pending", "progress": 5}
                    ]
                },
            ),
            httpx.Response(
                200,
                json={
                    "pushes": [
                        {
                            "push_id": push_id,
                            "status": "completed",
                            "progress": 100,
                        }
                    ]
                },
            ),
        ]
    )

    with patch("verlet.datasets.push.asyncio.sleep", new=_async_noop_sleep):
        result = cli_runner.invoke(
            cli,
            ["datasets", "push", slug, "--to", "huggingface://acme/test"],
        )

    assert result.exit_code == 0, result.output
    poll_calls = [
        c for c in respx_mock.calls
        if c.request.url.path == "/api/platform/v1/downloads/pushes/recent"
    ]
    assert len(poll_calls) >= 2, f"expected ≥2 polls, got {len(poll_calls)}"


def test_push_400_prints_detail_and_exits_nonzero(
    cli_runner, respx_mock, tmp_home, monkeypatch
):
    """Server 400 with detail → CLI prints detail to stderr + exits non-zero."""
    from verlet.cli import cli

    _seed_default_profile(hf_token="hf_xxx")
    monkeypatch.delenv("HF_TOKEN", raising=False)
    slug = "my-slug"

    respx_mock.post(
        f"https://api.verlet.co/api/platform/v1/downloads/{slug}/push"
    ).respond(400, json={"detail": "Only huggingface:// destination_url is supported"})

    result = cli_runner.invoke(
        cli,
        ["datasets", "push", slug, "--to", "huggingface://acme/test"],
    )
    assert result.exit_code != 0
    combined = (result.output or "") + (result.stderr or "")
    assert "Only huggingface://" in combined or "push failed" in combined


def test_push_malformed_url_exits_2_no_http(
    cli_runner, respx_mock, tmp_home, monkeypatch
):
    """Malformed --to URL → click.BadParameter (exit 2) before any HTTP fires."""
    from verlet.cli import cli

    _seed_default_profile(hf_token="hf_xxx")
    monkeypatch.delenv("HF_TOKEN", raising=False)

    result = cli_runner.invoke(
        cli,
        ["datasets", "push", "my-slug", "--to", "huggingface://no-slash"],
    )
    assert result.exit_code == 2, result.output
    # ZERO HTTP — parser fires before the network call.
    assert len(respx_mock.calls) == 0, (
        f"malformed URL must short-circuit before HTTP, "
        f"got {len(respx_mock.calls)} calls"
    )


def test_push_help_documents_to_flag():
    """`verlet datasets push --help` mentions --to."""
    from click.testing import CliRunner

    from verlet.cli import cli

    runner = CliRunner()
    result = runner.invoke(cli, ["datasets", "push", "--help"])
    assert result.exit_code == 0, result.output
    assert "--to" in result.output


def test_push_no_token_configured_exits_with_verbatim_error(
    cli_runner, respx_mock, tmp_home, monkeypatch
):
    """No hf_token + no HF_TOKEN env → exit non-zero with verbatim D-FORMAT2 message.

    Phase 31 byte-asserted-error pattern: assert NO_HF_TOKEN_MSG appears
    verbatim in stderr/output.
    """
    from verlet.cli import cli
    from verlet.datasets._validation import NO_HF_TOKEN_MSG

    _seed_default_profile()  # NO hf_token
    monkeypatch.delenv("HF_TOKEN", raising=False)

    result = cli_runner.invoke(
        cli,
        ["datasets", "push", "my-slug", "--to", "huggingface://acme/test"],
    )
    assert result.exit_code != 0
    combined = (result.output or "") + (result.stderr or "")
    assert NO_HF_TOKEN_MSG in combined, combined
    # Zero HTTP — token gate fires before the network call.
    assert len(respx_mock.calls) == 0
