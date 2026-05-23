"""Tests for `verlet destinations add` — the three connect dispatches.

Manual providers POST /destinations directly; deeplink (AWS) runs the
CloudFormation browser+paste flow; oauth providers are deferred with a
pointer to the web app.
"""
from __future__ import annotations

import httpx

from verlet.auth.credentials import upsert_profile
from verlet.cli import cli

BASE = "https://api.verlet.co/api/platform/v1/downloads/destinations"
PROVIDERS_URL = f"{BASE}/providers"
CONNECT_INIT_URL = f"{BASE}/connect/init"
CONNECT_CALLBACK_URL = f"{BASE}/connect/callback"
TEST_CONN_URL = f"{BASE}/test-connection"
FAR_FUTURE = "2099-01-01T00:00:00+00:00"


def _seed() -> None:
    upsert_profile(
        "default",
        kind="device_flow",
        api_url="https://api.verlet.co",
        access_token="jwt.access.value",
        refresh_token="rt",
        expires_at=FAR_FUTURE,
        identity={"display_name": "Jane", "email": "jane@x.com"},
    )


def _provider(name: str, auth_kind: str) -> dict:
    return {
        "name": name,
        "label": name.upper(),
        "auth_kind": auth_kind,
        "manual_fields": None,
        "deeplink_hint": None,
    }


def _dest(name: str, provider: str, auth_kind: str) -> dict:
    return {
        "id": "00000000-0000-0000-0000-0000000000d1",
        "account_id": "acc-1",
        "name": name,
        "provider": provider,
        "auth_kind": auth_kind,
        "bucket": "b",
        "prefix": None,
        "region": None,
        "created_at": "2026-05-01T00:00:00+00:00",
        "updated_at": "2026-05-01T00:00:00+00:00",
    }


# --------------------------------------------------------------------------
# manual
# --------------------------------------------------------------------------


def test_add_manual_credential_pairs_posts_destinations(
    tmp_home, cli_runner, respx_mock
):
    """`--credential K=V` pairs become the credentials dict on POST /destinations."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("r2", "manual")])
    )
    create = respx_mock.post(BASE).mock(
        return_value=httpx.Response(201, json=_dest("my-r2", "r2", "manual"))
    )
    test = respx_mock.post(TEST_CONN_URL).mock(
        return_value=httpx.Response(200, json={"success": True, "message": "ok"})
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "r2",
            "--name", "my-r2", "--bucket", "data",
            "--credential", "account_id=a1",
            "--credential", "access_key_id=k1",
            "--credential", "secret_access_key=s1",
        ],
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert create.called and test.called
    body = create.calls.last.request.read()
    import json as _json
    sent = _json.loads(body)
    assert sent["provider"] == "r2"
    assert sent["name"] == "my-r2"
    assert sent["credentials"] == {
        "account_id": "a1",
        "access_key_id": "k1",
        "secret_access_key": "s1",
    }


def test_add_manual_credentials_json_stdin_non_interactive(
    tmp_home, cli_runner, respx_mock
):
    """`--credentials-json -` reads creds from stdin with zero prompts."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("r2", "manual")])
    )
    create = respx_mock.post(BASE).mock(
        return_value=httpx.Response(201, json=_dest("my-r2", "r2", "manual"))
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "r2",
            "--name", "my-r2", "--bucket", "data",
            "--credentials-json", "-", "--no-test",
        ],
        input='{"account_id": "a1", "access_key_id": "k1"}',
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    import json as _json
    sent = _json.loads(create.calls.last.request.read())
    assert sent["credentials"] == {"account_id": "a1", "access_key_id": "k1"}


def test_add_manual_no_test_skips_connection_test(tmp_home, cli_runner, respx_mock):
    """`--no-test` suppresses the post-create test-connection call."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("r2", "manual")])
    )
    respx_mock.post(BASE).mock(
        return_value=httpx.Response(201, json=_dest("my-r2", "r2", "manual"))
    )
    test = respx_mock.post(TEST_CONN_URL).mock(
        return_value=httpx.Response(200, json={"success": True, "message": "ok"})
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "r2", "--name", "x", "--bucket", "b",
            "--credential", "account_id=a", "--no-test",
        ],
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert not test.called


def test_add_manual_falls_back_to_static_field_prompts(
    tmp_home, cli_runner, respx_mock
):
    """When the server returns ``manual_fields=null`` and the user passes no
    flag-based creds, the CLI's per-provider static fallback drives
    interactive prompts so ``add`` is usable today. R2's three keys
    (account_id, access_key_id, secret_access_key) are prompted in order;
    the resulting credentials dict carries the right keys.
    """
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("r2", "manual")])
    )
    create = respx_mock.post(BASE).mock(
        return_value=httpx.Response(201, json=_dest("my-r2", "r2", "manual"))
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "r2",
            "--name", "my-r2", "--bucket", "data", "--no-test",
        ],
        input="acct-1\nkey-1\nsecret-1\n",
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert create.called
    import json as _json
    sent = _json.loads(create.calls.last.request.read())
    assert sent["credentials"] == {
        "account_id": "acct-1",
        "access_key_id": "key-1",
        "secret_access_key": "secret-1",
    }


def test_add_gcs_without_credentials_json_errors_with_pointer(
    tmp_home, cli_runner, respx_mock
):
    """GCS is JSON-only (a service-account document). With no
    ``--credentials-json`` and no ``--credential``, ``add`` must NOT fall
    into a per-field prompt loop — it must fail fast with a pointer at
    ``--credentials-json`` so the user doesn't try to type a multi-line
    JSON blob into a prompt.
    """
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("gcs", "manual")])
    )
    create = respx_mock.post(BASE).mock(
        return_value=httpx.Response(201, json=_dest("g", "gcs", "manual"))
    )

    result = cli_runner.invoke(
        cli, ["destinations", "add", "gcs", "--name", "g", "--bucket", "b"]
    )
    assert result.exit_code == 2, (result.output, result.stderr)
    assert not create.called
    out = result.output + (result.stderr or "")
    assert "--credentials-json" in out
    assert "service-account" in out.lower()


def test_add_huggingface_prompts_for_token(tmp_home, cli_runner, respx_mock):
    """Single-field providers (HF token) still flow through the prompt path."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(
            200, json=[_provider("huggingface", "manual")]
        )
    )
    create = respx_mock.post(BASE).mock(
        return_value=httpx.Response(
            201, json=_dest("my-hf", "huggingface", "manual")
        )
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "huggingface",
            "--name", "my-hf", "--bucket", "org/ds", "--no-test",
        ],
        input="hf_TOKEN\n",
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    import json as _json
    sent = _json.loads(create.calls.last.request.read())
    assert sent["credentials"] == {"token": "hf_TOKEN"}


def test_add_server_manual_fields_win_over_static_fallback(
    tmp_home, cli_runner, respx_mock
):
    """When the backend DOES advertise ``manual_fields``, it drives the
    prompts — the static fallback is a backstop, not an override. This
    guards against drift the day the backend starts populating the field.
    """
    _seed()
    custom_fields = [
        {"key": "custom_key", "label": "Custom Key", "secret": False},
    ]
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "name": "r2",
                    "label": "Cloudflare R2",
                    "auth_kind": "manual",
                    "manual_fields": custom_fields,
                    "deeplink_hint": None,
                }
            ],
        )
    )
    create = respx_mock.post(BASE).mock(
        return_value=httpx.Response(201, json=_dest("my-r2", "r2", "manual"))
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "r2",
            "--name", "my-r2", "--bucket", "data", "--no-test",
        ],
        input="value-1\n",
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    import json as _json
    sent = _json.loads(create.calls.last.request.read())
    # Only ``custom_key`` is prompted — NOT R2's static account_id/etc.
    assert sent["credentials"] == {"custom_key": "value-1"}


# --------------------------------------------------------------------------
# deeplink (AWS S3)
# --------------------------------------------------------------------------

_CFN_URL = "https://console.aws.amazon.com/cloudformation/home#/stacks/quickcreate"


def test_add_deeplink_opens_browser_and_posts_callback(
    tmp_home, cli_runner, respx_mock, mocked_webbrowser
):
    """Deeplink: opens the CFN URL, then POSTs callback with the pasted RoleArn."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("aws_s3", "deeplink")])
    )
    init = respx_mock.post(CONNECT_INIT_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "auth_kind": "deeplink",
                "authorize_url": _CFN_URL,
                "state": "signed-state-xyz",
                "redirect_uri": "https://app.verlet.co/oauth/callback",
            },
        )
    )
    callback = respx_mock.post(CONNECT_CALLBACK_URL).mock(
        return_value=httpx.Response(
            201, json=_dest("my-s3", "aws_s3", "deeplink")
        )
    )

    result = cli_runner.invoke(
        cli,
        ["destinations", "add", "aws_s3", "--name", "my-s3", "--bucket", "b"],
        input="arn:aws:iam::123456789012:role/verlet-cloud-my-s3\n",
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert init.called and callback.called
    mocked_webbrowser.assert_called_once_with(_CFN_URL)

    import json as _json
    sent = _json.loads(callback.calls.last.request.read())
    assert sent["state"] == "signed-state-xyz"
    assert sent["payload"] == {
        "role_arn": "arn:aws:iam::123456789012:role/verlet-cloud-my-s3"
    }


def test_add_deeplink_no_browser_prints_url(
    tmp_home, cli_runner, respx_mock, mocked_webbrowser
):
    """`--no-browser` prints the CFN URL and does not open a browser."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("aws_s3", "deeplink")])
    )
    respx_mock.post(CONNECT_INIT_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "auth_kind": "deeplink",
                "authorize_url": _CFN_URL,
                "state": "s",
                "redirect_uri": None,
            },
        )
    )
    respx_mock.post(CONNECT_CALLBACK_URL).mock(
        return_value=httpx.Response(201, json=_dest("my-s3", "aws_s3", "deeplink"))
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "aws_s3",
            "--name", "my-s3", "--bucket", "b", "--no-browser",
        ],
        input="arn:aws:iam::123456789012:role/verlet\n",
    )
    assert result.exit_code == 0, (result.output, result.stderr)
    assert _CFN_URL in result.output
    mocked_webbrowser.assert_not_called()


def test_add_deeplink_bad_arn_exits_2_no_callback(
    tmp_home, cli_runner, respx_mock, mocked_webbrowser
):
    """A malformed RoleArn fails (exit 2) before any callback POST."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("aws_s3", "deeplink")])
    )
    respx_mock.post(CONNECT_INIT_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "auth_kind": "deeplink",
                "authorize_url": _CFN_URL,
                "state": "s",
                "redirect_uri": None,
            },
        )
    )
    callback = respx_mock.post(CONNECT_CALLBACK_URL).mock(
        return_value=httpx.Response(201, json=_dest("x", "aws_s3", "deeplink"))
    )

    result = cli_runner.invoke(
        cli,
        ["destinations", "add", "aws_s3", "--name", "x", "--bucket", "b"],
        input="not-an-arn\n",
    )
    assert result.exit_code == 2, (result.output, result.stderr)
    assert not callback.called


def test_add_deeplink_callback_400_surfaces_error(
    tmp_home, cli_runner, respx_mock, mocked_webbrowser
):
    """A callback 400 (AssumeRole failure) surfaces verbatim, no traceback."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("aws_s3", "deeplink")])
    )
    respx_mock.post(CONNECT_INIT_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "auth_kind": "deeplink",
                "authorize_url": _CFN_URL,
                "state": "s",
                "redirect_uri": None,
            },
        )
    )
    respx_mock.post(CONNECT_CALLBACK_URL).mock(
        return_value=httpx.Response(
            400, json={"detail": "Could not assume role; trust policy mismatch"}
        )
    )

    result = cli_runner.invoke(
        cli,
        ["destinations", "add", "aws_s3", "--name", "x", "--bucket", "b"],
        input="arn:aws:iam::123456789012:role/verlet\n",
    )
    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert "assume role" in (result.output + (result.stderr or "")).lower()


# --------------------------------------------------------------------------
# oauth (deferred) + provider/flag errors
# --------------------------------------------------------------------------


def test_add_oauth_provider_is_deferred(tmp_home, cli_runner, respx_mock):
    """An oauth provider prints the web-app deferral and makes no connect call."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("gcs", "oauth")])
    )
    init = respx_mock.post(CONNECT_INIT_URL).mock(
        return_value=httpx.Response(200, json={})
    )

    result = cli_runner.invoke(
        cli, ["destinations", "add", "gcs", "--name", "x", "--bucket", "b"]
    )
    assert result.exit_code != 0
    assert "OAuth" in (result.output + (result.stderr or ""))
    assert not init.called


def test_add_unknown_provider_errors(tmp_home, cli_runner, respx_mock):
    """An unknown provider name errors with the available list."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("aws_s3", "deeplink")])
    )

    result = cli_runner.invoke(
        cli, ["destinations", "add", "nonsense", "--name", "x", "--bucket", "b"]
    )
    assert result.exit_code != 0
    assert "Unknown provider" in (result.output + (result.stderr or ""))


def test_add_credential_flags_on_deeplink_provider_exit_2(
    tmp_home, cli_runner, respx_mock
):
    """`--credential` on a non-manual provider → BadParameter, no connect call."""
    _seed()
    respx_mock.get(PROVIDERS_URL).mock(
        return_value=httpx.Response(200, json=[_provider("aws_s3", "deeplink")])
    )
    init = respx_mock.post(CONNECT_INIT_URL).mock(
        return_value=httpx.Response(200, json={})
    )

    result = cli_runner.invoke(
        cli,
        [
            "destinations", "add", "aws_s3",
            "--name", "x", "--bucket", "b", "--credential", "k=v",
        ],
    )
    assert result.exit_code == 2, (result.output, result.stderr)
    assert not init.called
