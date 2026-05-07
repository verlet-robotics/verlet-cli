"""CLIAUTH-06 — ``verlet auth login`` device flow.

Three scenarios cover the RFC 8628 wire contract:

  * test_full_device_flow_cycle — happy path with one ``authorization_pending``
    poll, then a 200 token, then ``/auth/me`` for identity. Asserts profile
    persistence and that the browser was opened.
  * test_no_browser_prints_url — ``--no-browser`` mode prints the URL and
    does NOT call ``webbrowser.open``.
  * test_slow_down_bumps_interval — RFC 8628 §3.5: every ``slow_down`` response
    bumps the polling interval by exactly +5s.
"""

import httpx

from verlet.auth import credentials as creds
from verlet.auth.login import device_flow_login


def test_full_device_flow_cycle(tmp_home, respx_mock, mocked_webbrowser, monkeypatch):
    """Happy path: code → pending → success → /me → profile persisted."""
    # Speed up by zeroing out time.sleep
    import verlet.auth.login as login_mod

    monkeypatch.setattr(login_mod.time, "sleep", lambda *_: None)

    respx_mock.post("/api/platform/v1/auth/device/code").mock(
        return_value=httpx.Response(
            200,
            json={
                "device_code": "dc-plaintext",
                "user_code": "ABCD1234",
                "verification_uri": "https://verlet.co/device",
                "verification_uri_complete": "https://verlet.co/device?user_code=ABCD-1234",
                "expires_in": 600,
                "interval": 1,
            },
        )
    )
    # First poll: pending. Second poll: success.
    respx_mock.post("/api/platform/v1/auth/device/token").mock(
        side_effect=[
            httpx.Response(400, json={"error": "authorization_pending"}),
            httpx.Response(
                200,
                json={
                    "access_token": "jwt.access.value",
                    "refresh_token": "rt.opaque.value",
                    "token_type": "bearer",
                },
            ),
        ]
    )
    respx_mock.get("/api/platform/v1/auth/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": "u1",
                "account_id": "a1",
                "email": "jane@example.com",
                "display_name": "Jane Doe",
                "slug": "jane",
                "active_namespace": {
                    "account_id": "a1",
                    "type": "user",
                    "slug": "jane",
                    "display_name": "Jane Doe",
                    "role": "owner",
                },
                "namespaces": [],
            },
        )
    )

    result = device_flow_login(
        api_url="https://api.verlet.co",
        profile_name="default",
        no_browser=False,
    )

    assert result["email"] == "jane@example.com"
    profile = creds.get_profile("default")
    assert profile is not None
    assert profile["kind"] == "device_flow"
    assert profile["access_token"] == "jwt.access.value"
    assert profile["refresh_token"] == "rt.opaque.value"
    assert profile["identity"]["email"] == "jane@example.com"
    assert profile["identity"]["display_name"] == "Jane Doe"
    assert "expires_at" in profile
    assert profile["api_url"] == "https://api.verlet.co"
    assert profile["active_namespace"]["slug"] == "jane"
    # Browser was opened (no_browser=False).
    assert mocked_webbrowser.called


def test_no_browser_prints_url(
    tmp_home, respx_mock, mocked_webbrowser, monkeypatch, capsys
):
    """--no-browser prints verification URL and does NOT call webbrowser.open."""
    import verlet.auth.login as login_mod

    monkeypatch.setattr(login_mod.time, "sleep", lambda *_: None)

    respx_mock.post("/api/platform/v1/auth/device/code").mock(
        return_value=httpx.Response(
            200,
            json={
                "device_code": "dc",
                "user_code": "ABCD1234",
                "verification_uri": "https://verlet.co/device",
                "verification_uri_complete": "https://verlet.co/device?user_code=ABCD-1234",
                "expires_in": 600,
                "interval": 1,
            },
        )
    )
    respx_mock.post("/api/platform/v1/auth/device/token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "j",
                "refresh_token": "r",
                "token_type": "bearer",
            },
        )
    )
    respx_mock.get("/api/platform/v1/auth/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": "u1",
                "account_id": "a1",
                "email": "jane@example.com",
                "display_name": "Jane Doe",
                "slug": "jane",
                "active_namespace": None,
                "namespaces": [],
            },
        )
    )

    device_flow_login(
        api_url="https://api.verlet.co",
        profile_name="default",
        no_browser=True,
    )
    captured = capsys.readouterr()
    # Verification URL appears in stdout
    assert "https://verlet.co/device?user_code=ABCD-1234" in captured.out
    # --no-browser advisory printed
    assert "--no-browser" in captured.out
    # webbrowser.open NOT called
    assert not mocked_webbrowser.called


def test_slow_down_bumps_interval(
    tmp_home, respx_mock, mocked_webbrowser, monkeypatch
):
    """RFC 8628 §3.5: each ``slow_down`` response adds 5s to the poll interval."""
    # Capture every sleep duration to verify the +5s bump.
    sleeps: list[float] = []
    import verlet.auth.login as login_mod

    monkeypatch.setattr(login_mod.time, "sleep", lambda s: sleeps.append(s))

    respx_mock.post("/api/platform/v1/auth/device/code").mock(
        return_value=httpx.Response(
            200,
            json={
                "device_code": "dc",
                "user_code": "ABCD1234",
                "verification_uri": "https://verlet.co/device",
                "verification_uri_complete": "https://verlet.co/device?user_code=ABCD-1234",
                "expires_in": 600,
                "interval": 5,
            },
        )
    )
    respx_mock.post("/api/platform/v1/auth/device/token").mock(
        side_effect=[
            httpx.Response(400, json={"error": "slow_down"}),
            httpx.Response(400, json={"error": "slow_down"}),
            httpx.Response(
                200,
                json={
                    "access_token": "j",
                    "refresh_token": "r",
                    "token_type": "bearer",
                },
            ),
        ]
    )
    respx_mock.get("/api/platform/v1/auth/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": "u1",
                "account_id": "a1",
                "email": "j@x.com",
                "display_name": "J",
                "slug": "j",
                "active_namespace": None,
                "namespaces": [],
            },
        )
    )

    device_flow_login(
        api_url="https://api.verlet.co",
        profile_name="default",
        no_browser=True,
    )
    # Three polls -> three sleeps before each. Initial 5, bump to 10, bump to 15.
    assert sleeps == [5, 10, 15], f"Expected [5,10,15], got {sleeps}"
