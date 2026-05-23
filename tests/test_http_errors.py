"""Regression test for the 0.8.4 fix: API helpers in ``datasets/_api.py``
and ``bundles/_api.py`` should convert ``httpx.HTTPStatusError`` (4xx/5xx)
and ``httpx.RequestError`` (network failures) into
``click.ClickException`` so the CLI prints
``Error: <context>: <detail>`` instead of a raw Python traceback.

Pre-existing bug (0.8.0–0.8.3, before this fix): ``resp.raise_for_status()``
was called bare in those helpers, so a 404 from
``GET /api/platform/v1/catalog/datasets/<bad-slug>`` produced this user-
facing output:

    Traceback (most recent call last):
      ... 30+ frames ...
    httpx.HTTPStatusError: Client error '404 Not Found' for url
    'https://api.verlet.co/api/platform/v1/catalog/datasets/...'

Fix: wrap each call site with ``verlet._http_errors.friendly_http(context)``,
which converts both exception types to ``click.ClickException`` carrying
the FastAPI-style ``{"detail": …}`` envelope when present.
"""

from __future__ import annotations

import httpx
import pytest

from verlet._http_errors import friendly_http
from verlet.cli import cli


# ---------------------------------------------------------------------------
# Unit-level: the context manager itself
# ---------------------------------------------------------------------------


def test_friendly_http_passes_through_on_success():
    """No exception inside the block → no exception raised."""
    with friendly_http("doing something"):
        x = 1 + 1
    assert x == 2


def test_friendly_http_converts_404_with_detail():
    """FastAPI-style ``{"detail": "..."}`` envelope surfaces verbatim."""
    import click

    response = httpx.Response(
        404,
        json={"detail": "Dataset 'unknown' not found"},
        request=httpx.Request("GET", "https://example.com/x"),
    )
    with pytest.raises(click.ClickException) as exc_info:
        with friendly_http("fetching dataset 'unknown'"):
            response.raise_for_status()
    assert (
        "fetching dataset 'unknown': Dataset 'unknown' not found"
        in str(exc_info.value)
    )


def test_friendly_http_falls_back_to_status_when_no_detail():
    """Bare ``HTTP <status>`` when the response body has no ``detail`` field."""
    import click

    response = httpx.Response(
        503,
        text="bad gateway html or whatever",
        request=httpx.Request("GET", "https://example.com/x"),
    )
    with pytest.raises(click.ClickException) as exc_info:
        with friendly_http("listing bundles"):
            response.raise_for_status()
    assert "listing bundles: HTTP 503" in str(exc_info.value)


def test_friendly_http_converts_network_error():
    """``httpx.RequestError`` (DNS / connection / timeout) → ClickException."""
    import click

    request = httpx.Request("GET", "https://example.com/x")
    with pytest.raises(click.ClickException) as exc_info:
        with friendly_http("listing bundles"):
            raise httpx.ConnectError("Could not connect", request=request)
    assert "Network error listing bundles" in str(exc_info.value)
    assert "Could not connect" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Integration: actual CLI invocations route through the wrapper end-to-end
# ---------------------------------------------------------------------------


def test_datasets_info_404_renders_friendly_error(respx_mock, cli_runner, tmp_home):
    """``verlet datasets info <bad-slug>`` should not surface a traceback.

    ``tmp_home`` isolates ``~/.verlet`` so no real credential is picked up —
    with no profile the command takes the anonymous platform-catalog path.
    """
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/datasets/bad-slug"
    ).mock(
        return_value=httpx.Response(
            404, json={"detail": "Dataset 'bad-slug' not found"}
        )
    )
    result = cli_runner.invoke(cli, ["datasets", "info", "bad-slug"])
    assert result.exit_code == 1, (result.output, result.exception)
    assert "Traceback" not in result.output
    assert "fetching dataset 'bad-slug'" in result.output
    assert "Dataset 'bad-slug' not found" in result.output


def test_bundles_browse_500_renders_friendly_error(respx_mock, cli_runner):
    """``verlet bundles browse`` on 500 → friendly error, no traceback."""
    respx_mock.get(
        "https://api.verlet.co/api/platform/v1/catalog/research-bundles"
    ).mock(
        return_value=httpx.Response(
            500, json={"detail": "internal server error"}
        )
    )
    result = cli_runner.invoke(cli, ["bundles", "browse"])
    assert result.exit_code == 1, (result.output, result.exception)
    assert "Traceback" not in result.output
    assert "browsing public bundles" in result.output
    assert "internal server error" in result.output


# ---------------------------------------------------------------------------
# _format_detail — FastAPI 422 validation-error list rendering
# ---------------------------------------------------------------------------
#
# Hands-on surfaced this leak: ``verlet datasets download <ego-slug>`` on a
# showcase profile without ``--variant`` produced
# ``Error: ... [{'type': 'missing', 'loc': ['query', 'variant'], ...}]`` —
# the literal Python repr of FastAPI's validation-error list. The wire
# detail is a list, so the old f-string rendered repr(list). The
# formatter flattens it into ``field: msg`` form.


def test_format_detail_passes_string_through():
    from verlet._http_errors import _format_detail

    assert _format_detail("Invalid access code.") == "Invalid access code."


def test_format_detail_renders_pydantic_validation_list():
    """A missing-field validation error renders as ``field: msg``, with
    FastAPI's source prefix (query/body/path) stripped."""
    from verlet._http_errors import _format_detail

    detail = [
        {
            "type": "missing",
            "loc": ["query", "variant"],
            "msg": "Field required",
            "input": None,
        }
    ]
    assert _format_detail(detail) == "variant: Field required"


def test_format_detail_joins_multiple_validation_errors():
    from verlet._http_errors import _format_detail

    detail = [
        {"type": "missing", "loc": ["query", "variant"], "msg": "Field required"},
        {"type": "value_error", "loc": ["body", "name"], "msg": "Too short"},
    ]
    assert (
        _format_detail(detail)
        == "variant: Field required; name: Too short"
    )


def test_format_detail_renders_nested_loc_with_dots():
    from verlet._http_errors import _format_detail

    detail = [
        {
            "type": "missing",
            "loc": ["body", "credentials", "access_key_id"],
            "msg": "Field required",
        }
    ]
    assert (
        _format_detail(detail)
        == "credentials.access_key_id: Field required"
    )


def test_friendly_http_422_renders_validation_list_inline(respx_mock):
    """End-to-end through the wrapper: a 422 with a validation-error list
    body must NOT leak the Python list-of-dicts repr to users."""
    import click

    response = httpx.Response(
        422,
        json={
            "detail": [
                {
                    "type": "missing",
                    "loc": ["query", "variant"],
                    "msg": "Field required",
                    "input": None,
                }
            ]
        },
        request=httpx.Request("GET", "https://example.com/x"),
    )
    with pytest.raises(click.ClickException) as exc_info:
        with friendly_http("fetching download manifest"):
            response.raise_for_status()
    rendered = str(exc_info.value)
    assert (
        "fetching download manifest: variant: Field required" in rendered
    )
    # The raw repr must not bleed through.
    assert "{'type'" not in rendered
    assert "[{'" not in rendered
