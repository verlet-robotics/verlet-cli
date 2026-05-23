"""Shared httpx → ``click.ClickException`` conversion (0.8.4).

Wraps any block that makes httpx requests so that 4xx/5xx responses and
network errors render through Click's top-level handler as
``Error: <context>: <detail>`` with exit code 1 — instead of dumping the
raw ``httpx.HTTPStatusError`` or ``httpx.RequestError`` traceback. The
context manager is sync but works inside ``async def`` functions too
because the body only catches and re-raises (no awaits required).

Promoted from the in-module ``_raise_http`` helper that originally lived
in ``verlet.ego.catalog``. ``ego.catalog`` has always rendered API errors
cleanly; the rest of the CLI did not, hence the user-visible tracebacks
on ``verlet datasets info <slug>`` and friends fixed in 0.8.4.
"""
from __future__ import annotations

import contextlib
from typing import Iterator

import click
import httpx


@contextlib.contextmanager
def friendly_http(context: str) -> Iterator[None]:
    """Catch httpx errors and re-raise as ``click.ClickException``.

    Args:
        context: A short noun phrase describing what the wrapped block
            is doing — e.g., ``"fetching dataset 'foo'"`` or
            ``"listing bundles"``. Appears in the final user-visible
            error: ``Error: <context>: <detail>``.

    Raises:
        click.ClickException: For any ``httpx.HTTPStatusError`` (4xx/5xx)
            or ``httpx.RequestError`` (DNS / TLS / connection refused /
            timeout) raised inside the ``with`` block. Click's main()
            renders these as ``Error: …`` on stderr with exit 1.

    For HTTP status errors, surfaces the FastAPI-style ``{"detail": …}``
    envelope if the response body is JSON with that shape; otherwise
    falls back to ``HTTP <status>``. For network errors, surfaces the
    underlying httpx exception's string form (``ConnectError``,
    ``ReadTimeout``, etc.).

    Usage from sync or async:

        with friendly_http(f"fetching dataset '{slug}'"):
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.json()
    """
    try:
        yield
    except httpx.HTTPStatusError as exc:
        detail = f"HTTP {exc.response.status_code}"
        try:
            body = exc.response.json()
            if isinstance(body, dict) and body.get("detail"):
                detail = _format_detail(body["detail"])
        except Exception:
            pass
        raise click.ClickException(f"{context}: {detail}") from exc
    except httpx.RequestError as exc:
        raise click.ClickException(
            f"Network error {context}: {exc}"
        ) from exc


def _format_detail(detail: object) -> str:
    """Render a FastAPI ``detail`` field as a readable one-line string.

    A 422 carries ``detail`` as a list of Pydantic validation-error dicts
    (``{type, loc, msg, input}``) — passing that straight to an f-string
    leaks the Python repr (``[{'type': 'missing', 'loc': [...]}]``), which
    is what users were seeing on ``datasets download`` of an ego dataset
    without ``--variant``. Flatten the list into ``field: msg; …`` form;
    fall through to ``str(detail)`` for any other shape.
    """
    if isinstance(detail, str):
        return detail
    if isinstance(detail, list) and detail:
        parts = []
        for entry in detail:
            if not isinstance(entry, dict):
                parts.append(str(entry))
                continue
            loc = entry.get("loc") or []
            # ``loc`` is e.g. ["query", "variant"] — the tail is the
            # field name the user actually controls; the head is the
            # FastAPI source (query/body/path), which we drop.
            field = ".".join(str(x) for x in loc[1:]) or (
                ".".join(str(x) for x in loc) or "?"
            )
            msg = entry.get("msg") or entry.get("type") or "invalid"
            parts.append(f"{field}: {msg}")
        return "; ".join(parts)
    return str(detail)
