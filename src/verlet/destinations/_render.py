"""Rich table builders for ``verlet destinations`` output.

Pure functions: take dict payloads, return Rich ``Table`` objects with no
print side-effect. The caller (commands.py) prints to ``verlet.display.console``.
"""
from __future__ import annotations

from typing import Any

from rich.table import Table


def destinations_table(items: list[dict[str, Any]]) -> Table:
    """Saved cloud destinations — one row per ``CloudDestination``.

    Credentials are never part of the response, so nothing secret renders.
    The ID column is truncated; ``--json`` carries the full id for scripting.
    """
    table = Table(title="Cloud Destinations")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Provider")
    table.add_column("Connect")
    table.add_column("Bucket")
    table.add_column("Prefix")
    table.add_column("Region")
    table.add_column("ID")
    for d in items:
        dest_id = d.get("id") or ""
        table.add_row(
            d.get("name", ""),
            d.get("provider") or "—",
            d.get("auth_kind") or "—",
            d.get("bucket") or "—",
            d.get("prefix") or "—",
            d.get("region") or "—",
            f"{dest_id[:8]}…" if dest_id else "—",
        )
    return table


def providers_table(items: list[dict[str, Any]]) -> Table:
    """Connectable destination providers — name, label, how to connect,
    and the credential keys ``add`` will prompt for (or the deeplink hint
    when the backend serves one). The Credentials column reads from the
    server's ``manual_fields`` when populated and falls back to the CLI's
    per-provider static knowledge so the column is never empty for
    manual-kind providers — see :mod:`verlet.destinations._fields`.
    """
    from verlet.destinations._fields import fallback_summary

    table = Table(title="Destination Providers")
    table.add_column("Provider", style="cyan", no_wrap=True)
    table.add_column("Label")
    table.add_column("Connect via")
    table.add_column("Credentials")
    table.add_column("Notes")
    for p in items:
        name = p.get("name", "")
        auth_kind = p.get("auth_kind") or "—"
        # Server-advertised manual_fields > CLI fallback. Deeplink/oauth
        # providers carry their own connect flow and have no field list
        # to render here — we leave Credentials blank for those.
        if auth_kind == "manual":
            server_fields = p.get("manual_fields") or []
            if server_fields:
                creds_str = ", ".join(
                    f.get("key") or f.get("name") or "" for f in server_fields
                ) or "—"
            else:
                creds_str = fallback_summary(name) or "—"
        else:
            creds_str = "—"
        table.add_row(
            name,
            p.get("label") or "—",
            auth_kind,
            creds_str,
            p.get("deeplink_hint") or "",
        )
    return table
