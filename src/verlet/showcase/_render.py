"""Rich renderers for ``verlet showcase`` output."""
from __future__ import annotations

from typing import Any

from rich.table import Table


def operation_stats_view(body: dict[str, Any]) -> Table:
    """Headline fleet numbers for ``verlet showcase stats`` (G-S3).

    A single key/value table — the credibility numbers a prospect wants
    (fleet size, recent throughput, QC pass rate) with the ego/teleop split.
    """
    table = Table(title="Verlet Fleet — Operation Stats", show_header=False)
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    table.add_row("rigs deployed", str(body.get("rigs_deployed") or 0))
    table.add_row(
        "  ego / teleop",
        f"{body.get('ego_rigs_deployed') or 0} / "
        f"{body.get('teleop_rigs_deployed') or 0}",
    )
    table.add_row(
        "active operators (24h)", str(body.get("active_operators_24h") or 0)
    )
    table.add_row("episodes (7d)", str(body.get("total_episodes_7d") or 0))

    duration_secs = body.get("total_duration_secs_7d") or 0
    table.add_row("hours collected (7d)", f"{duration_secs / 3600:.1f}h")

    qc = body.get("qc_pass_rate_7d")
    table.add_row(
        "QC pass rate (7d)",
        f"{qc * 100:.1f}%" if isinstance(qc, (int, float)) else "—",
    )
    table.add_row("generated at", body.get("generated_at") or "—")
    return table
