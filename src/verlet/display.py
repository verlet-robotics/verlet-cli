"""Rich display helpers for CLI output."""
from rich.console import Console

console = Console()


def format_duration(secs: float) -> str:
    if secs <= 0:
        return "---"
    h = int(secs // 3600)
    m = int((secs % 3600) // 60)
    s = int(secs % 60)
    if h > 0:
        return f"{h}h {m}m"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def format_bytes(b: int | None) -> str:
    if not b or b <= 0:
        return "---"
    if b >= 1e9:
        return f"{b / 1e9:.1f} GB"
    if b >= 1e6:
        return f"{b / 1e6:.1f} MB"
    return f"{b / 1e3:.0f} KB"
