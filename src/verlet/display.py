"""Rich display helpers for CLI output."""
from rich.table import Table
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


def ego_segment_table(segments: list[dict]) -> Table:
    table = Table(title="Ego Segments")
    table.add_column("ID", style="dim")
    table.add_column("Category")
    table.add_column("Subcategory")
    table.add_column("Duration", justify="right")
    for seg in segments:
        table.add_row(
            seg["id"][:8],
            seg.get("category", "---"),
            seg.get("subcategory", "---"),
            format_duration(seg.get("duration_s", 0)),
        )
    return table


def teleop_dataset_table(datasets: list[dict]) -> Table:
    table = Table(title="Teleop Datasets")
    table.add_column("ID", style="dim")
    table.add_column("Task Name")
    table.add_column("Episodes", justify="right")
    table.add_column("Frames", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Size", justify="right")
    for ds in datasets:
        table.add_row(
            ds["id"][:8],
            ds["task_name"],
            str(ds["total_episodes"]),
            f"{ds['total_frames']:,}",
            format_duration(ds["total_duration_secs"]),
            format_bytes(ds.get("total_bytes")),
        )
    return table


def teleop_episode_table(episodes: list[dict]) -> Table:
    table = Table(title="Episodes")
    table.add_column("Index", justify="right")
    table.add_column("Frames", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Cameras", justify="right")
    for ep in episodes:
        cameras = len(ep.get("video_paths") or {})
        table.add_row(
            str(ep["episode_index"]),
            str(ep["frame_count"]),
            format_duration(ep["duration_secs"]),
            str(cameras),
        )
    return table
