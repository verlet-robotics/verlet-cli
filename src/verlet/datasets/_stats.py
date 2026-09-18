"""LeRobot v2.1 per-episode statistics for locally finalized teleop datasets.

Verbatim port of ``backend/core/rules/lerobot_stats.py`` (the CLI carries no
backend dependency). Scalar columns use closed-form formulas from the row
count; vector columns (observation.state, action) use per-dimension
min/max/mean and population std. No image stats — the parquet holds only
path references and the recorder has never emitted them.

episodes_stats.jsonl line shape::

    {"episode_index": N, "stats": {feature: {min, max, mean, std, count}}}
"""

from __future__ import annotations

import math

import numpy as np
import pyarrow as pa

VECTOR_FEATURES = ("observation.state", "action")


def compute_scalar_stats(
    rows: int, fps: float, episode_index: int, task_index: int, global_index_offset: int
) -> dict[str, dict]:
    if rows <= 0:
        return {}
    n = float(rows)
    fps_f = float(fps)

    def arange_stats(start: float) -> dict:
        return {
            "min": [start],
            "max": [start + n - 1.0],
            "mean": [start + (n - 1.0) / 2.0],
            "std": [math.sqrt((n * n - 1.0) / 12.0)],
            "count": [rows],
        }

    def constant_stats(value: float) -> dict:
        return {"min": [value], "max": [value], "mean": [value], "std": [0.0], "count": [rows]}

    nd_mean = 1.0 / n
    return {
        "timestamp": {
            "min": [0.0],
            "max": [(n - 1.0) / fps_f],
            "mean": [(n - 1.0) / (2.0 * fps_f)],
            "std": [math.sqrt((n * n - 1.0) / (12.0 * fps_f * fps_f))],
            "count": [rows],
        },
        "frame_index": arange_stats(0.0),
        "episode_index": constant_stats(float(episode_index)),
        "index": arange_stats(float(global_index_offset)),
        "task_index": constant_stats(float(task_index)),
        "next.done": {
            "min": [0.0],
            "max": [1.0],
            "mean": [nd_mean],
            "std": [math.sqrt((1.0 - nd_mean) * nd_mean)],
            "count": [rows],
        },
    }


def list_col_to_stats(col, dim: int, num_rows: int) -> dict | None:
    """Per-dim stats for a list<float> Arrow column; None if rows vary in length."""
    if num_rows == 0 or dim == 0:
        return None
    parts = [
        np.asarray(chunk.flatten().to_numpy(zero_copy_only=False), dtype=np.float64)
        for chunk in col.chunks
    ]
    flat = np.concatenate(parts) if parts else np.empty(0, dtype=np.float64)
    if flat.size != num_rows * dim:
        return None
    arr = flat.reshape(num_rows, dim)
    return {
        "min": arr.min(axis=0).tolist(),
        "max": arr.max(axis=0).tolist(),
        "mean": arr.mean(axis=0).tolist(),
        "std": arr.std(axis=0).tolist(),
        "count": [num_rows],
    }


def compute_episode_stats(table: pa.Table, fps: float) -> dict | None:
    """Full ``stats`` object for an episode table ALREADY re-indexed to its
    target ``episode_index`` / ``index`` / ``task_index`` values."""
    num_rows = table.num_rows
    if num_rows == 0:
        return None
    names = set(table.schema.names)

    def first(col: str, default: int = 0) -> int:
        return int(table.column(col)[0].as_py()) if col in names else default

    stats = compute_scalar_stats(
        rows=num_rows,
        fps=fps,
        episode_index=first("episode_index"),
        task_index=first("task_index"),
        global_index_offset=first("index"),
    )
    stats = {k: v for k, v in stats.items() if k in names}
    for feature in VECTOR_FEATURES:
        if feature not in names:
            continue
        col = table.column(feature)
        dim = len(col[0].as_py() or [])
        vec = list_col_to_stats(col, dim, num_rows)
        if vec is not None:
            stats[feature] = vec
    return stats or None
