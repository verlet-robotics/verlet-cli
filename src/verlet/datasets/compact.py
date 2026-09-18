"""Post-download compaction of filtered showcase datasets.

A publisher quality filter (``exclude_worst_pct`` / ``exclude_recovery_episodes``)
drops episodes from the served download manifest. The showcase download then lands
a *gappy* tree — surviving episodes keep their original indices, so the local
directory has holes — and its ``meta/`` still describes the full, pre-filter
dataset. The LeRobot v2.1 loader assumes a contiguous ``0..total_episodes-1``
range (and the episode index lives *inside* the parquet bytes), so a gappy tree
will not load.

This module renumbers the downloaded tree locally so it loads cleanly:

* **Teleop** — renumber survivors ``0..N-1``, rewrite each parquet's
  ``episode_index`` (constant) + dataset-global ``index`` columns, move files into
  the canonical ``data/chunk-XXX/`` + ``videos/chunk-XXX/observation.images.CAM/``
  layout, and regenerate ``meta/info.json`` / ``episodes.jsonl`` /
  ``episodes_stats.jsonl``. Mirrors the backend transform in
  ``core/workflows/dataset_aggregation.py::_process_parquet`` and the meta regen in
  ``scripts/backfill_compact_gappy_datasets.py``.

  **Ledger-only datasets** (the manifest carries inline ``meta`` instead of
  ``meta/`` objects) are *always* finalized here, even when the tree is already
  contiguous: their parquets still carry the station's own episode numbering and
  no ``meta/`` exists until this step writes it from the manifest's ``info`` +
  ``task`` and the parquets on disk.
* **Ego** — segments are self-contained (per-segment, segment-local parquet index),
  so compaction is pure ``segment_NNNNNN`` directory renumbering; no parquet rewrite.

Cost is gated on gaps actually existing: an already-contiguous set skips the
parquet rewrite entirely (videos are *moved*, never re-encoded), so an unfiltered
download pays ~nothing. ``pyarrow`` is imported lazily inside the teleop path so
the ego / no-op / ``--no-compact`` paths stay light.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

# ── LeRobot v2.1 chunked-layout conventions (kept in sync with the backend's
# core/infrastructure/s3_keys.py; copied rather than imported to keep the CLI
# free of a backend dependency). ──────────────────────────────────────────────
CHUNK_SIZE = 1000

_EPISODE_DIR_RE = re.compile(r"^episode_(\d{6})$")
_EPISODE_PARQUET_RE = re.compile(r"episode_(\d{6})\.parquet$")
_SEGMENT_DIR_RE = re.compile(r"^segment_(\d{6})$")


def _chunk_dir(index: int) -> str:
    return f"chunk-{index // CHUNK_SIZE:03d}"


def _episode_stem(index: int) -> str:
    return f"episode_{index:06d}"


def _parquet_rel(index: int) -> str:
    return f"data/{_chunk_dir(index)}/{_episode_stem(index)}.parquet"


def _video_rel(index: int, camera: str) -> str:
    return (
        f"videos/{_chunk_dir(index)}/observation.images.{camera}/"
        f"{_episode_stem(index)}.mp4"
    )


@dataclass
class CompactResult:
    """Outcome summary; ``None`` work fields mean the tree was already clean."""

    modality: str
    units_before: int
    units_after: int
    gaps_closed: bool
    reindexed: bool  # True when parquet index columns were rewritten


# ─────────────────────────────────────────────────────────────────────────────
# Teleop
# ─────────────────────────────────────────────────────────────────────────────


def _discover_teleop_episodes(dataset_dir: Path) -> dict[int, Path]:
    """Map old episode index -> its parquet path, across both layouts.

    Handles the showcase *exploded* layout
    (``episode_NNNNNN/episode_NNNNNN.parquet``) and the canonical chunked layout
    (``data/chunk-XXX/episode_NNNNNN.parquet``). Returns ``{}`` when neither is
    present (not a teleop tree).
    """
    found: dict[int, Path] = {}
    for pq_path in dataset_dir.rglob("episode_*.parquet"):
        m = _EPISODE_PARQUET_RE.search(pq_path.name)
        if m:
            found[int(m.group(1))] = pq_path
    return found


def _episode_videos(dataset_dir: Path, old_index: int) -> dict[str, Path]:
    """Find {camera: mp4_path} for an episode across both layouts."""
    cams: dict[str, Path] = {}
    # Exploded: episode_NNNNNN/videos/<camera>.mp4
    exploded = dataset_dir / _episode_stem(old_index) / "videos"
    if exploded.is_dir():
        for mp4 in exploded.glob("*.mp4"):
            cams[mp4.stem] = mp4
    # Canonical: videos/chunk-XXX/observation.images.<camera>/episode_NNNNNN.mp4
    canonical_glob = (
        f"videos/{_chunk_dir(old_index)}/observation.images.*/"
        f"{_episode_stem(old_index)}.mp4"
    )
    for mp4 in dataset_dir.glob(canonical_glob):
        cam = mp4.parent.name.split("observation.images.", 1)[-1]
        cams.setdefault(cam, mp4)
    return cams


def _episode_calibration(dataset_dir: Path, old_index: int) -> Path | None:
    """Per-episode calibration JSON, in either downloaded layout."""
    for cand in (
        dataset_dir / _episode_stem(old_index) / "calibration.json",
        dataset_dir / "meta" / "calibration" / f"{_episode_stem(old_index)}.json",
    ):
        if cand.is_file():
            return cand
    return None


def _reindex_table(table, new_index: int, offset: int):
    """Rewrite the index columns of one episode's parquet for position
    ``new_index`` / global row offset ``offset``: ``episode_index`` (constant),
    ``index`` (cumulative), ``task_index`` (0 — one task per dataset), and the
    ``path`` field of any legacy ``observation.images.*`` struct column. Mirrors
    the backend's ``_process_parquet``."""
    import pyarrow as pa
    import pyarrow.compute as pc

    num_rows = table.num_rows
    names = table.column_names

    def _set(name: str, values) -> None:
        nonlocal table
        if name in names:
            t = table.schema.field(name).type
            table = table.set_column(names.index(name), name, pa.array(values, type=t))

    _set("episode_index", [new_index] * num_rows)
    _set("index", range(offset, offset + num_rows))
    _set("task_index", [0] * num_rows)

    for name in names:
        if not name.startswith("observation.images."):
            continue
        col = table.column(name)
        if not pa.types.is_struct(col.type) or col.type.get_field_index("path") < 0:
            continue
        combined = col.combine_chunks() if col.num_chunks > 1 else col.chunks[0]
        fields = [combined.type.field(i).name for i in range(combined.type.num_fields)]
        arrays = []
        for f in fields:
            arr = combined.field(f)
            if f == "path":
                arr = pc.replace_substring_regex(arr, r"episode_\d{6}", _episode_stem(new_index))
                arr = pc.replace_substring_regex(arr, r"chunk-\d{3}", _chunk_dir(new_index))
            arrays.append(arr)
        table = table.set_column(
            names.index(name), name, pa.StructArray.from_arrays(arrays, names=fields)
        )
    return table


def compact_teleop(
    dataset_dir: Path, *, force: bool = False, meta: dict | None = None
) -> CompactResult | None:
    """Close index gaps in a teleop dataset tree, in place. Idempotent.

    Acts when episode indices are gappy (a quality filter dropped episodes) or
    when ``force`` is set. A contiguous ``0..N-1`` download of a dataset that
    shipped its own ``meta/`` is left exactly as-is. Otherwise survivors are
    renumbered ``0..N-1``: each parquet's index columns are rewritten, files are
    placed in the canonical ``data/chunk-XXX/`` + ``videos/chunk-XXX/`` layout,
    and ``meta/`` is regenerated — from the downloaded ``meta/`` when there is
    one, or from the manifest's inline ``meta`` (``info`` + ``task``) plus the
    parquets on disk for a ledger-only dataset.

    Returns a :class:`CompactResult`, or ``None`` if ``dataset_dir`` holds no
    teleop episodes (e.g. an ego tree).
    """
    episodes = _discover_teleop_episodes(dataset_dir)
    if not episodes:
        return None

    old_indices = sorted(episodes)
    n = len(old_indices)
    gappy = old_indices != list(range(n))
    if not gappy and not force:
        # No gaps — leave the tree untouched (status quo for unfiltered pulls).
        return CompactResult("teleop", n, n, gaps_closed=False, reindexed=False)

    import pyarrow.parquet as pq  # lazy: only the rewrite path needs pyarrow

    remap = {old: new for new, old in enumerate(old_indices)}  # old -> 0..N-1

    staging = dataset_dir.parent / f".{dataset_dir.name}.compacting"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    fps = float(((meta or {}).get("info") or {}).get("fps") or 30)
    cameras: set[str] = set()
    lengths: dict[int, int] = {}  # new_index -> frame count
    stats: dict[int, dict] = {}
    calibrated: set[int] = set()
    offset = 0
    for old in old_indices:
        new = remap[old]
        dest_pq = staging / _parquet_rel(new)
        dest_pq.parent.mkdir(parents=True, exist_ok=True)

        table = _reindex_table(pq.read_table(episodes[old]), new, offset)
        pq.write_table(table, dest_pq)
        if meta is not None:
            from verlet.datasets._stats import compute_episode_stats

            ep_stats = compute_episode_stats(table, fps)
            if ep_stats:
                stats[new] = ep_stats

        lengths[new] = table.num_rows
        offset += table.num_rows

        for camera, mp4 in _episode_videos(dataset_dir, old).items():
            cameras.add(camera)
            dest_mp4 = staging / _video_rel(new, camera)
            dest_mp4.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(mp4), str(dest_mp4))

        cal = _episode_calibration(dataset_dir, old)
        if cal is not None:
            dest_cal = staging / "meta" / "calibration" / f"{_episode_stem(new)}.json"
            dest_cal.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(cal), str(dest_cal))
            calibrated.add(new)

    if meta is not None:
        _write_teleop_meta(staging, meta, lengths, sorted(cameras), stats, calibrated)
    else:
        _regenerate_teleop_meta(dataset_dir, staging, remap, lengths, sorted(cameras))
    _verify_teleop(staging, n, len(cameras))

    # Top-level files that are not part of the episode tree (LICENSE, notes)
    # ride along; only directories are rebuilt.
    for leftover in dataset_dir.iterdir():
        if leftover.is_file() and not (staging / leftover.name).exists():
            shutil.move(str(leftover), str(staging / leftover.name))

    # Swap: replace the old tree with the compacted one. On any earlier failure
    # we leave staging in place for inspection; re-download (resumable) recovers.
    shutil.rmtree(dataset_dir)
    staging.rename(dataset_dir)

    return CompactResult("teleop", n, n, gaps_closed=gappy, reindexed=True)


def _write_teleop_meta(
    staging: Path,
    meta: dict,
    lengths: dict[int, int],
    cameras: list[str],
    stats: dict[int, dict],
    calibrated: set[int],
) -> None:
    """Write ``meta/`` for a ledger-only dataset from the manifest's inline
    ``meta`` (source ``info.json`` + task string) and what landed on disk."""
    dst_meta = staging / "meta"
    dst_meta.mkdir(parents=True, exist_ok=True)
    n = len(lengths)
    task = meta.get("task") or ""

    info: dict = dict(meta.get("info") or {})
    info.setdefault("codebase_version", "v2.1")
    info.setdefault("fps", 30)
    info.setdefault("chunks_size", CHUNK_SIZE)
    info.setdefault("data_path", "data/chunk-{episode_chunk:03d}/episode_{episode_index:06d}.parquet")
    info.setdefault(
        "video_path",
        "videos/chunk-{episode_chunk:03d}/{video_key}/episode_{episode_index:06d}.mp4",
    )
    # Only the cameras that were actually downloaded (depth is not served).
    features = dict(info.get("features") or {})
    for key in list(features):
        if key.startswith("observation.images.") and key[len("observation.images."):] not in cameras:
            del features[key]
    info["features"] = features
    info["total_episodes"] = n
    info["total_frames"] = sum(lengths.values())
    info["total_videos"] = n * len(cameras)
    info["total_chunks"] = ((n - 1) // CHUNK_SIZE + 1) if n else 0
    info["total_tasks"] = 1
    info["splits"] = {"train": f"0:{n}"}
    (dst_meta / "info.json").write_text(json.dumps(info, indent=4) + "\n")

    (dst_meta / "tasks.jsonl").write_text(
        json.dumps({"task_index": 0, "task": task}) + "\n"
    )
    (dst_meta / "episodes.jsonl").write_text(
        "".join(
            # ``tasks: [0]`` (a task_index) matches what the backend writes.
            json.dumps({"episode_index": i, "tasks": [0], "length": lengths[i]}) + "\n"
            for i in sorted(lengths)
        )
    )
    (dst_meta / "episodes_stats.jsonl").write_text(
        "".join(
            json.dumps({"episode_index": i, "stats": stats[i]}) + "\n"
            for i in sorted(stats)
        )
    )


def _regenerate_teleop_meta(
    dataset_dir: Path,
    staging: Path,
    remap: dict[int, int],
    lengths: dict[int, int],
    cameras: list[str],
) -> None:
    """Rewrite info.json / episodes.jsonl / episodes_stats.jsonl for the new set.

    Reads the downloaded originals under ``dataset_dir/meta`` (which still describe
    the full dataset), remaps old->new indices, and writes the compacted meta into
    ``staging/meta``. Unrelated meta files (tasks.jsonl, modality.json, stats.json,
    …) are copied verbatim. Mirrors backfill_compact_gappy_datasets.py.
    """
    src_meta = dataset_dir / "meta"
    dst_meta = staging / "meta"
    dst_meta.mkdir(parents=True, exist_ok=True)

    n = len(remap)
    total_frames = sum(lengths.values())
    new_by_old = remap  # alias

    # Pre-read per-old-index rows from the original episodes.jsonl (tasks/duration).
    old_episode_rows: dict[int, dict] = {}
    src_episodes = src_meta / "episodes.jsonl"
    if src_episodes.exists():
        for line in src_episodes.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            old_episode_rows[row["episode_index"]] = row

    # info.json — update totals; preserve everything else.
    src_info = src_meta / "info.json"
    info: dict = json.loads(src_info.read_text()) if src_info.exists() else {}
    info["total_episodes"] = n
    info["total_frames"] = total_frames
    info["total_videos"] = n * len(cameras)
    info["total_chunks"] = ((n - 1) // CHUNK_SIZE + 1) if n else 0
    info["splits"] = {"train": f"0:{n}"}
    (dst_meta / "info.json").write_text(json.dumps(info, indent=4) + "\n")

    # episodes.jsonl — one line per survivor in new order.
    ep_lines = []
    for old in sorted(remap):
        new = new_by_old[old]
        old_row = old_episode_rows.get(old, {})
        out = {
            "episode_index": new,
            "tasks": old_row.get("tasks", [0]),
            "length": lengths[new],
        }
        if "duration" in old_row:
            out["duration"] = old_row["duration"]
        ep_lines.append(json.dumps(out))
    (dst_meta / "episodes.jsonl").write_text("\n".join(ep_lines) + "\n")

    # episodes_stats.jsonl — remap old->new, drop episodes with no stats.
    src_stats = src_meta / "episodes_stats.jsonl"
    if src_stats.exists():
        by_old = {}
        for line in src_stats.read_text().splitlines():
            line = line.strip()
            if line:
                row = json.loads(line)
                by_old[row["episode_index"]] = row
        out_lines = []
        for old in sorted(remap):
            row = by_old.get(old)
            if row is not None:
                row["episode_index"] = new_by_old[old]
                out_lines.append(json.dumps(row))
        (dst_meta / "episodes_stats.jsonl").write_text(
            ("\n".join(out_lines) + "\n") if out_lines else ""
        )

    # Copy any remaining meta files verbatim (tasks.jsonl, modality.json, …).
    if src_meta.is_dir():
        regenerated = {"info.json", "episodes.jsonl", "episodes_stats.jsonl"}
        for f in src_meta.iterdir():
            if f.is_file() and f.name not in regenerated:
                shutil.copy2(f, dst_meta / f.name)


def _verify_teleop(tree: Path, n: int, num_cameras: int) -> None:
    """Post-condition guard: contiguous 0..N-1 parquet + N*cameras videos."""
    parquets = sorted(
        int(_EPISODE_PARQUET_RE.search(p.name).group(1))
        for p in tree.rglob("episode_*.parquet")
    )
    if parquets != list(range(n)):
        raise RuntimeError(
            f"compaction produced non-contiguous parquet set: {parquets[:5]}…"
        )
    if num_cameras:
        videos = list(tree.rglob("videos/**/episode_*.mp4"))
        if len(videos) != n * num_cameras:
            raise RuntimeError(
                f"compaction expected {n * num_cameras} videos, got {len(videos)}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Ego
# ─────────────────────────────────────────────────────────────────────────────


def compact_ego(dataset_dir: Path) -> CompactResult | None:
    """Renumber gappy ``segment_NNNNNN`` directories to contiguous ``0..N-1``.

    Ego segments are self-contained (each carries its own pose.parquet/info.json
    with a segment-local frame index), so no parquet rewrite is needed — only the
    directory names are renumbered. Idempotent. Returns ``None`` if no segment
    directories are present.
    """
    seg_dirs: dict[int, Path] = {}
    for d in dataset_dir.iterdir() if dataset_dir.is_dir() else []:
        m = _SEGMENT_DIR_RE.match(d.name) if d.is_dir() else None
        if m:
            seg_dirs[int(m.group(1))] = d
    if not seg_dirs:
        return None

    old_indices = sorted(seg_dirs)
    n = len(old_indices)
    if old_indices == list(range(n)):
        return CompactResult("ego", n, n, gaps_closed=False, reindexed=False)

    # Stage to avoid rename collisions (e.g. segment_000005 -> segment_000001).
    staging = dataset_dir.parent / f".{dataset_dir.name}.compacting"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    try:
        for new, old in enumerate(old_indices):
            shutil.move(str(seg_dirs[old]), str(staging / f"segment_{new:06d}"))
        # Move any non-segment files (rare) across, then swap.
        for leftover in list(dataset_dir.iterdir()):
            shutil.move(str(leftover), str(staging / leftover.name))
        shutil.rmtree(dataset_dir)
        staging.rename(dataset_dir)
    except Exception:
        raise

    return CompactResult("ego", n, n, gaps_closed=True, reindexed=False)


# ─────────────────────────────────────────────────────────────────────────────


def compact_dataset(
    dataset_dir: Path, modality: str, *, force: bool = False, meta: dict | None = None
) -> CompactResult | None:
    """Dispatch to the teleop or ego compactor by modality."""
    if modality == "ego":
        return compact_ego(dataset_dir)
    return compact_teleop(dataset_dir, force=force, meta=meta)
