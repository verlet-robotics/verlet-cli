"""Tests for post-download dataset compaction (verlet.datasets.compact)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from verlet.datasets.compact import compact_ego, compact_teleop

_EP_RE = re.compile(r"episode_(\d{6})")


# ── helpers ───────────────────────────────────────────────────────────────────


def _write_episode(dataset_dir: Path, old_index: int, *, rows: int, cameras: list[str]):
    """Write one episode in the showcase *exploded* layout with realistic
    LeRobot index columns (episode_index constant, index = local 0..rows-1)."""
    ep_dir = dataset_dir / f"episode_{old_index:06d}"
    (ep_dir / "videos").mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "episode_index": pa.array([old_index] * rows, type=pa.int64()),
            "frame_index": pa.array(range(rows), type=pa.int64()),
            "index": pa.array(range(rows), type=pa.int64()),
            "task_index": pa.array([0] * rows, type=pa.int64()),
            "action": pa.array([float(i) for i in range(rows)], type=pa.float64()),
        }
    )
    pq.write_table(table, ep_dir / f"episode_{old_index:06d}.parquet")
    for cam in cameras:
        (ep_dir / "videos" / f"{cam}.mp4").write_bytes(b"\x00\x00\x00\x18ftyp" + bytes(8))


def _write_meta(dataset_dir: Path, old_indices: list[int], rows: int, cameras: list[str]):
    meta = dataset_dir / "meta"
    meta.mkdir(parents=True, exist_ok=True)
    # info.json describes the FULL pre-filter dataset (deliberately stale here is
    # not the point — we seed it as if it were the original whole dataset).
    (meta / "info.json").write_text(
        json.dumps(
            {
                "codebase_version": "v2.1",
                "total_episodes": len(old_indices),
                "total_frames": rows * len(old_indices),
                "total_videos": len(old_indices) * len(cameras),
                "total_chunks": 1,
                "chunks_size": 1000,
                "fps": 30,
                "splits": {"train": f"0:{len(old_indices)}"},
            }
        )
    )
    (meta / "episodes.jsonl").write_text(
        "\n".join(
            json.dumps(
                {"episode_index": i, "tasks": [0], "length": rows, "duration": 1.5}
            )
            for i in old_indices
        )
        + "\n"
    )
    (meta / "episodes_stats.jsonl").write_text(
        "\n".join(
            json.dumps({"episode_index": i, "stats": {"action": {"mean": [float(i)]}}})
            for i in old_indices
        )
        + "\n"
    )
    (meta / "tasks.jsonl").write_text(json.dumps({"task_index": 0, "task": "pick"}) + "\n")


def _gappy_teleop(tmp_path: Path, present: list[int], rows=4, cameras=("cam_high", "cam_low")):
    """Build a gappy exploded teleop tree containing only `present` episodes."""
    d = tmp_path / "my-dataset"
    d.mkdir()
    for i in present:
        _write_episode(d, i, rows=rows, cameras=list(cameras))
    _write_meta(d, present, rows, list(cameras))
    return d


# ── teleop ─────────────────────────────────────────────────────────────────────


def test_teleop_gappy_becomes_contiguous_canonical(tmp_path):
    cameras = ["cam_high", "cam_low"]
    d = _gappy_teleop(tmp_path, present=[0, 3, 7, 11], rows=4, cameras=cameras)

    result = compact_teleop(d)

    assert result is not None
    assert result.gaps_closed is True
    assert result.reindexed is True
    assert result.units_after == 4

    # Canonical contiguous parquet 0..3, no exploded dirs left.
    parquets = sorted(p.name for p in d.rglob("episode_*.parquet"))
    assert parquets == [f"episode_{i:06d}.parquet" for i in range(4)]
    assert (d / "data" / "chunk-000" / "episode_000000.parquet").exists()
    assert not list(d.glob("episode_*"))  # exploded dirs gone

    # Videos relocated to canonical layout, N*cameras of them.
    videos = sorted(p for p in d.rglob("videos/**/episode_*.mp4"))
    assert len(videos) == 4 * len(cameras)
    assert (
        d / "videos" / "chunk-000" / "observation.images.cam_high" / "episode_000002.mp4"
    ).exists()


def test_teleop_rewrites_parquet_index_columns(tmp_path):
    d = _gappy_teleop(tmp_path, present=[2, 5, 9], rows=3)
    compact_teleop(d)

    # episode_index must match new file number; global index must be contiguous.
    expected_global = 0
    for new in range(3):
        t = pq.read_table(d / "data" / "chunk-000" / f"episode_{new:06d}.parquet")
        assert t.column("episode_index").to_pylist() == [new] * t.num_rows
        assert t.column("index").to_pylist() == list(
            range(expected_global, expected_global + t.num_rows)
        )
        expected_global += t.num_rows


def test_teleop_regenerates_meta(tmp_path):
    cameras = ["cam_high", "cam_low"]
    d = _gappy_teleop(tmp_path, present=[0, 3, 7, 11], rows=4, cameras=cameras)
    compact_teleop(d)

    info = json.loads((d / "meta" / "info.json").read_text())
    assert info["total_episodes"] == 4
    assert info["total_frames"] == 16
    assert info["total_videos"] == 4 * len(cameras)
    assert info["splits"] == {"train": "0:4"}

    ep_lines = [
        json.loads(x)
        for x in (d / "meta" / "episodes.jsonl").read_text().splitlines()
        if x.strip()
    ]
    assert [r["episode_index"] for r in ep_lines] == [0, 1, 2, 3]
    assert sum(r["length"] for r in ep_lines) == info["total_frames"]

    stats = [
        json.loads(x)
        for x in (d / "meta" / "episodes_stats.jsonl").read_text().splitlines()
        if x.strip()
    ]
    assert [r["episode_index"] for r in stats] == [0, 1, 2, 3]
    # tasks.jsonl preserved verbatim.
    assert (d / "meta" / "tasks.jsonl").exists()


def test_teleop_contiguous_is_left_untouched(tmp_path):
    # No gaps -> compaction must not touch the tree at all (unfiltered pulls
    # pay nothing and keep their layout).
    d = _gappy_teleop(tmp_path, present=[0, 1, 2], rows=4)
    before = {p.relative_to(d): p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}

    result = compact_teleop(d)
    assert result is not None
    assert result.gaps_closed is False
    assert result.reindexed is False
    after = {p.relative_to(d): p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}
    assert before == after  # untouched: still exploded, nothing moved or rewritten


def test_teleop_idempotent_after_compaction(tmp_path):
    d = _gappy_teleop(tmp_path, present=[3, 8], rows=4)
    compact_teleop(d)  # gappy -> canonical 0..1
    before = {p.relative_to(d): p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}

    second = compact_teleop(d)  # now contiguous -> no-op
    assert second is not None
    assert second.gaps_closed is False
    assert second.reindexed is False
    after = {p.relative_to(d): p.read_bytes() for p in sorted(d.rglob("*")) if p.is_file()}
    assert before == after


def test_teleop_no_episodes_returns_none(tmp_path):
    (tmp_path / "empty").mkdir()
    assert compact_teleop(tmp_path / "empty") is None


def test_teleop_ledger_only_force_finalizes_contiguous_tree(tmp_path):
    """A ledger-only download is contiguous by dataset index but its parquets
    still carry the station's episode numbering and there is no meta/. With
    ``force`` + inline ``meta`` it is renumbered, ``task_index`` zeroed,
    meta/ written from the manifest + parquets, calibration moved."""
    d = tmp_path / "ledger-ds"
    d.mkdir()
    rows = 4
    # Directory names are dataset indices; parquet episode_index is the source's.
    for dataset_idx, source_idx in ((0, 17), (1, 42)):
        _write_episode(d, dataset_idx, rows=rows, cameras=["cam_high"])
        pq_path = d / f"episode_{dataset_idx:06d}" / f"episode_{dataset_idx:06d}.parquet"
        t = pq.read_table(pq_path)
        t = t.set_column(
            t.column_names.index("episode_index"), "episode_index",
            pa.array([source_idx] * rows, type=pa.int64()),
        )
        t = t.set_column(
            t.column_names.index("task_index"), "task_index",
            pa.array([1] * rows, type=pa.int64()),
        )
        pq.write_table(t, pq_path)
    (d / "episode_000001" / "calibration.json").write_text('{"rig": "b"}')

    meta = {
        "info": {
            "fps": 30,
            "features": {
                "observation.images.cam_high": {"dtype": "video", "shape": [480, 640, 3]},
                "observation.images.cam_gone": {"dtype": "video", "shape": [480, 640, 3]},
                "action": {"dtype": "float64", "shape": [1]},
            },
        },
        "task": "fold towel",
    }
    res = compact_teleop(d, force=True, meta=meta)
    assert res is not None and res.reindexed and not res.gaps_closed

    t0 = pq.read_table(d / "data" / "chunk-000" / "episode_000000.parquet")
    t1 = pq.read_table(d / "data" / "chunk-000" / "episode_000001.parquet")
    assert set(t0.column("episode_index").to_pylist()) == {0}
    assert set(t1.column("episode_index").to_pylist()) == {1}
    assert t1.column("index").to_pylist() == list(range(rows, 2 * rows))
    assert set(t1.column("task_index").to_pylist()) == {0}
    assert (d / "videos" / "chunk-000" / "observation.images.cam_high" / "episode_000001.mp4").exists()

    info = json.loads((d / "meta" / "info.json").read_text())
    assert info["total_episodes"] == 2 and info["total_frames"] == 2 * rows
    assert info["total_videos"] == 2 and info["splits"] == {"train": "0:2"}
    assert "observation.images.cam_gone" not in info["features"]
    assert "observation.images.cam_high" in info["features"]
    assert json.loads((d / "meta" / "tasks.jsonl").read_text()) == {"task_index": 0, "task": "fold towel"}
    eps = [json.loads(x) for x in (d / "meta" / "episodes.jsonl").read_text().splitlines()]
    assert [e["episode_index"] for e in eps] == [0, 1] and eps[0]["length"] == rows
    stats = [json.loads(x) for x in (d / "meta" / "episodes_stats.jsonl").read_text().splitlines()]
    assert [s["episode_index"] for s in stats] == [0, 1]
    assert stats[1]["stats"]["index"]["min"] == [float(rows)]
    assert stats[0]["stats"]["episode_index"]["max"] == [0.0]
    assert (d / "meta" / "calibration" / "episode_000001.json").read_text() == '{"rig": "b"}'
    assert not (d / "meta" / "calibration" / "episode_000000.json").exists()
    assert not (d / "episode_000000").exists()


def test_ledger_plan_lands_canonical_and_refinalizes_in_place(tmp_path):
    """Ledger-only manifests plan files straight into the canonical layout so a
    re-run skips them; a second forced finalize is a no-op on the tree and
    top-level files (LICENSE) survive the swap."""
    from verlet.datasets._manifest import plan_items

    slug = "ledger-ds"
    manifest = {
        "dataset_slug": slug,
        "meta": {"info": {"fps": 30}, "task": "fold"},
        "episodes": [
            {
                "episode_index": i,
                "parquet_url": f"https://x/{i}.parquet",
                "video_urls": [{"camera": "cam_high", "url": f"https://x/{i}.mp4"}],
                "calibration_url": f"https://x/{i}.json",
                "meta_urls": [],
            }
            for i in (0, 1)
        ],
    }
    items = plan_items(slug, tmp_path, manifest)
    d = tmp_path / slug
    paths = {str(it.local_path.relative_to(d)) for it in items}
    assert paths == {
        "data/chunk-000/episode_000000.parquet",
        "data/chunk-000/episode_000001.parquet",
        "videos/chunk-000/observation.images.cam_high/episode_000000.mp4",
        "videos/chunk-000/observation.images.cam_high/episode_000001.mp4",
        "meta/calibration/episode_000000.json",
        "meta/calibration/episode_000001.json",
    }
    rows = 3
    for it in items:
        it.local_path.parent.mkdir(parents=True, exist_ok=True)
        if it.local_path.suffix == ".parquet":
            src = 40 + int(_EP_RE.search(it.local_path.name).group(1))
            pq.write_table(
                pa.table({
                    "episode_index": pa.array([src] * rows, type=pa.int64()),
                    "index": pa.array(range(rows), type=pa.int64()),
                    "action": pa.array([0.0] * rows, type=pa.float64()),
                }),
                it.local_path,
            )
        else:
            it.local_path.write_bytes(b"x")
    (d / "LICENSE").write_text("terms")

    first = compact_teleop(d, force=True, meta=manifest["meta"])
    assert first is not None and first.reindexed
    assert (d / "LICENSE").read_text() == "terms"
    assert (d / "meta" / "info.json").is_file()
    assert {str(p.relative_to(d)) for p in d.rglob("*") if p.is_file()} >= paths
    snapshot = {p: p.read_bytes() for p in d.rglob("*") if p.is_file()}

    second = compact_teleop(d, force=True, meta=manifest["meta"])
    assert second is not None and second.reindexed
    assert {p: p.read_bytes() for p in d.rglob("*") if p.is_file()} == snapshot


# ── ego ──────────────────────────────────────────────────────────────────────


def test_ego_gappy_segments_renumbered(tmp_path):
    d = tmp_path / "ego-ds"
    d.mkdir()
    for i in [0, 4, 9]:
        seg = d / f"segment_{i:06d}"
        seg.mkdir()
        (seg / "pose.parquet").write_bytes(b"PAR1")
        (seg / "info.json").write_text("{}")

    result = compact_ego(d)
    assert result is not None
    assert result.gaps_closed is True
    assert sorted(p.name for p in d.glob("segment_*")) == [
        "segment_000000",
        "segment_000001",
        "segment_000002",
    ]
    # Self-contained files preserved.
    assert (d / "segment_000002" / "pose.parquet").exists()


def test_ego_contiguous_is_noop(tmp_path):
    d = tmp_path / "ego-ds"
    d.mkdir()
    for i in [0, 1, 2]:
        (d / f"segment_{i:06d}").mkdir()
    result = compact_ego(d)
    assert result is not None
    assert result.gaps_closed is False


def test_ego_no_segments_returns_none(tmp_path):
    (tmp_path / "empty").mkdir()
    assert compact_ego(tmp_path / "empty") is None


# ── integration: real plan_items layout -> compaction ───────────────────────


def test_compaction_consumes_real_plan_items_layout(tmp_path):
    """Materialize files at the exact paths `plan_items` produces for a gappy
    showcase manifest, then compact — proving the layout contract end-to-end."""
    from verlet.datasets._manifest import plan_items

    slug = "my-dataset"
    present = [0, 4, 9]
    cameras = ["cam_high", "cam_low"]
    manifest = {
        "dataset_slug": slug,
        "episodes": [
            {
                "episode_index": i,
                "parquet_url": f"https://x/{i}.parquet",
                "video_urls": [{"camera": c, "url": f"https://x/{i}-{c}.mp4"} for c in cameras],
                "meta_urls": [
                    {"filename": "info.json", "url": "https://x/info"},
                    {"filename": "episodes.jsonl", "url": "https://x/eps"},
                ],
            }
            for i in present
        ],
    }
    items = plan_items(slug, tmp_path, manifest)

    # Simulate the download: create each planned file. Parquet must be real so
    # compaction can read it.
    rows = 5
    for it in items:
        it.local_path.parent.mkdir(parents=True, exist_ok=True)
        if it.local_path.suffix == ".parquet":
            old = int(_EP_RE.search(it.local_path.name).group(1))
            pq.write_table(
                pa.table(
                    {
                        "episode_index": pa.array([old] * rows, type=pa.int64()),
                        "index": pa.array(range(rows), type=pa.int64()),
                        "action": pa.array([0.0] * rows, type=pa.float64()),
                    }
                ),
                it.local_path,
            )
        elif it.local_path.name == "info.json":
            it.local_path.write_text(json.dumps({"total_episodes": len(present)}))
        elif it.local_path.name == "episodes.jsonl":
            it.local_path.write_text(
                "\n".join(
                    json.dumps({"episode_index": i, "tasks": [0], "length": rows})
                    for i in present
                )
                + "\n"
            )
        else:
            it.local_path.write_bytes(b"\x00")

    dataset_dir = tmp_path / slug
    result = compact_teleop(dataset_dir)

    assert result.gaps_closed is True
    parquets = sorted(int(_EP_RE.search(p.name).group(1)) for p in dataset_dir.rglob("episode_*.parquet"))
    assert parquets == [0, 1, 2]
    assert json.loads((dataset_dir / "meta" / "info.json").read_text())["total_episodes"] == 3
    assert not list(dataset_dir.glob("episode_*"))  # exploded dirs gone
