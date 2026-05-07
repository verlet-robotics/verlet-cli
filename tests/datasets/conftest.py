"""Fixtures for tests/datasets/. Extends tests/auth/conftest.py.

The `pytest_plugins = ["tests.auth.conftest"]` directive that wires in the
auth conftest's `cli_runner` / `tmp_home` fixtures lives at the rootdir
(`verlet-cli/conftest.py`) rather than here. Pytest 8+ deprecated
`pytest_plugins` from non-top-level conftests because it implicitly affects
the whole session, and the directive at the rootdir is exactly what
`pytest tests/datasets/ tests/auth/` (combined) needs to avoid the
"Plugin already registered under a different name" double-load error.
"""
from __future__ import annotations

from typing import Any

import pytest


@pytest.fixture
def mock_catalog_list_response() -> dict[str, Any]:
    """Canned `/api/platform/v1/catalog/datasets` paginated list.

    Two rows: one teleop (arm) row, one ego row. Mirrors
    CatalogDatasetListItem wire shape from
    backend/services/platform_catalog/schema_platform_catalog.py.
    """
    return {
        "items": [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "slug": "pick-and-place-yam-v3",
                "title": "Pick and Place YAM v3",
                "task_type": "pick-and-place",
                "robot_embodiment": "yam",
                "episode_count": 120,
                "total_hours": 8.5,
                "total_bytes": 1_250_000_000,
                "available_variants": ["processed"],
                "data_tiers": ["processed"],
                "published_at": "2026-04-15T12:00:00Z",
                "price_per_hour_cents": 1500,
                "currency": "USD",
            },
            {
                "id": "00000000-0000-0000-0000-000000000002",
                "slug": "kitchen-cooking-aria-spring-2026",
                "title": "Kitchen Cooking Aria",
                "task_type": "cooking",
                "robot_embodiment": "aria",
                "episode_count": 45,
                "total_hours": 12.0,
                "total_bytes": 4_800_000_000,
                "available_variants": ["raw", "processed"],
                "data_tiers": ["raw", "processed"],
                "published_at": "2026-04-20T08:00:00Z",
                "price_per_hour_cents": 2000,
                "price_raw_per_hour_cents": 800,
                "currency": "USD",
            },
        ],
        "total": 2,
        "page": 1,
        "page_size": 20,
    }


@pytest.fixture
def mock_arm_manifest_response() -> dict[str, Any]:
    """Canned arm DownloadManifest from /api/platform/v1/downloads/{slug}/manifest."""
    return {
        "dataset_id": "00000000-0000-0000-0000-000000000001",
        "dataset_slug": "pick-and-place-yam-v3",
        "format": "lerobot-v2",
        "available_variants": ["processed"],
        "data_tiers": ["processed"],
        "files": [
            {
                "path": "data/chunk-000/episode_000000.parquet",
                "url": "https://r2.verlet.co/signed/episode_000000.parquet?token=abc",
                "size_bytes": 1_500_000,
            },
            {
                "path": "videos/observation.images.cam_high/episode_000000.mp4",
                "url": "https://r2.verlet.co/signed/episode_000000.mp4?token=def",
                "size_bytes": 25_000_000,
            },
            {
                "path": "meta/info.json",
                "url": "https://r2.verlet.co/signed/info.json?token=ghi",
                "size_bytes": 2_048,
            },
        ],
    }


@pytest.fixture
def mock_ego_manifest_response() -> dict[str, Any]:
    """Canned ego variant DownloadManifest (variant=processed) from
    /api/platform/v1/downloads/ego/datasets/{slug}/manifest."""
    return {
        "dataset_id": "00000000-0000-0000-0000-000000000002",
        "dataset_slug": "kitchen-cooking-aria-spring-2026",
        "variant": "processed",
        "available_variants": ["raw", "processed"],
        "data_tiers": ["raw", "processed"],
        "files": [
            {
                "path": "segments/seg-001/hand_pose/pose_full.parquet",
                "url": "https://r2.verlet.co/signed/pose_full.parquet?token=xyz",
                "size_bytes": 500_000,
            },
            {
                "path": "segments/seg-001/rgb.mp4",
                "url": "https://r2.verlet.co/signed/rgb.mp4?token=uvw",
                "size_bytes": 18_000_000,
            },
        ],
    }
