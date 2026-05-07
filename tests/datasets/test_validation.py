"""CLIDATA-06: pre-flight flag matrix (verlet/datasets/_validation.py)."""
import click
import pytest


def test_episode_ids_rejected_for_processed():
    from verlet.datasets._validation import validate_download_flags
    with pytest.raises(click.UsageError, match="episode_ids invalid for variant=processed"):
        validate_download_flags(
            modality="ego", variant="processed",
            episode_ids="1,2,3", segment_ids=None, format=None,
        )


def test_segment_ids_rejected_for_raw():
    from verlet.datasets._validation import validate_download_flags
    with pytest.raises(click.UsageError, match="segment_ids invalid for variant=raw"):
        validate_download_flags(
            modality="ego", variant="raw",
            episode_ids=None, segment_ids="s1,s2", format=None,
        )


def test_variant_required_on_ego_pure():
    from verlet.datasets._validation import validate_download_flags
    with pytest.raises(click.UsageError, match=r"--variant is required for ego"):
        validate_download_flags(
            modality="ego", variant=None,
            episode_ids=None, segment_ids=None, format=None,
        )


def test_variant_rejected_on_arm_pure():
    from verlet.datasets._validation import validate_download_flags
    with pytest.raises(click.UsageError, match="--variant is ego-only"):
        validate_download_flags(
            modality="arm", variant="raw",
            episode_ids=None, segment_ids=None, format=None,
        )


def test_arm_format_accepted_pure():
    """Phase 30 (CLIDATA-07): the validator no longer rejects non-native arm
    formats — the validate_format Click callback handles unknown values
    pre-HTTP, and the 202+job-id polling branch in commands.py handles the
    server-side conversion. validate_download_flags' arm branch only checks
    --variant / --segment-ids exclusivity now."""
    from verlet.datasets._validation import validate_download_flags

    # All 8 SUPPORTED_FORMATS are accepted on arm rows (no exception raised).
    for fmt in (
        "lerobot-v2",
        "lerobot-v3",
        "hdf5",
        "zarr",
        "rlds",
        "rosbag",
        "robodm",
        "egomimic",
    ):
        validate_download_flags(
            modality="arm", variant=None,
            episode_ids=None, segment_ids=None, format=fmt,
        )


def test_ego_format_still_rejected_pure():
    """Ego variants (raw/processed) remain exclusive of arm-style format
    conversion; the CLI surfaces this via a flag-incompatibility error."""
    from verlet.datasets._validation import validate_download_flags
    with pytest.raises(click.UsageError, match="--format is teleop-only"):
        validate_download_flags(
            modality="ego", variant="processed",
            episode_ids=None, segment_ids=None, format="hdf5",
        )


def test_category_with_kind_teleop_rejected():
    from verlet.datasets._validation import validate_kind_category
    with pytest.raises(click.UsageError, match=r"--category is ego-only"):
        validate_kind_category(kind="teleop", category="cooking")
