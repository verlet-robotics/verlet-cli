"""CLIDATA-06: pre-flight flag matrix (verlet/datasets/_validation.py). Wave 0 stubs."""
import pytest

PHASE_29_NOT_IMPLEMENTED = "Phase 29 implementation pending — green by Plan 02 Task 1"


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_episode_ids_rejected_for_processed():
    raise NotImplementedError("Plan 02 Task 1 (_validation.validate_download_flags)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_segment_ids_rejected_for_raw():
    raise NotImplementedError("Plan 02 Task 1")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_variant_required_on_ego_pure():
    raise NotImplementedError("Plan 02 Task 1")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_variant_rejected_on_arm_pure():
    raise NotImplementedError("Plan 02 Task 1")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_non_native_format_rejected_pure():
    raise NotImplementedError("Plan 02 Task 1")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_category_with_kind_teleop_rejected():
    raise NotImplementedError("Plan 02 Task 1 (--category ego-only)")
