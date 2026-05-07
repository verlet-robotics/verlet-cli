"""CLIDATA-06: verlet datasets download. Wave 0 stubs."""
import pytest

PHASE_29_NOT_IMPLEMENTED = "Phase 29 implementation pending — green by Plan 02/03 task"


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_arm_dispatch(cli_runner, respx_mock, tmp_home, mock_catalog_list_response, mock_arm_manifest_response):
    """Arm catalog row → `/downloads/{slug}/manifest`; no `--variant` accepted."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_ego_dispatch(cli_runner, respx_mock, tmp_home, mock_catalog_list_response, mock_ego_manifest_response):
    """Ego catalog row → `/downloads/ego/datasets/{slug}/manifest?variant=…`."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_variant_rejected_on_arm(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """`--variant` on an arm row → pre-flight error (D-MOD2)."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_variant_required_on_ego(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """Ego row without `--variant` → pre-flight error (D-MOD2)."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_non_native_format_phase_30_hint(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """`--format hdf5` (non-native) → Phase-30 hint + clean exit."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_resume_skips_existing(cli_runner, respx_mock, tmp_home, mock_catalog_list_response, mock_arm_manifest_response):
    """`--resume` skips files that exist with nonzero size (current `_should_skip` semantics)."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_force_overrides_resume(cli_runner, respx_mock, tmp_home, mock_catalog_list_response, mock_arm_manifest_response):
    """`--force` re-downloads files that would otherwise be skipped by `--resume`."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_dry_run_no_writes(cli_runner, respx_mock, tmp_home, mock_catalog_list_response, mock_arm_manifest_response):
    """`--dry-run` prints planned writes; nothing written to disk."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_partial_failure_exits_nonzero(cli_runner, respx_mock, tmp_home, mock_catalog_list_response, mock_arm_manifest_response):
    """Any file failure → SystemExit(1); other files still attempted (ROADMAP §29 SC3)."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_unauthenticated_early_exit(cli_runner, respx_mock, tmp_home):
    """No active profile → fail-fast pre-flight error (D-MOD4); no HTTP round-trip."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_format_lerobot_v2_happy_path(cli_runner, respx_mock, tmp_home, mock_catalog_list_response, mock_arm_manifest_response):
    """`--format lerobot-v2` (native) → manifest fetched + files downloaded."""
    raise NotImplementedError("Plan 03 Task 3 (download command)")
