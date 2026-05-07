"""CLIDATA-05: verlet datasets info. Wave 0 stubs."""
import pytest

PHASE_29_NOT_IMPLEMENTED = "Phase 29 implementation pending — green by Plan 02/03 task"


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_info_by_slug(cli_runner, respx_mock, tmp_home):
    """`verlet datasets info pick-and-place-yam-v3` resolves slug-primary."""
    raise NotImplementedError("Plan 03 Task 2")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_info_by_uuid(cli_runner, respx_mock, tmp_home):
    """`verlet datasets info <uuid>` works (UUID fallback per D-MOD3)."""
    raise NotImplementedError("Plan 03 Task 2")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_info_json_output(cli_runner, respx_mock, tmp_home):
    """--json emits CatalogDatasetDetail payload directly."""
    raise NotImplementedError("Plan 03 Task 2 + Plan 02 Task 3 (_render.info_json)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_info_anonymous_public(cli_runner, respx_mock, tmp_home):
    """Anonymous works for public rows (D-MOD4)."""
    raise NotImplementedError("Plan 03 Task 2")
