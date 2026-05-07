"""CLIDATA-04: verlet datasets list. Wave 0 stubs."""
import pytest

PHASE_29_NOT_IMPLEMENTED = "Phase 29 implementation pending — green by Plan 02/03 task"


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_kind_teleop_translates_to_modality_arm(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """`--kind teleop` MUST send `?modality=arm` (KIND_TO_MODALITY mapping)."""
    raise NotImplementedError("Plan 02 Task 2 (_api.py) + Plan 03 Task 1 (commands.py)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_limit_clamps_at_100_and_prints_footer(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """--limit > 100 → page_size=100 + truncation footer in stdout."""
    raise NotImplementedError("Plan 03 Task 1 (commands.py + _render.py)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_since_round_trips_to_backend(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """--since 2026-04-01 reaches backend with `?since=2026-04-01...`."""
    raise NotImplementedError("Plan 02 Task 2 (_api.py)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_anonymous_no_authorization(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """No active profile → NO Authorization header sent (D-MOD4)."""
    raise NotImplementedError("Plan 02 Task 2 (_api_url_and_headers anonymous path)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_authenticated_sends_bearer(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """Active profile → `Authorization: Bearer <token>` header sent."""
    raise NotImplementedError("Plan 02 Task 2 (_api_url_and_headers authenticated path)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_json_output(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """--json emits CatalogDatasetListItem[] verbatim (no client-side reshape)."""
    raise NotImplementedError("Plan 03 Task 1 (commands.py --json branch)")


@pytest.mark.xfail(reason=PHASE_29_NOT_IMPLEMENTED, strict=True)
def test_repeatable_task_flag(cli_runner, respx_mock, tmp_home, mock_catalog_list_response):
    """`--task pick --task push` → two `task_type=` query repetitions (D-FL3)."""
    raise NotImplementedError("Plan 03 Task 1 (Click multiple=True option)")
