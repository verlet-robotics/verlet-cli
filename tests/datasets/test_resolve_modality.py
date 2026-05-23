"""Unit tests for ``datasets._api.resolve_modality``.

The live showcase backend currently omits the explicit ``modality`` field
from the wire response despite the Pydantic schema declaring it — every
ego dataset was rendered as ``(teleop)`` until this fallback landed.
Coverage:

* Explicit ``modality`` field wins when present and valid.
* ``task_type == "ego"`` triggers ego (showcase listing path).
* ``robot_embodiment == "human-ego"`` triggers ego (showcase listing path).
* ``ego_task_dataset_id`` set triggers ego (platform catalog path).
* Defaults to teleop when nothing matches.
"""
from __future__ import annotations

from verlet.datasets._api import resolve_modality


def test_explicit_modality_ego_wins():
    assert resolve_modality({"modality": "ego"}) == "ego"


def test_explicit_modality_teleop_wins():
    assert resolve_modality({"modality": "teleop"}) == "teleop"


def test_task_type_ego_triggers_ego():
    """The showcase listing response carries task_type but not modality."""
    assert resolve_modality({"task_type": "ego"}) == "ego"


def test_robot_embodiment_human_ego_triggers_ego():
    """``human-ego`` is the canonical ego marker on both listing + detail."""
    assert resolve_modality({"robot_embodiment": "human-ego"}) == "ego"


def test_platform_catalog_discriminator_triggers_ego():
    """``ego_task_dataset_id`` is the platform catalog's discriminator."""
    assert (
        resolve_modality({"ego_task_dataset_id": "abc-123"}) == "ego"
    )


def test_defaults_to_teleop_when_nothing_matches():
    assert resolve_modality({"task_type": "pick-and-place"}) == "teleop"
    assert resolve_modality({}) == "teleop"


def test_unknown_modality_string_falls_through_to_heuristics():
    """A garbled modality value (e.g. server adds a new kind) must NOT be
    blindly trusted — fall through to the rest of the heuristic."""
    assert (
        resolve_modality({"modality": "future_kind", "task_type": "ego"})
        == "ego"
    )
