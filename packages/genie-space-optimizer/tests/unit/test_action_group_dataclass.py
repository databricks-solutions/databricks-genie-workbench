"""Plan 1 Task 6 — ActionGroup typed view contract.

ActionGroup is the typed projection of the legacy AG dict shape
every strategist + harness path produces today.
``ActionGroup.from_legacy(ag)`` is the read-only adapter; the dict
remains the source of truth in Plan 1. Plan 4 promotes ActionGroup
to the source of truth.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from genie_space_optimizer.optimization.action_group import ActionGroup
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def _sample_legacy_ag() -> dict:
    return {
        "id": "AG_H001_L5",
        "ag_id": "AG_H001_L5",
        "ag_kind": "structural",
        "primary_cluster_id": "H001",
        "source_cluster_ids": ("H001",),
        "target_qids": ("gs_009",),
        "affected_questions": ("gs_009",),
        "rca_id": "rca_H001_v1",
        "lever_directives": [5],
        "needs_rca_regeneration": False,
    }


def test_action_group_mixes_jsonroundtrip() -> None:
    assert issubclass(ActionGroup, JsonRoundTrip)


def test_action_group_is_frozen() -> None:
    ag = ActionGroup.from_legacy(_sample_legacy_ag())
    with pytest.raises(FrozenInstanceError):
        ag.ag_kind = "lean"  # type: ignore[misc]


def test_from_legacy_pulls_canonical_id() -> None:
    ag = ActionGroup.from_legacy(_sample_legacy_ag())
    assert ag.ag_id == "AG_H001_L5"


def test_from_legacy_prefers_id_over_ag_id_when_both_present() -> None:
    """The legacy AG dict carries both ``id`` and ``ag_id``; the
    harness convention is ``id`` is canonical."""
    ag = ActionGroup.from_legacy({"id": "AG_canonical", "ag_id": "AG_other"})
    assert ag.ag_id == "AG_canonical"


def test_from_legacy_falls_back_to_ag_id_when_id_absent() -> None:
    ag = ActionGroup.from_legacy({"ag_id": "AG_fallback"})
    assert ag.ag_id == "AG_fallback"


def test_from_legacy_normalises_target_qids_to_tuple() -> None:
    ag = ActionGroup.from_legacy({
        "id": "AG_X",
        "target_qids": ["gs_001", "gs_002"],
    })
    assert ag.target_qids == ("gs_001", "gs_002")


def test_from_legacy_uses_affected_questions_when_target_qids_missing() -> None:
    ag = ActionGroup.from_legacy({
        "id": "AG_X",
        "affected_questions": ["gs_001"],
    })
    assert ag.target_qids == ("gs_001",)


def test_from_legacy_carries_source_cluster_ids() -> None:
    ag = ActionGroup.from_legacy(_sample_legacy_ag())
    assert ag.source_cluster_ids == ("H001",)


def test_from_legacy_carries_lever_directives() -> None:
    ag = ActionGroup.from_legacy(_sample_legacy_ag())
    assert ag.lever_directives == (5,)


def test_from_legacy_handles_empty_lever_directives() -> None:
    ag = ActionGroup.from_legacy({"id": "AG_X"})
    assert ag.lever_directives == ()


def test_round_trip_preserves_all_fields() -> None:
    ag = ActionGroup.from_legacy(_sample_legacy_ag())
    restored = ActionGroup.from_json(ag.to_json())
    assert restored == ag


def test_from_legacy_raises_on_missing_id() -> None:
    """No ``id`` and no ``ag_id`` is a data-integrity bug."""
    with pytest.raises(ValueError, match="ag_id"):
        ActionGroup.from_legacy({"target_qids": ["gs_001"]})
