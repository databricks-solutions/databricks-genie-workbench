"""Plan 1 Task 12 — ActionGroupSlate.ag_records typed sidecar.

Same pattern as Task 11: legacy ``ags`` tuple of dicts stays
byte-stable; new ``ag_records: tuple[ActionGroup, ...]`` is derived
in __post_init__ via ``ActionGroup.from_legacy``. Malformed AGs (no
id) are skipped silently from typed records but remain in ``ags``.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.action_group import ActionGroup
from genie_space_optimizer.optimization.stages.action_groups import ActionGroupSlate


def _ag_dict() -> dict:
    return {
        "id": "AG_H001_L5",
        "ag_kind": "structural",
        "primary_cluster_id": "H001",
        "source_cluster_ids": ("H001",),
        "target_qids": ("gs_009",),
        "rca_id": "rca_H001_v1",
        "lever_directives": [5],
    }


def test_slate_has_ag_records_field() -> None:
    slate = ActionGroupSlate(ags=())
    assert hasattr(slate, "ag_records")
    assert slate.ag_records == ()


def test_ag_records_derived_from_ags() -> None:
    slate = ActionGroupSlate(ags=(_ag_dict(),))
    assert len(slate.ag_records) == 1
    ag = slate.ag_records[0]
    assert isinstance(ag, ActionGroup)
    assert ag.ag_id == "AG_H001_L5"


def test_explicit_ag_records_not_overwritten() -> None:
    ag = ActionGroup.from_legacy(_ag_dict())
    slate = ActionGroupSlate(ags=({"id": "AG_other"},), ag_records=(ag,))
    assert slate.ag_records[0] is ag


def test_round_trip_preserves_ag_records() -> None:
    slate = ActionGroupSlate(ags=(_ag_dict(),))
    payload = slate.to_json()
    restored = ActionGroupSlate.from_json(payload)
    assert len(restored.ag_records) == 1
    assert restored.ag_records[0].ag_id == "AG_H001_L5"


def test_malformed_ag_silently_skipped_in_typed_records() -> None:
    slate = ActionGroupSlate(ags=(_ag_dict(), {"no_id": True}))
    assert len(slate.ag_records) == 1
    assert len(slate.ags) == 2
