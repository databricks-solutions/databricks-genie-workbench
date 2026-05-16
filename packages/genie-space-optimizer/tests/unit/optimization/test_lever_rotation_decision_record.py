"""Unit test for the lever_rotation_decided decision-record emitter."""

from __future__ import annotations


def test_lever_rotation_decided_record_fields():
    from genie_space_optimizer.optimization.decision_emitters import (
        lever_rotation_decided_record,
    )

    rec = lever_rotation_decided_record(
        run_id="r-1",
        iteration=2,
        cluster_id="C1",
        rca_kind="measure_swap",
        selected_lever=2,
        selected_patch_type="update_column_description",
        legacy_lever=6,
        tried_lever_families=(6,),
    )
    payload = rec.to_dict()
    assert payload["decision_type"] == "lever_rotation_decided"
    assert payload["cluster_id"] == "C1"
    assert payload["rca_kind"] == "measure_swap"
    assert payload["selected_lever"] == 2
    assert payload["selected_patch_type"] == "update_column_description"
    assert payload["legacy_lever"] == 6
    assert payload["tried_lever_families"] == [6]
