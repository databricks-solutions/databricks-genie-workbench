"""Causal-drop strategist feedback (Cycle 5 T2).

Today blast_radius drops with high_collateral_risk_flagged emit no
strategist-visible signal. This suite pins the new typed
DroppedCausalPatch dataclass + the strategist input plumbing.
"""
from __future__ import annotations

import os
from unittest.mock import patch


def test_flag_helper_default_off() -> None:
    from genie_space_optimizer.common.config import (
        causal_drop_feedback_to_strategist_enabled,
    )
    with patch.dict(os.environ, {}, clear=True):
        assert causal_drop_feedback_to_strategist_enabled() is False


def test_flag_helper_on_when_env_set() -> None:
    from genie_space_optimizer.common.config import (
        causal_drop_feedback_to_strategist_enabled,
    )
    with patch.dict(
        os.environ,
        {"GSO_CAUSAL_DROP_FEEDBACK_TO_STRATEGIST": "1"}, clear=True,
    ):
        assert causal_drop_feedback_to_strategist_enabled() is True


def test_dropped_causal_patch_dataclass() -> None:
    from genie_space_optimizer.optimization.stages.gates import (
        DroppedCausalPatch,
    )
    d = DroppedCausalPatch(
        gate="blast_radius",
        reason="high_collateral_risk_flagged",
        proposal_id="P002",
        patch_type="add_sql_snippet_measure",
        target="catalog.schema.tkt_document",
        target_qids=("gs_026",),
        dependents_outside_target=("gs_004", "gs_007"),
        rca_id="rca_x",
        root_cause="plural_top_n_collapse",
    )
    assert d.gate == "blast_radius"
    assert d.dependents_outside_target == ("gs_004", "gs_007")
    # frozen dataclass: hashable for set membership / dedup
    assert hash(d) == hash(d)


def test_blast_radius_drop_captures_causal_patch_when_target_overlaps() -> None:
    """When the dropped patch's target_qids overlap the AG's
    causal target, the helper records it as DroppedCausalPatch."""
    from genie_space_optimizer.optimization.stages.gates import (
        DroppedCausalPatch,
        capture_dropped_causal_patch,
    )
    decision = {
        "gate": "blast_radius",
        "outcome": "dropped",
        "reason_code": "no_causal_target",
        "reason_detail": "high_collateral_risk_flagged",
        "proposal_id": "P002#1",
        "metrics": {
            "patch_type": "add_sql_snippet_measure",
            "target": "catalog.schema.tkt_document",
            "passing_dependents_outside_target": [
                "gs_004", "gs_007", "gs_008",
            ],
        },
    }
    captured = capture_dropped_causal_patch(
        decision=decision,
        ag_target_qids=("gs_026",),
        rca_id="rca_x",
        root_cause="plural_top_n_collapse",
    )
    assert isinstance(captured, DroppedCausalPatch)
    assert captured.dependents_outside_target == ("gs_004", "gs_007", "gs_008")
    assert captured.target_qids == ("gs_026",)


def test_blast_radius_drop_returns_none_when_target_does_not_overlap() -> None:
    """Empty AG target → not causal → not captured."""
    from genie_space_optimizer.optimization.stages.gates import (
        capture_dropped_causal_patch,
    )
    decision = {
        "gate": "blast_radius",
        "outcome": "dropped",
        "reason_detail": "high_collateral_risk_flagged",
        "metrics": {"target": "x"},
    }
    captured = capture_dropped_causal_patch(
        decision=decision,
        ag_target_qids=(),  # empty target → not causal
        rca_id="", root_cause="",
    )
    assert captured is None


def test_capture_dropped_causal_patch_skips_non_drop_outcomes() -> None:
    """Helper returns None for any outcome other than 'dropped'."""
    from genie_space_optimizer.optimization.stages.gates import (
        capture_dropped_causal_patch,
    )
    decision = {
        "gate": "blast_radius",
        "outcome": "accepted",
        "metrics": {"target": "x"},
    }
    captured = capture_dropped_causal_patch(
        decision=decision,
        ag_target_qids=("gs_026",),
        rca_id="rca_x",
        root_cause="plural_top_n_collapse",
    )
    assert captured is None


def test_action_groups_input_carries_prior_iteration_dropped_causal_patches() -> None:
    """ActionGroupsInput must accept the new optional field."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )
    from genie_space_optimizer.optimization.stages.gates import (
        DroppedCausalPatch,
    )
    drops = (
        DroppedCausalPatch(
            gate="blast_radius",
            reason="high_collateral_risk_flagged",
            proposal_id="P002#1",
            patch_type="add_sql_snippet_measure",
            target="catalog.schema.tkt_document",
            target_qids=("gs_026",),
            dependents_outside_target=("gs_004", "gs_007"),
            rca_id="rca_x",
            root_cause="plural_top_n_collapse",
        ),
    )
    inp = ActionGroupsInput(
        action_groups=(),
        prior_iteration_dropped_causal_patches=drops,
    )
    assert inp.prior_iteration_dropped_causal_patches == drops


def test_action_groups_input_default_empty_dropped_causal_patches() -> None:
    """The new field defaults to empty tuple — backwards compatible."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )
    inp = ActionGroupsInput(action_groups=())
    assert inp.prior_iteration_dropped_causal_patches == ()
