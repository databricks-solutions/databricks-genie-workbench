"""Cycle 16 T4 — pure detect_structural_causal_drop contract."""

from __future__ import annotations


def _expression_drop(parent_pid: str, rca_id: str) -> dict:
    return {
        "proposal_id": parent_pid,
        "patch_type": "add_sql_snippet_expression",
        "reason": "high_collateral_risk_flagged",
        "target": "mv_esr_dim_location.zone_vp_name",
        "original_patch": {
            "proposal_id": parent_pid,
            "patch_type": "add_sql_snippet_expression",
            "target": "mv_esr_dim_location.zone_vp_name",
            "rca_id": rca_id,
        },
    }


def _filter_drop(parent_pid: str, rca_id: str) -> dict:
    return {
        "proposal_id": parent_pid,
        "patch_type": "add_sql_snippet_filter",
        "reason": "high_collateral_risk_flagged",
        "target": "mv_esr_dim_location.zone_vp_name",
        "original_patch": {
            "proposal_id": parent_pid,
            "patch_type": "add_sql_snippet_filter",
            "target": "mv_esr_dim_location.zone_vp_name",
            "rca_id": rca_id,
        },
    }


def test_structural_drop_with_no_survivor_yields_one_record() -> None:
    from genie_space_optimizer.optimization.stages.gates import (
        detect_structural_causal_drop,
    )

    out = detect_structural_causal_drop(
        blast_dropped=[_expression_drop("L6:P001#3", "RCA_H002")],
        narrow_survivors=[],
        ag_rca_id="RCA_H002",
        ag_target_qids=("gs_024", "gs_026"),
    )
    assert len(out) == 1
    rec = out[0]
    assert rec.ag_rca_id == "RCA_H002"
    assert rec.original_proposal_id == "L6:P001#3"
    assert rec.original_patch_type == "add_sql_snippet_expression"
    assert rec.original_target == "mv_esr_dim_location.zone_vp_name"
    assert rec.drop_reason == "high_collateral_risk_flagged"
    assert rec.target_qids == ("gs_024", "gs_026")


def test_structural_drop_with_branch_c_survivor_is_silent() -> None:
    from genie_space_optimizer.optimization.stages.gates import (
        detect_structural_causal_drop,
    )

    survivors = [{
        "proposal_id": "L6:P001#3#L5_BRANCH_C_gs_024",
        "patch_type": "add_example_sql",
        "derived_from": "L6:P001#3",
    }]
    out = detect_structural_causal_drop(
        blast_dropped=[_expression_drop("L6:P001#3", "RCA_H002")],
        narrow_survivors=survivors,
        ag_rca_id="RCA_H002",
        ag_target_qids=("gs_024",),
    )
    assert out == ()


def test_non_structural_filter_drop_is_silent() -> None:
    from genie_space_optimizer.optimization.stages.gates import (
        detect_structural_causal_drop,
    )

    out = detect_structural_causal_drop(
        blast_dropped=[_filter_drop("L4:F1", "RCA_H002")],
        narrow_survivors=[],
        ag_rca_id="RCA_H002",
        ag_target_qids=("gs_024",),
    )
    assert out == ()


def test_non_causal_drop_rca_mismatch_is_silent() -> None:
    from genie_space_optimizer.optimization.stages.gates import (
        detect_structural_causal_drop,
    )

    out = detect_structural_causal_drop(
        blast_dropped=[_expression_drop("L6:P001#3", "RCA_OTHER")],
        narrow_survivors=[],
        ag_rca_id="RCA_H002",
        ag_target_qids=("gs_024",),
    )
    assert out == ()


def test_diagnostic_ag_with_no_rca_is_silent() -> None:
    from genie_space_optimizer.optimization.stages.gates import (
        detect_structural_causal_drop,
    )

    out = detect_structural_causal_drop(
        blast_dropped=[_expression_drop("L6:P001#3", "")],
        narrow_survivors=[],
        ag_rca_id="",
        ag_target_qids=("gs_024",),
    )
    assert out == ()


def test_multiple_structural_drops_yield_one_record_per_orphan() -> None:
    from genie_space_optimizer.optimization.stages.gates import (
        detect_structural_causal_drop,
    )

    survivors = [{
        "proposal_id": "L6:P001#3#L5_BRANCH_C_gs_024",
        "patch_type": "add_example_sql",
        "derived_from": "L6:P001#3",
    }]
    out = detect_structural_causal_drop(
        blast_dropped=[
            _expression_drop("L6:P001#3", "RCA_H002"),
            _expression_drop("L6:P002#1", "RCA_H002"),
        ],
        narrow_survivors=survivors,
        ag_rca_id="RCA_H002",
        ag_target_qids=("gs_024",),
    )
    assert len(out) == 1
    assert out[0].original_proposal_id == "L6:P002#1"
