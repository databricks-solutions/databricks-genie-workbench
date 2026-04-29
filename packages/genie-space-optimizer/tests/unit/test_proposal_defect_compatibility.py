from __future__ import annotations

from genie_space_optimizer.optimization.proposal_grounding import (
    proposal_is_defect_compatible,
)


def test_missing_filter_rejects_measure_patch() -> None:
    proposal = {
        "patch_type": "add_sql_snippet_measure",
        "rca_kind": "missing_filter",
        "expression": "SUM(cy_sales)",
    }
    decision = proposal_is_defect_compatible(proposal)
    assert decision["compatible"] is False
    assert decision["reason"] == "patch_type_incompatible_with_rca_kind"


def test_missing_filter_accepts_filter_or_instruction_patch() -> None:
    for patch_type in ("add_sql_snippet_filter", "add_instruction", "update_instruction_section"):
        decision = proposal_is_defect_compatible({
            "patch_type": patch_type,
            "rca_kind": "missing_filter",
        })
        assert decision["compatible"] is True


def test_missing_measure_accepts_measure_patch() -> None:
    decision = proposal_is_defect_compatible({
        "patch_type": "add_sql_snippet_measure",
        "rca_kind": "missing_measure",
    })
    assert decision["compatible"] is True


def test_unknown_rca_kind_is_diagnostic_not_blocking() -> None:
    decision = proposal_is_defect_compatible({
        "patch_type": "add_sql_snippet_measure",
        "rca_kind": "unknown_new_kind",
    })
    assert decision["compatible"] is True
    assert decision["reason"] == "unknown_rca_kind"
