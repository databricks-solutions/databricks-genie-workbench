"""P0 Task 1 (DIAGNOSTIC): pin the current narrow-replacement
behavior for add_sql_snippet_expression / add_sql_snippet_measure
patch shapes mirroring the H002 drop in run 900000000000001.

These tests document the *current* state. They are not the fix —
they are the failing baseline that determines which branch (A/B/C)
of the P0 plan the executor follows. Tasks 2+ in the active branch
will mutate these expectations.
"""

from __future__ import annotations


def _h002_expression_patch() -> dict:
    return {
        "proposal_id": "L6:P001#3",
        "patch_type": "add_sql_snippet_expression",
        "target": "mv_esr_dim_location.zone_vp_name",
        "sql_expression": (
            "CASE WHEN role = 'VP' AND zone IS NOT NULL THEN name END"
        ),
        "rationale": "plural top-N collapse for zone-VP",
    }


def test_diagnose_expression_patch_lacks_where_predicate_when_flag_off(
    monkeypatch,
) -> None:
    monkeypatch.delenv(
        "GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION", raising=False
    )
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )

    diag = narrow_replacement_diagnosis(
        original_patch=_h002_expression_patch(),
        ag_target_qids=("7now_delivery_analytics_space_gs_026",),
        root_cause="plural_top_n_collapse",
    )
    assert diag["applicable"] is False
    assert str(diag["reason"]) == "patch_type_lacks_where_predicate"
    assert str(diag["original_patch_type"]) == "add_sql_snippet_expression"


def test_build_returns_none_for_expression_patch_when_flag_off(
    monkeypatch,
) -> None:
    monkeypatch.delenv(
        "GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION", raising=False
    )
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_narrow_l6_replacement,
    )

    result = build_narrow_l6_replacement(
        original_patch=_h002_expression_patch(),
        ag_target_qids=("7now_delivery_analytics_space_gs_026",),
        root_cause="plural_top_n_collapse",
    )
    assert result is None
