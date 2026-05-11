"""Cycle 16 T2 — Branch C diagnosis & flag-discipline contract.

Branch C is gated by GSO_L6_NARROW_REPLACEMENT_BRANCH_C (default-off).
When the flag is on AND the patch is an L6 expression / measure AND at
least one target QID is resolvable, narrow_replacement_diagnosis
returns ``applicable=True, reason="l5_example_sql_per_qid",
branch="C"``. Branch C takes precedence over Branch A.

When the flag is off, the diagnosis routes through the legacy
Branch A path unchanged (replay byte-stability).
"""

from __future__ import annotations


def _expression_patch() -> dict:
    return {
        "proposal_id": "L6:P001#3",
        "patch_type": "add_sql_snippet_expression",
        "target": "mv_esr_dim_location.zone_vp_name",
        "sql_expression": (
            "CASE WHEN role = 'VP' AND zone IS NOT NULL THEN name END"
        ),
        "rca_id": "RCA_H002",
    }


def test_flag_off_diagnosis_routes_to_branch_a_when_for_expression_on(
    monkeypatch,
) -> None:
    """When Branch C flag off + legacy FOR_EXPRESSION flag on, diagnosis
    returns Branch A's existing reason (no behavior change)."""
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", raising=False)
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )

    diag = narrow_replacement_diagnosis(
        original_patch=_expression_patch(),
        ag_target_qids=("gs_024",),
        root_cause="plural_top_n_collapse",
    )
    assert diag["applicable"] is True
    assert diag["reason"] == "expression_qid_scope"
    assert diag.get("branch", "A") == "A"


def test_flag_off_diagnosis_declines_when_both_flags_off(
    monkeypatch,
) -> None:
    """When both Branch C and FOR_EXPRESSION are off, an L6 expression
    is not applicable — preserves the pre-P0 legacy decline."""
    monkeypatch.delenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", raising=False)
    monkeypatch.delenv(
        "GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION", raising=False,
    )
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )

    diag = narrow_replacement_diagnosis(
        original_patch=_expression_patch(),
        ag_target_qids=("gs_024",),
        root_cause="plural_top_n_collapse",
    )
    assert diag["applicable"] is False
    assert diag["reason"] == "patch_type_lacks_where_predicate"


def test_flag_on_diagnosis_routes_to_branch_c_with_resolvable_qids(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )

    diag = narrow_replacement_diagnosis(
        original_patch=_expression_patch(),
        ag_target_qids=("gs_024", "gs_026"),
        root_cause="plural_top_n_collapse",
        qid_to_question_text={
            "gs_024": "q1?", "gs_026": "q2?",
        },
        qid_to_reference_sql={
            "gs_024": "SELECT 1", "gs_026": "SELECT 2",
        },
    )
    assert diag["applicable"] is True
    assert diag["reason"] == "l5_example_sql_per_qid"
    assert diag["branch"] == "C"
    assert set(diag["resolvable_target_qids"]) == {"gs_024", "gs_026"}


def test_flag_on_diagnosis_takes_precedence_over_branch_a(
    monkeypatch,
) -> None:
    """Both Branch C and FOR_EXPRESSION flags on → Branch C wins."""
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", "1")
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )

    diag = narrow_replacement_diagnosis(
        original_patch=_expression_patch(),
        ag_target_qids=("gs_024",),
        root_cause="plural_top_n_collapse",
        qid_to_question_text={"gs_024": "q1?"},
        qid_to_reference_sql={"gs_024": "SELECT 1"},
    )
    assert diag["applicable"] is True
    assert diag["reason"] == "l5_example_sql_per_qid"
    assert diag["branch"] == "C"


def test_flag_on_but_no_resolvable_qids_declines_with_typed_reason(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )

    diag = narrow_replacement_diagnosis(
        original_patch=_expression_patch(),
        ag_target_qids=("gs_024",),
        root_cause="plural_top_n_collapse",
        qid_to_question_text={},
        qid_to_reference_sql={},
    )
    assert diag["applicable"] is False
    assert diag["reason"] == "no_resolvable_target_qids"
    assert diag.get("branch") == "C"


def test_flag_on_filter_patch_does_not_take_branch_c(monkeypatch) -> None:
    """Branch C is L6 expression/measure only. Filter patches keep the
    legacy filter-narrow path."""
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )

    diag = narrow_replacement_diagnosis(
        original_patch={
            "proposal_id": "L4:filter#1",
            "patch_type": "add_sql_snippet_filter",
            "where_predicate": "role = 'VP'",
            "qid_predicate_column": "query_id",
        },
        ag_target_qids=("gs_024",),
        root_cause="plural_top_n_collapse",
        qid_to_question_text={"gs_024": "q1?"},
        qid_to_reference_sql={"gs_024": "SELECT 1"},
    )
    assert diag["applicable"] is True
    assert diag["reason"] == "filter_predicate_narrowable"
    assert diag.get("branch", "A") == "A"


def test_flag_helper_is_default_off() -> None:
    """``GSO_L6_NARROW_REPLACEMENT_BRANCH_C`` is default-off so legacy
    replay fixtures stay byte-stable."""
    import os
    os.environ.pop("GSO_L6_NARROW_REPLACEMENT_BRANCH_C", None)
    from genie_space_optimizer.common.config import (
        l6_narrow_replacement_branch_c_enabled,
    )
    assert l6_narrow_replacement_branch_c_enabled() is False
